use crate::protocol::{Packet, Protocol};
use crate::router::{KyChannel, KyChannelRecv, KyChannelSend, KyRecvMsg};
use crate::{EndpointDesc, Error, Result, StreamOwner};

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use async_trait::async_trait;
use quinn::{RecvStream, VarInt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug)]
enum RecvMessage {
    NewStream(RecvStream),
    NewPacket {
        packet: Packet,
        codec_gen: u32,
        config_gen: u32,
    },
}

/**
 * This protocol opens a new QUIC stream for every GOP (group of pictures).
 *
 * Concretely, a new QUIC stream is started on every keyframe.
 *
 * The principle is to transmit a GOP reliably (with retransmissions when
 * packets are lost, like TCP), but abandon any old GOPs whenever a new one is
 * started.
 *
 * The current codec and config packet are repeated at the beginning of each
 * QUIC stream, with an identifier so that the receiver only transmit them once
 * to the final client.
 *
 * For example, if the producer sends these packets to the kymux server (over
 * the client TCP socket):
 *     +--------+------+-----------------------+------+-----------------
 *     | codec1 | cfg1 | I P P P P P P I P P P | cfg2 | I P P P P P ...
 *     +--------+------+-----------------------+------+-----------------
 *     ----+--------+------+-------------------+
 *     ... | codec2 | cfg3 | I P P P I P P P P |
 *     ----+--------+------+-------------------+
 *
 * (I: key frame, P: non-key frame)
 *
 * Then the kymux server will transmit these data to the kymux client as follow:
 *
 * QUIC stream 1:
 *     +-----+--------+------+---------------+
 *     | ids | codec1 | cfg1 | I P P P P P P |
 *     +-----+--------+------+---------------+
 * QUIC stream 2:
 *     +-----+--------+------+-----------+
 *     | ids | codec1 | cfg1 | I P P P P |
 *     +-----+--------+------+-----------+
 * QUIC stream 3:
 *     +-----+--------+------+-------------+
 *     | ids | codec1 | cfg2 | I P P P P P |
 *     +-----+--------+------+-------------+
 * QUIC stream 4:
 *     +-----+--------+------+---------+
 *     | ids | codec2 | cfg3 | I P P P |
 *     +-----+--------+------+---------+
 * QUIC stream 5:
 *     +-----+--------+------+-----------+
 *     | ids | codec2 | cfg3 | I P P P P |
 *     +-----+--------+------+-----------+
 *
 * The ids are just two 32-bit numbers identifying the codec and config number.
 * They are incremented every time a new config or config packet is received
 * from the producer.
 *
 * The receiver uses these ids to make sure it sends a single codec or config
 * packet at most once.
 *
 * Every time a new QUIC stream is opened by the server, the previous one is
 * reset, so no more retransmissions will occur for the old GOPs.
 */

pub(crate) struct GopStreamProtocol {
    desc: EndpointDesc,
}

impl GopStreamProtocol {
    #[allow(dead_code)]
    pub(crate) fn new(desc: EndpointDesc) -> Self {
        Self { desc }
    }

    async fn send_video(
        &self,
        ky_channel_tx: KyChannelSend,
        mut client_rx: OwnedReadHalf,
    ) -> Result<()> {
        let mut codec = (0u32, None);
        let mut config = (0u32, None);

        let mut stream = ky_channel_tx.open_uni().await?;
        loop {
            let packet = Packet::read(&mut client_rx).await?;
            match packet {
                Packet::Codec(packet) => {
                    codec = (codec.0 + 1, Some(packet));
                }
                Packet::Media(packet) => {
                    if packet.is_config {
                        config = (config.0 + 1, Some(packet));
                    } else {
                        if packet.is_key {
                            // Start a new QUIC stream
                            let mut new_stream = ky_channel_tx.open_uni().await?;

                            // First write the current codec and config packets
                            // numbers. Since they are repeated on each QUIC
                            // stream, this helps the receiver to determine
                            // when there is a real new codec or config packet.
                            new_stream.write_u32(codec.0).await?;
                            new_stream.write_u32(config.0).await?;

                            new_stream
                                .write_all(&codec.1.as_ref().unwrap().header)
                                .await?;
                            new_stream
                                .write_all(&config.1.as_ref().unwrap().data)
                                .await?;

                            // Abandon the old stream
                            stream
                                .reset(VarInt::from_u32(0))
                                .expect("Unexpected unknown stream");

                            stream = new_stream;
                        }

                        stream.write_all(&packet.data).await?;
                    }
                }
            }
        }
    }

    async fn accept_unis(
        mut ky_channel_rx: KyChannelRecv,
        tx: mpsc::Sender<RecvMessage>,
    ) -> Result<()> {
        loop {
            let msg = ky_channel_rx.recv().await?;
            match msg {
                KyRecvMsg::AcceptUni(recv_stream) => {
                    tx.send(RecvMessage::NewStream(recv_stream))
                        .await
                        .map_err(|_| {
                            Error::KymuxProtocolError("Could not send message".to_string())
                        })?;
                }
                _ => {
                    return Err(Error::KymuxProtocolError(
                        "Unexpected KyRecvMsg".to_string(),
                    ));
                }
            }
        }
    }

    async fn read_packets(
        mut recv_stream: RecvStream,
        tx: mpsc::Sender<RecvMessage>,
    ) -> Result<()> {
        let codec_gen = recv_stream.read_u32().await?;
        let config_gen = recv_stream.read_u32().await?;
        loop {
            let packet = Packet::read(&mut recv_stream).await?;
            tx.send(RecvMessage::NewPacket {
                packet,
                codec_gen,
                config_gen,
            })
            .await
            .map_err(|_| Error::KymuxProtocolError("Could not send message".to_string()))?;
        }
    }

    async fn recv_video(
        &self,
        ky_channel_rx: KyChannelRecv,
        mut client_tx: OwnedWriteHalf,
    ) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(16);

        let tx2 = tx.clone();
        tokio::spawn(async move {
            let ret = Self::accept_unis(ky_channel_rx, tx2).await;
            if let Err(e) = ret {
                error!("Error: {e:?}");
            }
        });

        let mut join_handle: Option<JoinHandle<()>> = None;

        let mut last_codec_gen = 0u32;
        let mut last_config_gen = 0u32;

        loop {
            let msg = rx.recv().await.ok_or_else(|| {
                Error::KymuxProtocolError("Could not receive message".to_string())
            })?;
            match msg {
                RecvMessage::NewStream(recv_stream) => {
                    info!("NewStream");
                    let tx = tx.clone();
                    if let Some(join_handle) = join_handle {
                        // This is not totally optimal: we should abandon an
                        // old stream only once we received a complete frame on
                        // the new stream (in case we receive packets on an old
                        // stream before we receive a full frame on the new
                        // stream), but this is way more complex, so keep it
                        // simple: only keep the latest QUIC stream.
                        join_handle.abort();
                    }
                    join_handle = Some(tokio::spawn(async move {
                        let ret = Self::read_packets(recv_stream, tx).await;
                        if let Err(e) = ret {
                            // Expected when the stream is closed
                            debug!("Read packets error: {e:?}");
                        }
                    }));
                }
                RecvMessage::NewPacket {
                    packet,
                    codec_gen,
                    config_gen,
                } => match packet {
                    Packet::Codec(packet) => {
                        if codec_gen != last_codec_gen {
                            client_tx.write_all(&packet.header).await?;
                            last_codec_gen = codec_gen;
                        }
                    }
                    Packet::Media(packet) => {
                        if !packet.is_config || config_gen != last_config_gen {
                            client_tx.write_all(&packet.data).await?;
                            last_config_gen = config_gen;
                        }
                    }
                },
            }
        }
    }
}

#[async_trait]
impl Protocol for GopStreamProtocol {
    async fn forward(&mut self, ky_channel: KyChannel, client: TcpStream) -> Result<()> {
        let (client_rx, client_tx) = client.into_split();
        let (ky_channel_rx, ky_channel_tx) = ky_channel.into_split();

        if self.desc.owner == StreamOwner::Local {
            self.send_video(ky_channel_tx, client_rx).await
        } else {
            self.recv_video(ky_channel_rx, client_tx).await
        }
    }
}
