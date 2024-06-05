use crate::protocol::av::{AVPacket, AVPacketHeader, CodecPacket, MediaPacket};
use crate::protocol::driver::av;
use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;
use crate::task::Task;

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use kynet::error::{ConnectionError, ReadExactError};
use kynet::{RecvStream, SendStream};
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::sync::mpsc;

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
 * QUIC stream, with an identifier so that the receiver can determine if a new
 * one should be sent to the decoder.
 *
 * For example, if the producer sends these packets in sequence:
 *  - codec1 (a codec packet)
 *  - cfg1 (a config packet)
 *  - a sequence of media packets: I P P P P P P I P P P
 *  - cfg2
 *  - a sequence of media packets: I P P P P P P
 *  - codec2
 *  - cfg3
 *  - a sequence of media packets: I P P P I P P P P
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

pub(crate) struct VideoGopStreamProtocolSendDriver {
    ky_channel: KyChannel,
    current_stream: Option<SendStream>,
    codec_gen: u32,
    codec_packet: Option<CodecPacket>,
    config_gen: u32,
    config_packet: Option<MediaPacket>,
}

impl VideoGopStreamProtocolSendDriver {
    pub(crate) async fn start(ky_channel: KyChannel) -> Result<Self, ProtocolError> {
        Ok(Self {
            ky_channel,
            current_stream: None,
            codec_gen: 0,
            codec_packet: None,
            config_gen: 0,
            config_packet: None,
        })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for VideoGopStreamProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        match packet {
            AVPacket::Codec(packet) => {
                self.codec_gen += 1;
                self.codec_packet = Some(packet);
            }
            AVPacket::Media(packet) => {
                if packet.header.is_config {
                    self.config_gen += 1;
                    self.config_packet = Some(packet);
                } else {
                    if packet.header.is_key {
                        // Start a new stream
                        let mut new_stream = self.ky_channel.open_uni().await?;

                        // First write the current codec and config packets
                        // numbers. Since they are repeated on each stream,
                        // this helps the receiver to determine when there is a
                        // real new codec or config packet.
                        let mut buf = vec![];
                        buf.write_u32::<BigEndian>(self.codec_gen);
                        buf.write_u32::<BigEndian>(self.config_gen);

                        let codec_packet = &self.codec_packet.as_ref().unwrap();
                        let config_packet = &self.config_packet.as_ref().unwrap();

                        buf.extend_from_slice(&codec_packet.header.serialize());
                        buf.extend_from_slice(&config_packet.header.serialize());
                        buf.extend_from_slice(&config_packet.payload);

                        new_stream.write_all(&buf).await?;

                        if let Some(mut old_stream) = self.current_stream.replace(new_stream) {
                            if let Err(err) = old_stream.close().await {
                                warn!("Could not close stream: {err}");
                            }
                        }
                    }

                    if let Some(stream) = &mut self.current_stream {
                        stream.write_all(&packet.header.serialize()).await?;
                        stream.write_all(&packet.payload).await?;
                    } else {
                        warn!(
                            "Unexpected missing current stream, a key packet has not been received"
                        );
                    }
                }
            }
            AVPacket::Hole(_) => panic!("Unexpected input hole packet"),
        }
        Ok(())
    }
}

#[derive(Debug)]
enum RecvMsg {
    NewStream(RecvStream),
    NewPacket {
        packet: AVPacket,
        codec_gen: u32,
        config_gen: u32,
    },
}

pub(crate) struct VideoGopStreamProtocolRecvDriver {
    tx: mpsc::Sender<RecvMsg>,
    rx: mpsc::Receiver<RecvMsg>,
    last_codec_gen: u32,
    last_config_gen: u32,
    accept_unis_task: Option<Task>,
    read_packets_task: Option<Task>,
}

impl VideoGopStreamProtocolRecvDriver {
    pub(crate) async fn start(ky_channel: KyChannel) -> Result<Self, ProtocolError> {
        let (tx, rx) = mpsc::channel(16);

        let tx2 = tx.clone();
        let accept_unis_task = Task::spawn_task(
            async move {
                let ret = Self::accept_unis(ky_channel, tx2).await;
                if let Err(err) = ret {
                    error!("accept_uni_streams() error: {err}");
                }
            },
            "accept_unis",
        );

        Ok(Self {
            tx,
            rx,
            last_codec_gen: 0,
            last_config_gen: 0,
            accept_unis_task: Some(accept_unis_task),
            read_packets_task: None,
        })
    }

    async fn accept_unis(
        mut ky_channel: KyChannel,
        tx: mpsc::Sender<RecvMsg>,
    ) -> Result<(), ProtocolError> {
        loop {
            let recv = ky_channel.accept_uni().await?;
            tx.send(RecvMsg::NewStream(recv)).await?;
        }
    }

    async fn read_packets(
        mut recv: RecvStream,
        tx: mpsc::Sender<RecvMsg>,
    ) -> Result<(), ProtocolError> {
        let mut ids = [0; 8];
        recv.read_exact(&mut ids).await?;
        let codec_gen = BigEndian::read_u32(&ids[..4]);
        let config_gen = BigEndian::read_u32(&ids[4..]);
        while let Some(packet) = av::read_packet(&mut recv).await? {
            tx.send(RecvMsg::NewPacket {
                packet,
                codec_gen,
                config_gen,
            })
            .await?;
        }
        Ok(())
    }
}

impl Drop for VideoGopStreamProtocolRecvDriver {
    fn drop(&mut self) {
        if let Some(accept_unis_task) = self.accept_unis_task.take() {
            accept_unis_task.cancel();
        }
        if let Some(read_packets_task) = self.read_packets_task.take() {
            read_packets_task.cancel();
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for VideoGopStreamProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                RecvMsg::NewStream(recv) => {
                    info!("NewStream");
                    let tx = self.tx.clone();
                    if let Some(task) = self.read_packets_task.take() {
                        // This is not totally optimal: we should abandon an
                        // old stream only once we received a complete frame on
                        // the new stream (in case we receive packets on an old
                        // stream before we receive a full frame on the new
                        // stream), but this is way more complex, so keep it
                        // simple: only keep the latest QUIC stream.
                        task.cancel();
                    }

                    let read_packets_task = Task::spawn_task(
                        async move {
                            let ret = Self::read_packets(recv, tx).await;
                            if let Err(err) = ret {
                                error!("read_packets() error: {err}");
                            }
                        },
                        "read_packets",
                    );

                    self.read_packets_task = Some(read_packets_task);
                }
                RecvMsg::NewPacket {
                    packet,
                    codec_gen,
                    config_gen,
                } => {
                    match packet {
                        AVPacket::Codec(_) => {
                            if codec_gen != self.last_codec_gen {
                                self.last_codec_gen = codec_gen;
                            }
                        }
                        AVPacket::Media(ref packet) => {
                            if !packet.header.is_config || config_gen != self.last_config_gen {
                                self.last_config_gen = config_gen;
                            }
                        }
                        AVPacket::Hole(_) => panic!("Unexpected input hole packet"),
                    }

                    return Ok(Some(packet));
                }
            }
        }

        Ok(None)
    }
}
