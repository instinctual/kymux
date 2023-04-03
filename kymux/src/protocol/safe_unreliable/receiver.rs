use super::forwarder;
use crate::protocol::Packet;
use crate::router::{KyChannelRecv, KyRecvMsg};
use crate::{Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use bytes::{Buf, Bytes};
use quinn::RecvStream;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;

pub(super) struct Receiver;

#[derive(Debug)]
pub(super) enum RecvMsg {
    Stream(StreamMsg),
    Datagram(DatagramMsg),
}

#[derive(Debug)]
pub(super) struct StreamMsg {
    pub(super) packet: Packet,
    pub(super) raw_kypacket_seq: u32,
    pub(super) raw_group_seq: u32,
}

#[derive(Debug)]
pub(super) struct DatagramMsg {
    pub(super) data: Bytes,
    pub(super) raw_kypacket_seq: u32,
    pub(super) raw_group_seq: u32,
    pub(super) datagram_number: u32,
    pub(super) end: bool,
}

impl Receiver {
    pub(super) fn new() -> Self {
        Self
    }

    pub(super) async fn recv_video(
        &self,
        ky_channel_rx: KyChannelRecv,
        client_tx: OwnedWriteHalf,
    ) -> Result<()> {
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let ret = Self::recv_ky_channel(ky_channel_rx, tx).await;
            if let Err(e) = ret {
                error!("Error: {e:?}");
            }
        });

        let mut forwarder = forwarder::Forwarder::new(client_tx);
        forwarder.forward_to_client(rx).await
    }

    async fn recv_ky_channel(
        mut ky_channel_rx: KyChannelRecv,
        tx: mpsc::Sender<RecvMsg>,
    ) -> Result<()> {
        let mut join_handle = None;
        loop {
            let msg = ky_channel_rx.recv().await?;
            match msg {
                KyRecvMsg::AcceptUni(recv_stream) => {
                    if join_handle.is_some() {
                        return Err(Error::KymuxProtocolError(
                            "Unexpected multiple unistreams".to_string(),
                        ));
                    }
                    let tx = tx.clone();
                    join_handle = Some(tokio::spawn(async move {
                        let ret = Self::read_packets(recv_stream, tx).await;
                        if let Err(e) = ret {
                            // Expected when the stream is closed
                            debug!("Read packets error: {e:?}");
                        }
                    }));
                }
                KyRecvMsg::Datagram(mut buf) => {
                    assert!(buf.len() >= 20);
                    let _endpoint_id = buf.get_u64();
                    let raw_kypacket_seq = buf.get_u32();
                    let raw_group_seq = buf.get_u32();
                    let datagram_number_and_end = buf.get_u32();
                    let datagram_number = datagram_number_and_end & ((1 << 31) - 1);
                    let end = (datagram_number_and_end & (1 << 31)) != 0;
                    tx.send(RecvMsg::Datagram(DatagramMsg {
                        data: buf,
                        raw_kypacket_seq,
                        raw_group_seq,
                        datagram_number,
                        end,
                    }))
                    .await
                    .map_err(|_| Error::KymuxProtocolError("Could not send message".to_string()))?
                }
                _ => {
                    return Err(Error::KymuxProtocolError(
                        "Unexpected KyRecvMsg".to_string(),
                    ));
                }
            }
        }
    }

    async fn read_packets(mut recv_stream: RecvStream, tx: mpsc::Sender<RecvMsg>) -> Result<()> {
        loop {
            let raw_kypacket_seq = recv_stream.read_u32().await?;
            let raw_group_seq = recv_stream.read_u32().await?;
            let packet = Packet::read(&mut recv_stream).await?;
            tx.send(RecvMsg::Stream(StreamMsg {
                packet,
                raw_kypacket_seq,
                raw_group_seq,
            }))
            .await
            .map_err(|_| Error::KymuxProtocolError("Could not send message".to_string()))?;
        }
    }
}
