use crate::protocol::av::{AVPacket, AVPacketHeader, CodecPacket, MediaPacket};
use crate::protocol::driver::util;
use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::{KyChannel, KyRecvMsg};

use async_trait::async_trait;
use bytes::BytesMut;
use kynet::error::ReadExactError;
use kynet::{RecvStream, SendStream};

pub(crate) struct ReliableProtocolSendDriver {
    ky_channel: KyChannel,
    send: SendStream,
}

impl ReliableProtocolSendDriver {
    pub(crate) async fn start(ky_channel: KyChannel) -> Result<Self, ProtocolError> {
        let send = ky_channel.open_uni().await?;
        Ok(Self { ky_channel, send })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for ReliableProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        match packet {
            AVPacket::Codec(packet) => {
                let header = packet.header.serialize();
                self.send.write_all(&header).await?;
            }
            AVPacket::Media(packet) => {
                let header = packet.header.serialize();
                self.send.write_all(&header).await?;
                self.send.write_all(&packet.payload).await?;
            }
        }

        Ok(())
    }
}

pub(crate) struct ReliableProtocolRecvDriver {
    ky_channel: KyChannel,
    recv: RecvStream,
}

impl ReliableProtocolRecvDriver {
    pub(crate) async fn start(mut ky_channel: KyChannel) -> Result<Self, ProtocolError> {
        let recv = loop {
            match ky_channel.recv().await? {
                KyRecvMsg::AcceptUni(recv) => {
                    break recv;
                }
                KyRecvMsg::AcceptBi(..) => {
                    Err(ProtocolError("Unexpected accept_bi()".to_string()))?
                }
                KyRecvMsg::Datagram(..) => { /* ignore */ }
            }
        };
        Ok(Self { ky_channel, recv })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for ReliableProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        util::read_packet(&mut self.recv).await
    }
}
