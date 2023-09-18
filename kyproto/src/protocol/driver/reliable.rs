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
    send: Option<SendStream>,
}

pub(crate) struct ReliableProtocolRecvDriver {
    ky_channel: KyChannel,
    recv: Option<RecvStream>,
}

impl ReliableProtocolSendDriver {
    pub(crate) fn new(ky_channel: KyChannel) -> Self {
        Self {
            ky_channel,
            send: None,
        }
    }
}

impl ReliableProtocolRecvDriver {
    pub(crate) fn new(ky_channel: KyChannel) -> Self {
        Self {
            ky_channel,
            recv: None,
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for ReliableProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        if self.send.is_none() {
            let send = self.ky_channel.open_uni().await?;
            self.send = Some(send);
        }

        let send = self.send.as_mut().unwrap();

        match packet {
            AVPacket::Codec(packet) => {
                let header = packet.header.serialize();
                send.write_all(&header).await?;
            }
            AVPacket::Media(packet) => {
                let header = packet.header.serialize();
                send.write_all(&header).await?;
                send.write_all(&packet.payload).await?;
            }
        }

        Ok(())
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for ReliableProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        if self.recv.is_none() {
            loop {
                match self.ky_channel.recv().await? {
                    KyRecvMsg::AcceptUni(recv) => {
                        self.recv = Some(recv);
                        break;
                    }
                    KyRecvMsg::AcceptBi(..) => {
                        Err(ProtocolError("Unexpected accept_bi()".to_string()))?
                    }
                    KyRecvMsg::Datagram(..) => { /* ignore */ }
                }
            }
        }

        let recv = self.recv.as_mut().unwrap();

        util::read_packet(recv).await
    }
}
