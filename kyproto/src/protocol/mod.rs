#![allow(unused)] // TODO remove

use crate::error::ProtocolError;
pub use crate::protocol::av::{
    AVPacket, AVPacketHeader, CodecPacket, CodecPacketHeader, MediaPacket, MediaPacketHeader,
};
use crate::protocol::driver::reliable::{ReliableProtocolRecvDriver, ReliableProtocolSendDriver};
use crate::protocol::driver::{ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;

use async_trait::async_trait;
use thiserror::Error;

mod av;
pub(crate) mod driver;

pub struct ProtocolSend<T> {
    driver: Box<dyn ProtocolSendDriver<Packet = T>>,
}

impl<T> ProtocolSend<T> {
    pub async fn send(&mut self, packet: T) -> Result<(), ProtocolError> {
        self.driver.send(packet).await
    }
}

pub struct ProtocolRecv<T> {
    driver: Box<dyn ProtocolRecvDriver<Packet = T>>,
}

impl<T> ProtocolRecv<T> {
    pub async fn recv(&mut self) -> Result<Option<T>, ProtocolError> {
        self.driver.recv().await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoProtocol {
    Reliable,
    GopStream,
    UnreliableFec,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamType {
    Video(VideoProtocol),
    Audio,
    Input,
}

pub(crate) async fn start_video_protocol_send(
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
) -> Result<ProtocolSend<AVPacket>, ProtocolError> {
    let driver = match video_protocol {
        VideoProtocol::Reliable => Box::new(ReliableProtocolSendDriver::start(ky_channel).await?),
        _ => unimplemented!(),
    };

    Ok(ProtocolSend { driver })
}

pub(crate) async fn start_video_protocol_recv(
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
) -> Result<ProtocolRecv<AVPacket>, ProtocolError> {
    let driver = match video_protocol {
        VideoProtocol::Reliable => Box::new(ReliableProtocolRecvDriver::start(ky_channel).await?),
        _ => unimplemented!(),
    };

    Ok(ProtocolRecv { driver })
}
