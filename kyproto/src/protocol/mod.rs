#![allow(unused)] // TODO remove

use crate::error::ProtocolError;
pub use crate::protocol::av::{
    AVPacket, AVPacketHeader, CodecPacket, CodecPacketHeader, MediaPacket, MediaPacketHeader,
};
use crate::protocol::driver::{ProtocolRecvDriver, ProtocolSendDriver};
pub use crate::protocol::input::InputPacket;
use crate::router::KyChannel;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod av;
pub(crate) mod driver;
mod input;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum VideoProtocol {
    Reliable,
    GopStream,
    Unreliable,
    UnreliableFec,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum AudioProtocol {
    Reliable,
}

pub(crate) async fn start_video_protocol_send(
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
) -> Result<ProtocolSend<AVPacket>, ProtocolError> {
    let protocol = match video_protocol {
        VideoProtocol::Reliable => ProtocolSend {
            driver: Box::new(
                driver::av::reliable::ReliableProtocolSendDriver::start(ky_channel).await?,
            ),
        },
        VideoProtocol::GopStream => ProtocolSend {
            driver: Box::new(
                driver::av::video_gopstream::VideoGopStreamProtocolSendDriver::start(ky_channel)
                    .await?,
            ),
        },
        VideoProtocol::Unreliable => ProtocolSend {
            driver: Box::new(
                driver::av::video_unreliable::VideoUnreliableProtocolSendDriver::start(ky_channel)
                    .await?,
            ),
        },
        VideoProtocol::UnreliableFec => ProtocolSend {
            driver: Box::new(
                driver::av::video_unreliable_fec::VideoUnreliableFecProtocolSendDriver::start(
                    ky_channel,
                )
                .await?,
            ),
        },
    };

    Ok(protocol)
}

pub(crate) async fn start_video_protocol_recv(
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
) -> Result<ProtocolRecv<AVPacket>, ProtocolError> {
    let protocol = match video_protocol {
        VideoProtocol::Reliable => ProtocolRecv {
            driver: Box::new(
                driver::av::reliable::ReliableProtocolRecvDriver::start(ky_channel).await?,
            ),
        },
        VideoProtocol::GopStream => ProtocolRecv {
            driver: Box::new(
                driver::av::video_gopstream::VideoGopStreamProtocolRecvDriver::start(ky_channel)
                    .await?,
            ),
        },
        VideoProtocol::Unreliable => ProtocolRecv {
            driver: Box::new(
                driver::av::video_unreliable::VideoUnreliableProtocolRecvDriver::start(ky_channel)
                    .await?,
            ),
        },
        VideoProtocol::UnreliableFec => ProtocolRecv {
            driver: Box::new(
                driver::av::video_unreliable_fec::VideoUnreliableFecProtocolRecvDriver::start(
                    ky_channel,
                )
                .await?,
            ),
        },
    };

    Ok(protocol)
}

pub(crate) async fn start_audio_protocol_send(
    ky_channel: KyChannel,
    audio_protocol: AudioProtocol,
) -> Result<ProtocolSend<AVPacket>, ProtocolError> {
    let protocol = match audio_protocol {
        AudioProtocol::Reliable => ProtocolSend {
            driver: Box::new(
                driver::av::reliable::ReliableProtocolSendDriver::start(ky_channel).await?,
            ),
        },
    };

    Ok(protocol)
}

pub(crate) async fn start_audio_protocol_recv(
    ky_channel: KyChannel,
    audio_protocol: AudioProtocol,
) -> Result<ProtocolRecv<AVPacket>, ProtocolError> {
    let protocol = match audio_protocol {
        AudioProtocol::Reliable => ProtocolRecv {
            driver: Box::new(
                driver::av::reliable::ReliableProtocolRecvDriver::start(ky_channel).await?,
            ),
        },
    };

    Ok(protocol)
}

pub(crate) async fn start_input_protocol(
    ky_channel: KyChannel,
) -> Result<(ProtocolSend<InputPacket>, ProtocolRecv<InputPacket>), ProtocolError> {
    let (ky_channel_recv, ky_channel_send) = ky_channel.into_split();

    let protocol_send = ProtocolSend {
        driver: Box::new(
            driver::input::reliable::ReliableProtocolSendDriver::start(ky_channel_send).await?,
        ),
    };
    let protocol_recv = ProtocolRecv {
        driver: Box::new(
            driver::input::reliable::ReliableProtocolRecvDriver::start(ky_channel_recv).await?,
        ),
    };

    Ok((protocol_send, protocol_recv))
}
