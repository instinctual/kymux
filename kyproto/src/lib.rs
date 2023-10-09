#[cfg(all(not(feature = "js"), not(feature = "tokio-rt")))]
compile_error!("No feature selected, pass either --features=js or --features=tokio-rt");

use control::{Control, ReadyNotifier};
use error::*;
use router::{KyChannel, Router};

use async_trait::async_trait;
use kynet::error::ConnectionError;
use kynet::Connection;

pub use protocol::{
    AVPacket, AVPacketHeader, CodecPacket, CodecPacketHeader, InputPacket, MediaPacket,
    MediaPacketHeader, ProtocolRecv, ProtocolSend, VideoProtocol,
};

mod control;
pub mod error;
mod protocol;
mod router;
mod runtime;
mod task;
mod util;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ProtocolEndpoint {
    type Protocol;

    fn id(&self) -> u16;
    async fn ready(self) -> Result<Self::Protocol, ProtocolError>;
}

pub struct VideoServerEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for VideoServerEndpoint {
    type Protocol = ProtocolSend<AVPacket>;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready(self) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier.ready().await?;
        protocol::start_video_protocol_send(self.ky_channel, self.video_protocol).await
    }
}

pub struct VideoClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for VideoClientEndpoint {
    type Protocol = ProtocolRecv<AVPacket>;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready(self) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier.ready().await?;
        protocol::start_video_protocol_recv(self.ky_channel, self.video_protocol).await
    }
}

pub struct AudioServerEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for AudioServerEndpoint {
    type Protocol = ProtocolSend<AVPacket>;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready(self) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier.ready().await?;
        protocol::start_audio_protocol_send(self.ky_channel).await
    }
}

pub struct AudioClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for AudioClientEndpoint {
    type Protocol = ProtocolRecv<AVPacket>;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready(self) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier.ready().await?;
        protocol::start_audio_protocol_recv(self.ky_channel).await
    }
}

pub struct InputEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for InputEndpoint {
    type Protocol = (ProtocolSend<InputPacket>, ProtocolRecv<InputPacket>);

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready(self) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier.ready().await?;
        protocol::start_input_protocol(self.ky_channel).await
    }
}

pub struct KyProto {
    router: Router,
    control: Control,
}

impl KyProto {
    pub async fn connect(conn: Connection) -> Result<Self, ConnectionError> {
        let (tx, rx) = conn.open_bi().await?;
        let control = Control::start(tx, rx);
        let router = Router::start(conn);
        Ok(Self { router, control })
    }

    pub async fn accept(conn: Connection) -> Result<Self, ConnectionError> {
        let (tx, rx) = conn.accept_bi().await?;
        let control = Control::start(tx, rx);
        let router = Router::start(conn);
        Ok(Self { router, control })
    }

    pub async fn register_video_endpoint(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoServerEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        self.control.register_endpoint(id).await?;
        let ky_channel = self.router.register(id)?;
        let endpoint = VideoServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
            video_protocol,
        };
        Ok(endpoint)
    }

    pub fn connect_video_endpoint(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoClientEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        let ky_channel = self.router.register(id)?;
        let endpoint = VideoClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
            video_protocol,
        };
        Ok(endpoint)
    }

    pub async fn register_audio_endpoint(
        &self,
        id: u16,
    ) -> Result<AudioServerEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        self.control.register_endpoint(id).await?;
        let ky_channel = self.router.register(id)?;
        let endpoint = AudioServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn connect_audio_endpoint(&self, id: u16) -> Result<AudioClientEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        let ky_channel = self.router.register(id)?;
        let endpoint = AudioClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub async fn register_input_endpoint(&self, id: u16) -> Result<InputEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        self.control.register_endpoint(id).await?;
        let ky_channel = self.router.register(id)?;
        let endpoint = InputEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn connect_input_endpoint(&self, id: u16) -> Result<InputEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        let ky_channel = self.router.register(id)?;
        let endpoint = InputEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }
}
