#[cfg(all(not(feature = "js"), not(feature = "tokio-rt")))]
compile_error!("No feature selected, pass either --features=js or --features=tokio-rt");

use control::{Control, ReadyNotifier};
use error::*;
use router::{KyChannel, Router};

use kynet::error::ConnectionError;
use kynet::Connection;

pub use protocol::{
    AVPacket, AVPacketHeader, CodecPacket, CodecPacketHeader, MediaPacket, MediaPacketHeader,
    ProtocolRecv, ProtocolSend, VideoProtocol,
};

mod control;
pub mod error;
mod protocol;
mod router;
mod runtime;
mod task;
mod util;

pub struct VideoServerEndpoint {
    pub id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
}

impl VideoServerEndpoint {
    pub async fn ready(self) -> Result<ProtocolSend<AVPacket>, ProtocolError> {
        self.ready_notifier.ready().await?;
        protocol::start_video_protocol_send(self.ky_channel, self.video_protocol).await
    }
}

pub struct VideoClientEndpoint {
    pub id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
}

impl VideoClientEndpoint {
    pub async fn ready(self) -> Result<ProtocolRecv<AVPacket>, ProtocolError> {
        self.ready_notifier.ready().await?;
        protocol::start_video_protocol_recv(self.ky_channel, self.video_protocol).await
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
}
