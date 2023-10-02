#[cfg(all(not(feature = "js"), not(feature = "tokio-rt")))]
compile_error!("No feature selected, pass either --features=js or --features=tokio-rt");

use control::{Control, ControlMsg};
use error::*;
use router::{KyChannel, Router};

use kynet::error::ConnectionError;
use kynet::Connection;
use tokio::sync::{mpsc, oneshot};

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

pub struct VideoSendEndpoint {
    start_request_receiver: oneshot::Receiver<()>,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
}

impl VideoSendEndpoint {
    pub async fn started(self) -> Result<ProtocolSend<AVPacket>, ProtocolError> {
        self.start_request_receiver.await?;
        protocol::start_video_protocol_send(self.ky_channel, self.video_protocol).await
    }
}

pub struct VideoRecvEndpoint {
    endpoint_id: u16,
    control_msg_sender: mpsc::Sender<ControlMsg>,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
}

impl VideoRecvEndpoint {
    pub async fn start(self) -> Result<ProtocolRecv<AVPacket>, ProtocolError> {
        let msg = ControlMsg::RequestStart {
            endpoint_id: self.endpoint_id,
        };
        self.control_msg_sender.send(msg).await?;
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

    pub fn register_video_endpoint_send(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoSendEndpoint, EndpointAlreadyRegistered> {
        let ky_channel = self.router.register(id)?;
        let start_request_receiver = self.control.register_start_request_receiver(id)?;
        let endpoint = VideoSendEndpoint {
            start_request_receiver,
            ky_channel,
            video_protocol,
        };
        Ok(endpoint)
    }

    pub fn register_video_endpoint_recv(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoRecvEndpoint, EndpointAlreadyRegistered> {
        let ky_channel = self.router.register(id)?;
        let control_msg_sender = self.control.control_msg_sender().clone();
        let endpoint = VideoRecvEndpoint {
            endpoint_id: id,
            control_msg_sender,
            ky_channel,
            video_protocol,
        };
        Ok(endpoint)
    }
}
