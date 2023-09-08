#[cfg(all(not(feature = "js"), not(feature = "tokio-rt")))]
compile_error!("No feature selected, pass either --features=js or --features=tokio-rt");

use control::{Control, ControlMsg};
use error::*;
use router::Router;

use kynet::error::ConnectionError;
use kynet::Connection;
use tokio::sync::{mpsc, oneshot};

pub use protocol::{AVPacket, ProtocolRecv, ProtocolSend, VideoProtocol};

mod control;
mod error;
mod protocol;
mod router;
mod runtime;
mod task;
mod util;

pub struct VideoSendEndpoint {
    start_request_receiver: oneshot::Receiver<()>,
    protocol_send: ProtocolSend<AVPacket>,
}

impl VideoSendEndpoint {
    pub async fn started(self) -> Result<ProtocolSend<AVPacket>, ProtocolStartError> {
        self.start_request_receiver.await?;
        Ok(self.protocol_send)
    }
}

pub struct VideoRecvEndpoint {
    endpoint_id: u16,
    control_msg_sender: mpsc::Sender<ControlMsg>,
    protocol_recv: ProtocolRecv<AVPacket>,
}

impl VideoRecvEndpoint {
    pub async fn start(self) -> Result<ProtocolRecv<AVPacket>, ProtocolStartError> {
        let msg = ControlMsg::RequestStart {
            endpoint_id: self.endpoint_id,
        };
        self.control_msg_sender.send(msg).await?;
        Ok(self.protocol_recv)
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
        let protocol_send = protocol::create_video_protocol_send(ky_channel, video_protocol);
        let start_request_receiver = self.control.register_start_request_receiver(id)?;
        let endpoint = VideoSendEndpoint {
            start_request_receiver,
            protocol_send,
        };
        Ok(endpoint)
    }

    pub fn register_video_endpoint_recv(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoRecvEndpoint, EndpointAlreadyRegistered> {
        let ky_channel = self.router.register(id)?;
        let protocol_recv = protocol::create_video_protocol_recv(ky_channel, video_protocol);
        let control_msg_sender = self.control.control_msg_sender().clone();
        let endpoint = VideoRecvEndpoint {
            endpoint_id: id,
            control_msg_sender,
            protocol_recv,
        };
        Ok(endpoint)
    }
}

impl From<oneshot::error::RecvError> for ProtocolStartError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self
    }
}

impl<T> From<mpsc::error::SendError<T>> for ProtocolStartError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self
    }
}
