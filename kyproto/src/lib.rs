#[cfg(all(not(feature = "js"), not(feature = "tokio-rt")))]
compile_error!("No feature selected, pass either --features=js or --features=tokio-rt");

use std::sync::atomic::{AtomicU16, Ordering};

use clock_sync::{ClockSyncClientProtocol, ClockSyncServerProtocol};
use control::{Control, ReadyNotifier};
use error::*;
use router::{KyChannel, Router};

use async_trait::async_trait;
use kynet::Connection;

pub use protocol::{
    AVPacket, AVPacketHeader, AudioProtocol, CodecPacket, CodecPacketHeader, HolePacket,
    HolePacketHeader, InputPacket, MediaPacket, MediaPacketHeader, ProtocolRecv, ProtocolSend,
    VideoProtocol,
};

mod clock_sync;
mod connection;
mod control;
pub mod error;
mod protocol;
mod router;
pub mod runtime;
mod task;
mod util;

#[cfg(all(
    any(feature = "kynet-quinn", feature = "kynet-wtransport"),
    not(target_family = "wasm")
))]
pub use {kynet::cert, std::net::SocketAddr};

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub use connection::quinn;

#[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
pub use connection::webtransport_js;

#[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
pub use connection::wtransport;

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
    audio_protocol: AudioProtocol,
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
        protocol::start_audio_protocol_send(self.ky_channel, self.audio_protocol).await
    }
}

pub struct AudioClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    audio_protocol: AudioProtocol,
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
        protocol::start_audio_protocol_recv(self.ky_channel, self.audio_protocol).await
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

pub struct ClockSyncServerEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for ClockSyncServerEndpoint {
    type Protocol = ClockSyncServerProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready(self) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier.ready().await?;
        Ok(ClockSyncServerProtocol::new(self.ky_channel))
    }
}

pub struct ClockSyncClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for ClockSyncClientEndpoint {
    type Protocol = ClockSyncClientProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready(self) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier.ready().await?;
        Ok(ClockSyncClientProtocol::new(self.ky_channel))
    }
}

const INITIATOR_SERVER: u16 = 0;
const INITIATOR_CLIENT: u16 = 1;

pub struct KyProto {
    conn: Connection,
    router: Router,
    control: Control,

    // Endpoint ID generation: ids can be allocated by both sides.
    // Like Quic streams, use u16 parity to avoid collision.
    //
    // The parity bit is defined by INITIATOR_SERVER/INITIATOR_CLIENT.
    //
    // quinn example: https://github.com/quinn-rs/quinn/blob/e652b6d999f053ffe21eeea247854882ae480281/quinn-proto/src/lib.rs#L230
    next_endpoint_index: AtomicU16,
    initiator: u16,
}

impl KyProto {
    pub async fn connect(conn: Connection) -> Result<Self, ConnectionError> {
        let (mut tx, rx) = conn.open_bi().await?;

        // Force a packet to be sent so that accept_bi() can detect bi-stream opening
        tx.write(&[0])
            .await
            .map_err(|_| ConnectionError("Dummy byte write failed".to_string()))?;

        let control = Control::start(tx, rx);
        let router = Router::start(conn.clone());
        Ok(Self {
            conn,
            router,
            control,
            initiator: INITIATOR_CLIENT,
            next_endpoint_index: AtomicU16::new(0),
        })
    }

    pub async fn accept(conn: Connection) -> Result<Self, ConnectionError> {
        let (tx, mut rx) = conn.accept_bi().await?;

        // Consume the dummy byte used to detect the bi-stream immediately
        rx.read(&mut [0])
            .await
            .map_err(|_| ConnectionError("Dummy byte read failed".to_string()))?;

        let control = Control::start(tx, rx);
        let router = Router::start(conn.clone());
        Ok(Self {
            conn,
            router,
            control,
            initiator: INITIATOR_SERVER,
            next_endpoint_index: AtomicU16::new(0),
        })
    }

    #[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
    pub async fn quinn_connect(
        addr: SocketAddr,
        server_name: &str,
        certs: cert::RootCertStore,
        options: &quinn::QuinnClientOptions,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::quinn_connect(addr, server_name, certs, options).await?;
        Self::connect(conn).await
    }

    #[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
    pub fn quinn_start_server_on_addr(
        addr: SocketAddr,
        cert: cert::Certificate,
        key: cert::PrivateKey,
        options: &quinn::QuinnServerOptions,
    ) -> Result<quinn::QuinnServer, ConnectionError> {
        quinn::QuinnServer::start_on_addr(addr, cert, key, options)
    }

    #[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
    pub fn quinn_start_server(
        port: u16,
        cert: cert::Certificate,
        key: cert::PrivateKey,
        options: &quinn::QuinnServerOptions,
    ) -> Result<quinn::QuinnServer, ConnectionError> {
        quinn::QuinnServer::start(port, cert, key, options)
    }

    #[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
    pub async fn webtransport_js_connect(
        url: &str,
        options: &webtransport_js::WebTransportJSOptions,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::webtransport_js_connect(url, options).await?;
        Self::connect(conn).await
    }

    #[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
    pub async fn wtransport_connect(
        url: &str,
        certs: cert::RootCertStore,
        options: &wtransport::WTransportClientOptions,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::wtransport_connect(url, certs, options).await?;
        Self::connect(conn).await
    }

    #[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
    pub fn wtransport_start_server_on_addr(
        addr: SocketAddr,
        cert: cert::Certificate,
        key: cert::PrivateKey,
        options: &wtransport::WTransportServerOptions,
    ) -> Result<wtransport::WTransportServer, ConnectionError> {
        wtransport::WTransportServer::start_on_addr(addr, cert, key, options)
    }

    #[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
    pub fn wtransport_start_server(
        port: u16,
        cert: cert::Certificate,
        key: cert::PrivateKey,
        options: &wtransport::WTransportServerOptions,
    ) -> Result<wtransport::WTransportServer, ConnectionError> {
        wtransport::WTransportServer::start(port, cert, key, options)
    }

    fn get_endpoint_id(&self, id: Option<u16>) -> u16 {
        if let Some(id) = id {
            id
        } else {
            let index = self.next_endpoint_index.fetch_add(1, Ordering::Relaxed);
            (index << 1) | self.initiator
        }
    }

    pub async fn register_video_endpoint(
        &self,
        id: Option<u16>,
        video_protocol: VideoProtocol,
    ) -> Result<VideoServerEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
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
        id: Option<u16>,
        audio_protocol: AudioProtocol,
    ) -> Result<AudioServerEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
        let ready_notifier = self.control.register_ready_notifier(id)?;
        self.control.register_endpoint(id).await?;
        let ky_channel = self.router.register(id)?;
        let endpoint = AudioServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
            audio_protocol,
        };
        Ok(endpoint)
    }

    pub fn connect_audio_endpoint(
        &self,
        id: u16,
        audio_protocol: AudioProtocol,
    ) -> Result<AudioClientEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        let ky_channel = self.router.register(id)?;
        let endpoint = AudioClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
            audio_protocol,
        };
        Ok(endpoint)
    }

    pub async fn register_input_endpoint(
        &self,
        id: Option<u16>,
    ) -> Result<InputEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
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

    pub async fn register_clock_sync_endpoint(
        &self,
        id: u16,
    ) -> Result<ClockSyncServerEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        self.control.register_endpoint(id).await?;
        let ky_channel = self.router.register(id)?;
        let endpoint = ClockSyncServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn connect_clock_sync_endpoint(
        &self,
        id: u16,
    ) -> Result<ClockSyncClientEndpoint, ProtocolError> {
        let ready_notifier = self.control.register_ready_notifier(id)?;
        let ky_channel = self.router.register(id)?;
        let endpoint = ClockSyncClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub async fn closed(&self) -> Result<(), ConnectionError> {
        self.conn.closed().await
    }
}

impl Drop for KyProto {
    fn drop(&mut self) {
        self.conn.close(0, "KyProto closed");
    }
}
