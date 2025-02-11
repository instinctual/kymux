mod error;

#[cfg(feature = "ipc")]
pub mod ipc;

use std::time::Duration;

pub use error::{Error, Result};
pub use kyproto::{
    AVPacket, AudioClientEndpoint, AudioProtocol, AudioServerEndpoint, ConnectionStats,
    InputEndpoint, InputPacket, KyProto, ProtocolEndpoint, ProtocolRecv, ProtocolSend,
    StatsProvider, VideoClientEndpoint, VideoProtocol, VideoServerEndpoint,
};

#[allow(dead_code)]
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

pub enum ServerConfig {
    #[cfg(feature = "backend-quinn")]
    Quic {
        addr: std::net::SocketAddr,
        cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
        private_key: rustls::pki_types::PrivateKeyDer<'static>,
    },
    #[cfg(feature = "backend-wtransport")]
    Wtransport {
        addr: std::net::SocketAddr,
        cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
        private_key: rustls::pki_types::PrivateKeyDer<'static>,
    },
}

pub struct WebTransportCertificateHash {
    pub hash_algorithm: String,
    pub hash: String,
}

pub enum ClientConfig {
    #[cfg(feature = "backend-quinn")]
    Quic {
        addr: std::net::SocketAddr,
        roots: Option<rustls::RootCertStore>,
        server_name: String,
    },
    #[cfg(feature = "backend-webtransport-js")]
    WebTransport {
        url: String,
        certificate_hash: Option<WebTransportCertificateHash>,
    },
}

#[cfg(feature = "server")]
enum ServerInner {
    #[cfg(feature = "backend-quinn")]
    Quinn(kyproto::quinn::QuinnServer),
    #[cfg(feature = "backend-wtransport")]
    Wtransport(kyproto::wtransport::WTransportServer),
}

// Accept a single connection
#[cfg(feature = "server")]
pub struct Server {
    endpoint: ServerInner,
}

#[cfg(feature = "server")]
impl Server {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        // Setup quinn to accept connections
        let endpoint = match config {
            #[cfg(feature = "backend-quinn")]
            ServerConfig::Quic {
                addr,
                cert_chain,
                private_key,
            } => {
                let endpoint = kyproto::KyProto::quinn_start_server_on_addr(
                    addr,
                    cert_chain.into_iter().map(|c| c.into()).collect(),
                    private_key.into(),
                    &kyproto::quinn::QuinnServerOptions {
                        keep_alive_interval: Some(KEEP_ALIVE_INTERVAL),
                        ..Default::default()
                    },
                )?;

                ServerInner::Quinn(endpoint)
            }
            #[cfg(feature = "backend-wtransport")]
            ServerConfig::Wtransport {
                addr,
                cert_chain,
                private_key,
            } => {
                let endpoint = kyproto::KyProto::wtransport_start_server_on_addr(
                    addr,
                    cert_chain.into_iter().map(|c| c.into()).collect(),
                    private_key.into(),
                    &kyproto::wtransport::WTransportServerOptions {
                        keep_alive_interval: Some(KEEP_ALIVE_INTERVAL),
                        ..Default::default()
                    },
                )?;

                ServerInner::Wtransport(endpoint)
            }
        };

        Ok(Self { endpoint })
    }

    pub async fn accept(&self) -> Result<Connection> {
        let connection = match &self.endpoint {
            #[cfg(feature = "backend-quinn")]
            ServerInner::Quinn(endpoint) => endpoint.accept().await?,
            #[cfg(feature = "backend-wtransport")]
            ServerInner::Wtransport(endpoint) => endpoint.accept().await?,
        };

        Connection::new(connection)
    }

    pub fn close(&self, error_code: u32, reason: &str) {
        match &self.endpoint {
            #[cfg(feature = "backend-quinn")]
            ServerInner::Quinn(endpoint) => endpoint.close(error_code, reason),
            #[cfg(feature = "backend-wtransport")]
            ServerInner::Wtransport(endpoint) => endpoint.close(error_code, reason),
        };
    }

    pub async fn wait_idle(&self) {
        match &self.endpoint {
            #[cfg(feature = "backend-quinn")]
            ServerInner::Quinn(endpoint) => endpoint.wait_idle().await,
            #[cfg(feature = "backend-wtransport")]
            ServerInner::Wtransport(endpoint) => endpoint.wait_idle().await,
        };
    }
}

pub struct Connection {
    connection: kyproto::KyProto,
}

impl Connection {
    fn new(connection: kyproto::KyProto) -> Result<Self> {
        Ok(Self { connection })
    }

    pub async fn closed(&self) -> Result<()> {
        self.connection.closed().await?;
        Ok(())
    }

    pub async fn connection_stats(&self) -> ConnectionStats {
        self.connection.connection_stats().await
    }

    pub fn stats_provider(&self) -> KyProtoStatsProvider {
        self.connection.stats_provider()
    }

    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let connection = match config {
            #[cfg(feature = "backend-quinn")]
            ClientConfig::Quic {
                addr,
                roots,
                server_name,
            } => {
                kyproto::KyProto::quinn_connect(
                    addr,
                    &server_name,
                    roots.map(|roots| roots.into()),
                    &kyproto::quinn::QuinnClientOptions {
                        keep_alive_interval: Some(KEEP_ALIVE_INTERVAL),
                        ..Default::default()
                    },
                )
                .await?
            }
            #[cfg(feature = "backend-webtransport-js")]
            ClientConfig::WebTransport {
                url,
                certificate_hash,
            } => {
                use kyproto::webtransport_js::{
                    WebTransportJSCongestionControl, WebTransportJSHash, WebTransportJSOptions,
                };

                let server_certificate_hashes = if let Some(certificate_hash) = certificate_hash {
                    Some(vec![WebTransportJSHash::new_from_hex(
                        certificate_hash.hash_algorithm,
                        &certificate_hash.hash,
                    )?])
                } else {
                    None
                };

                let options = WebTransportJSOptions {
                    congestion_control: WebTransportJSCongestionControl::LowLatency,
                    require_unreliable: true,
                    server_certificate_hashes,
                };

                kyproto::KyProto::webtransport_js_connect(&url, &options).await?
            }
        };

        Self::new(connection)
    }

    pub async fn register_video_endpoint(
        &self,
        id: Option<u16>,
        video_protocol: VideoProtocol,
    ) -> Result<VideoServerEndpoint> {
        let endpoint = self
            .connection
            .register_video_endpoint(id, video_protocol)
            .await?;
        Ok(endpoint)
    }

    pub fn connect_video_endpoint(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoClientEndpoint> {
        let endpoint = self.connection.connect_video_endpoint(id, video_protocol)?;
        Ok(endpoint)
    }

    pub async fn register_audio_endpoint(
        &self,
        id: Option<u16>,
        audio_protocol: AudioProtocol,
    ) -> Result<AudioServerEndpoint> {
        let endpoint = self
            .connection
            .register_audio_endpoint(id, audio_protocol)
            .await?;
        Ok(endpoint)
    }

    pub fn connect_audio_endpoint(
        &self,
        id: u16,
        audio_protocol: AudioProtocol,
    ) -> Result<AudioClientEndpoint> {
        let endpoint = self.connection.connect_audio_endpoint(id, audio_protocol)?;
        Ok(endpoint)
    }

    pub async fn register_input_endpoint(&self, id: Option<u16>) -> Result<InputEndpoint> {
        let endpoint = self.connection.register_input_endpoint(id).await?;
        Ok(endpoint)
    }

    pub fn connect_input_endpoint(&self, id: u16) -> Result<InputEndpoint> {
        let endpoint = self.connection.connect_input_endpoint(id)?;
        Ok(endpoint)
    }
}
