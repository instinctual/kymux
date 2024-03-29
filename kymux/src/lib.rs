mod error;

#[cfg(feature = "ipc")]
mod ipc;

use std::time::Duration;

pub use error::{Error, Result};
pub use kyproto::{
    AVPacket, AudioClientEndpoint, AudioServerEndpoint, InputEndpoint, InputPacket,
    ProtocolEndpoint, ProtocolRecv, ProtocolSend, VideoClientEndpoint, VideoProtocol,
    VideoServerEndpoint,
};

#[allow(dead_code)]
const KYMUX_LOCAL_CLIENTS_PORT: u16 = 9090;

#[allow(dead_code)]
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

pub enum ServerConfig {
    #[cfg(feature = "backend-quinn")]
    Quic {
        addr: std::net::SocketAddr,
        certificate: rustls::Certificate,
        private_key: rustls::PrivateKey,
    },
    #[cfg(feature = "backend-wtransport")]
    Wtransport {
        addr: std::net::SocketAddr,
        certificate: Vec<u8>,
        private_key: Vec<u8>,
    },
}

pub enum ClientConfig {
    #[cfg(feature = "backend-quinn")]
    Quic {
        addr: std::net::SocketAddr,
        roots: rustls::RootCertStore,
        server_name: String,
    },
    #[cfg(feature = "backend-webtransport-js")]
    WebTransport {
        url: String,
        certificate_hash_algorithm: String,
        certificate_hash: String,
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
                certificate,
                private_key,
            } => {
                let endpoint = kyproto::KyProto::quinn_start_server_on_addr(
                    addr,
                    certificate.into(),
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
                certificate,
                private_key,
            } => {
                let endpoint = kyproto::KyProto::wtransport_start_server_on_addr(
                    addr,
                    kyproto::cert::Certificate::new(certificate),
                    kyproto::cert::PrivateKey::new(private_key),
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

        Connection::new(
            connection,
            ConnectionParam {
                #[cfg(feature = "ipc")]
                local_clients_port: KYMUX_LOCAL_CLIENTS_PORT + 1,
            },
        )
        .await
    }

    pub fn reject_new_connections(&self) {
        match &self.endpoint {
            #[cfg(feature = "backend-quinn")]
            ServerInner::Quinn(endpoint) => endpoint.reject_new_connections(),
            #[cfg(feature = "backend-wtransport")]
            ServerInner::Wtransport(endpoint) => endpoint.reject_new_connections(),
        };
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

struct ConnectionParam {
    #[cfg(feature = "ipc")]
    local_clients_port: u16,
}

pub struct Connection {
    connection: kyproto::KyProto,
    #[cfg(feature = "ipc")]
    ipc: ipc::IpcHandler,
}

impl Connection {
    #[allow(unused_variables)]
    async fn new(connection: kyproto::KyProto, params: ConnectionParam) -> Result<Self> {
        Ok(Self {
            connection,
            #[cfg(feature = "ipc")]
            ipc: ipc::IpcHandler::new(params.local_clients_port).await?,
        })
    }

    pub async fn stop(&self) -> Result<()> {
        #[cfg(feature = "ipc")]
        self.ipc.stop().await?;

        Ok(())
    }

    pub async fn closed(&self) -> Result<()> {
        self.connection.closed().await?;
        Ok(())
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
                    roots.into(),
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
                certificate_hash_algorithm,
                certificate_hash,
            } => {
                use kyproto::webtransport_js::{
                    WebTransportJSCongestionControl, WebTransportJSHash, WebTransportJSOptions,
                };

                let options = WebTransportJSOptions {
                    congestion_control: WebTransportJSCongestionControl::LowLatency,
                    require_unreliable: true,
                    server_certificate_hashes: vec![WebTransportJSHash::new_from_hex(
                        certificate_hash_algorithm,
                        &certificate_hash,
                    )?],
                };

                kyproto::KyProto::webtransport_js_connect(&url, &options).await?
            }
        };

        Self::new(
            connection,
            ConnectionParam {
                #[cfg(feature = "ipc")]
                local_clients_port: KYMUX_LOCAL_CLIENTS_PORT,
            },
        )
        .await
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

    #[cfg(feature = "ipc")]
    pub async fn register_video_endpoint_with_ipc_forward(
        &self,
        id: Option<u16>,
        video_protocol: VideoProtocol,
    ) -> Result<(u16, String)> {
        let endpoint = self.register_video_endpoint(id, video_protocol).await?;
        let id = endpoint.id();

        let forwarder = self.ipc.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.ipc.forward(forwarder)?;

        Ok((id, uri))
    }

    pub fn connect_video_endpoint(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoClientEndpoint> {
        let endpoint = self.connection.connect_video_endpoint(id, video_protocol)?;
        Ok(endpoint)
    }

    #[cfg(feature = "ipc")]
    pub fn connect_video_endpoint_with_ipc_forward(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<String> {
        let endpoint = self.connect_video_endpoint(id, video_protocol)?;

        let forwarder = self.ipc.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.ipc.forward(forwarder)?;

        Ok(uri)
    }

    pub async fn register_audio_endpoint(&self, id: Option<u16>) -> Result<AudioServerEndpoint> {
        let endpoint = self.connection.register_audio_endpoint(id).await?;
        Ok(endpoint)
    }

    #[cfg(feature = "ipc")]
    pub async fn register_audio_endpoint_with_ipc_forward(
        &self,
        id: Option<u16>,
    ) -> Result<(u16, String)> {
        let endpoint = self.register_audio_endpoint(id).await?;
        let id = endpoint.id();

        let forwarder = self.ipc.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.ipc.forward(forwarder)?;

        Ok((id, uri))
    }

    pub fn connect_audio_endpoint(&self, id: u16) -> Result<AudioClientEndpoint> {
        let endpoint = self.connection.connect_audio_endpoint(id)?;
        Ok(endpoint)
    }

    #[cfg(feature = "ipc")]
    pub fn connect_audio_endpoint_with_ipc_forward(&self, id: u16) -> Result<String> {
        let endpoint = self.connect_audio_endpoint(id)?;

        let forwarder = self.ipc.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.ipc.forward(forwarder)?;

        Ok(uri)
    }

    pub async fn register_input_endpoint(&self, id: Option<u16>) -> Result<InputEndpoint> {
        let endpoint = self.connection.register_input_endpoint(id).await?;
        Ok(endpoint)
    }

    #[cfg(feature = "ipc")]
    pub async fn register_input_endpoint_with_ipc_forward(
        &self,
        id: Option<u16>,
    ) -> Result<(u16, String)> {
        let endpoint = self.register_input_endpoint(id).await?;
        let id = endpoint.id();

        let forwarder = self.ipc.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.ipc.forward(forwarder)?;

        Ok((id, uri))
    }

    pub fn connect_input_endpoint(&self, id: u16) -> Result<InputEndpoint> {
        let endpoint = self.connection.connect_input_endpoint(id)?;
        Ok(endpoint)
    }

    #[cfg(feature = "ipc")]
    pub fn connect_input_endpoint_with_ipc_forward(&self, id: u16) -> Result<String> {
        let endpoint = self.connect_input_endpoint(id)?;

        let forwarder = self.ipc.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.ipc.forward(forwarder)?;

        Ok(uri)
    }
}
