#[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
pub use kynet::webtransport_js;

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub mod quinn {
    use crate::KyProto;

    use std::net::SocketAddr;

    use kynet::cert::{Certificate, PrivateKey};
    use kynet::error::ConnectionError;

    pub use kynet::quinn::{QuinnClientOptions, QuinnServerOptions};

    // Wrapper returning a KyProto on accept()
    pub struct QuinnServer(kynet::quinn::QuinnServer);

    impl QuinnServer {
        pub fn start_on_addr(
            addr: SocketAddr,
            cert: Certificate,
            key: PrivateKey,
            options: &QuinnServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server = kynet::quinn::QuinnServer::start_on_addr(addr, cert, key, options)?;
            Ok(Self(server))
        }

        pub fn start(
            port: u16,
            cert: Certificate,
            key: PrivateKey,
            options: &QuinnServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server = kynet::quinn::QuinnServer::start(port, cert, key, options)?;
            Ok(Self(server))
        }

        pub async fn accept(&self) -> Result<KyProto, ConnectionError> {
            let conn = self.0.accept().await?;
            let kyproto = KyProto::accept(conn).await?;
            Ok(kyproto)
        }

        pub fn reject_new_connections(&self) {
            self.0.reject_new_connections();
        }

        pub fn close(&self, error_code: u32, reason: &str) {
            self.0.close(error_code, reason);
        }

        pub async fn wait_idle(&self) {
            self.0.wait_idle().await;
        }
    }
}

#[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
pub mod wtransport {
    use crate::KyProto;

    use std::net::SocketAddr;

    use kynet::cert::{Certificate, PrivateKey};
    use kynet::error::ConnectionError;
    pub use kynet::wtransport::{WTransportClientOptions, WTransportServerOptions};

    // Wrapper returning a KyProto on accept()
    pub struct WTransportServer(kynet::wtransport::WTransportServer);

    impl WTransportServer {
        pub fn start_on_addr(
            addr: SocketAddr,
            cert: Certificate,
            key: PrivateKey,
            options: &WTransportServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server =
                kynet::wtransport::WTransportServer::start_on_addr(addr, cert, key, options)?;
            Ok(Self(server))
        }

        pub fn start(
            port: u16,
            cert: Certificate,
            key: PrivateKey,
            options: &WTransportServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server = kynet::wtransport::WTransportServer::start(port, cert, key, options)?;
            Ok(Self(server))
        }

        pub async fn accept(&self) -> Result<KyProto, ConnectionError> {
            let conn = self.0.accept().await?;
            let kyproto = KyProto::accept(conn).await?;
            Ok(kyproto)
        }
    }
}
