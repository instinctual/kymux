use crate::driver::{ConnectionDriver, RecvStreamDriver, SendStreamDriver};
use crate::error::*;

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
use crate::driver::quinn::QuinnConnectionDriver;

#[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
use crate::driver::webtransport_js::WebTransportJSConnectionDriver;

#[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
use crate::driver::wtransport::WTransportConnectionDriver;

use std::fmt::Debug;

#[cfg(target_family = "wasm")]
use std::rc::Rc;
#[cfg(not(target_family = "wasm"))]
use std::sync::Arc;

#[cfg(all(
    any(feature = "kynet-quinn", feature = "kynet-wtransport"),
    not(target_family = "wasm")
))]
use std::net::SocketAddr;

use bytes::Bytes;

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub mod quinn {
    pub use crate::driver::quinn::{QuinnClientOptions, QuinnServer, QuinnServerOptions};
}

#[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
pub mod webtransport_js {
    pub use crate::driver::webtransport_js::{
        DecodeHexError, WebTransportJSCongestionControl, WebTransportJSHash, WebTransportJSOptions,
    };
}

#[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
pub mod wtransport {
    pub use crate::driver::wtransport::{
        WTransportClientOptions, WTransportServer, WTransportServerOptions,
    };
}

#[cfg(all(
    any(feature = "kynet-quinn", feature = "kynet-wtransport"),
    not(target_family = "wasm")
))]
pub mod cert;

mod driver;
pub mod error;

// This abtraction does not handle the QUIC or WebTransport connection
// creation, but only streams and datagram once the connection is created

#[derive(Debug, Clone)]
pub struct Connection {
    #[cfg(not(target_family = "wasm"))]
    driver: Arc<dyn ConnectionDriver + Sync + Send>,
    #[cfg(target_family = "wasm")]
    driver: Rc<dyn ConnectionDriver>,
}

impl Connection {
    #[cfg(not(target_family = "wasm"))]
    pub fn new<T: ConnectionDriver + Sync + Send + 'static>(driver: T) -> Self {
        Self {
            driver: Arc::new(driver),
        }
    }

    #[cfg(target_family = "wasm")]
    pub fn new<T: ConnectionDriver + 'static>(driver: T) -> Self {
        Self {
            driver: Rc::new(driver),
        }
    }

    #[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
    pub async fn quinn_connect(
        addr: SocketAddr,
        server_name: &str,
        certs: cert::RootCertStore,
        options: &quinn::QuinnClientOptions,
    ) -> Result<Self, ConnectionError> {
        QuinnConnectionDriver::connect(addr, server_name, certs, options).await
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
        WebTransportJSConnectionDriver::connect(url, options).await
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

    #[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
    pub async fn wtransport_connect(
        url: &str,
        certs: cert::RootCertStore,
        options: &wtransport::WTransportClientOptions,
    ) -> Result<Self, ConnectionError> {
        WTransportConnectionDriver::connect(url, certs, options).await
    }

    pub async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        self.driver.open_uni().await
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        self.driver.open_bi().await
    }

    pub async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        self.driver.accept_uni().await
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        self.driver.accept_bi().await
    }

    pub async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        self.driver.read_datagram().await
    }

    pub async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        self.driver.send_datagram(data).await
    }

    pub async fn closed(&self) -> Result<(), ConnectionError> {
        self.driver.closed().await
    }

    pub fn close(&self, error_code: u32, reason: &str) {
        self.driver.close(error_code, reason)
    }

    pub fn max_datagram_size(&self) -> Option<usize> {
        self.driver.max_datagram_size()
    }
}

#[derive(Debug)]
pub struct SendStream {
    #[cfg(not(target_family = "wasm"))]
    driver: Box<dyn SendStreamDriver + Sync + Send>,
    #[cfg(target_family = "wasm")]
    driver: Box<dyn SendStreamDriver>,
}

impl SendStream {
    #[cfg(not(target_family = "wasm"))]
    pub fn new<T: SendStreamDriver + Sync + Send + 'static>(driver: T) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    #[cfg(target_family = "wasm")]
    pub fn new<T: SendStreamDriver + 'static>(driver: T) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        self.driver.write(buf).await
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        self.driver.write_all(buf).await
    }

    pub async fn close(&mut self) -> Result<(), WriteError> {
        self.driver.close().await
    }

    pub async fn abort(self) -> Result<(), UnknownStreamError> {
        self.driver.abort().await
    }
}

#[derive(Debug)]
pub struct RecvStream {
    #[cfg(not(target_family = "wasm"))]
    driver: Box<dyn RecvStreamDriver + Sync + Send>,
    #[cfg(target_family = "wasm")]
    driver: Box<dyn RecvStreamDriver>,
}

impl RecvStream {
    #[cfg(not(target_family = "wasm"))]
    pub fn new<T: RecvStreamDriver + Sync + Send + 'static>(driver: T) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    #[cfg(target_family = "wasm")]
    pub fn new<T: RecvStreamDriver + 'static>(driver: T) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        self.driver.read(buf).await
    }

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError> {
        self.driver.read_exact(buf).await
    }
}
