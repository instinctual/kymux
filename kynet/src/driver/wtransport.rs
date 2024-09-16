use crate::cert::{Certificate, PrivateKey, RootCertStore};
use crate::error::*;
use crate::{
    Connection, ConnectionDriver, RecvStream, RecvStreamDriver, SendStream, SendStreamDriver,
};

use std::net::{Ipv6Addr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
#[allow(unused)]
use log::{debug, error, info, warn};

impl From<wtransport::Connection> for Connection {
    fn from(value: wtransport::Connection) -> Self {
        Self::new(WTransportConnectionDriver::wrap(value))
    }
}

#[derive(Default)]
pub struct WTransportServerOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
}

pub struct WTransportServer {
    endpoint: wtransport::Endpoint<wtransport::endpoint::endpoint_side::Server>,
}

impl WTransportServer {
    pub fn start_on_addr(
        addr: SocketAddr,
        cert_chain: Vec<Certificate>,
        key: PrivateKey,
        options: &WTransportServerOptions,
    ) -> Result<Self, ConnectionError> {
        let cert_chain = cert_chain.into_iter().map(|c| c.0).collect();

        let mut tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key.0)?;
        tls_config.alpn_protocols = vec![wtransport_proto::WEBTRANSPORT_ALPN.to_vec()];

        let config = wtransport::ServerConfig::builder()
            .with_bind_address(addr)
            .with_custom_tls(tls_config)
            .max_idle_timeout(options.max_idle_timeout)?
            .keep_alive_interval(options.keep_alive_interval)
            .build();

        let endpoint = wtransport::Endpoint::server(config)?;
        Ok(Self { endpoint })
    }

    pub fn start(
        port: u16,
        cert_chain: Vec<Certificate>,
        key: PrivateKey,
        options: &WTransportServerOptions,
    ) -> Result<Self, ConnectionError> {
        let addr = SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port);
        Self::start_on_addr(addr, cert_chain, key, options)
    }

    pub async fn accept(&self) -> Result<Connection, ConnectionError> {
        let request = self.endpoint.accept().await.await?;
        let conn = request.accept().await?;
        Ok(conn.into())
    }

    pub fn close(&self, error_code: u32, reason: &str) {
        let var_int = wtransport_proto::varint::VarInt::from(error_code);
        self.endpoint.close(var_int, reason.as_bytes());
    }

    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await;
    }
}

#[derive(Default)]
pub struct WTransportClientOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct WTransportConnectionDriver {
    conn: wtransport::Connection,
}

impl WTransportConnectionDriver {
    fn wrap(conn: wtransport::Connection) -> Self {
        Self { conn }
    }

    pub async fn connect(
        url: &str,
        certs: Option<RootCertStore>,
        options: &WTransportClientOptions,
    ) -> Result<Connection, ConnectionError> {
        let certs = if let Some(certs) = certs {
            certs
        } else {
            let mut cert_store = RootCertStore::empty();

            for cert in rustls_native_certs::load_native_certs().certs {
                cert_store.add(Certificate(cert))?;
            }

            cert_store
        };

        let mut tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(certs.0)
            .with_no_client_auth();
        tls_config.alpn_protocols = vec![wtransport_proto::WEBTRANSPORT_ALPN.to_vec()];

        let config = wtransport::ClientConfig::builder()
            .with_bind_default()
            .with_custom_tls(tls_config)
            .max_idle_timeout(options.max_idle_timeout)?
            .keep_alive_interval(options.keep_alive_interval)
            .build();

        let endpoint = wtransport::Endpoint::client(config)?;
        let conn = endpoint.connect(url).await?;
        Ok(conn.into())
    }
}

#[async_trait]
impl ConnectionDriver for WTransportConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        let wt_send = self.conn.open_uni().await?.await?;
        let send_driver = WTransportSendStreamDriver::new(wt_send);
        let send = SendStream::new(send_driver);
        Ok(send)
    }

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (wt_send, wt_recv) = self.conn.open_bi().await?.await?;
        let send_driver = WTransportSendStreamDriver::new(wt_send);
        let recv_driver = WTransportRecvStreamDriver::new(wt_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        let wt_recv = self.conn.accept_uni().await?;
        let recv_driver = WTransportRecvStreamDriver::new(wt_recv);
        let recv = RecvStream::new(recv_driver);
        Ok(recv)
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (wt_send, wt_recv) = self.conn.accept_bi().await?;
        let send_driver = WTransportSendStreamDriver::new(wt_send);
        let recv_driver = WTransportRecvStreamDriver::new(wt_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        let datagram = self.conn.receive_datagram().await?;
        let bytes = Bytes::copy_from_slice(&datagram);
        Ok(bytes)
    }

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        self.conn.send_datagram(data)?;
        Ok(())
    }

    async fn closed(&self) -> Result<(), ConnectionError> {
        match self.conn.closed().await {
            wtransport::error::ConnectionError::LocallyClosed
            | wtransport::error::ConnectionError::ApplicationClosed(_) => Ok(()),
            err => Err(err.into()),
        }
    }

    fn close(&self, error_code: u32, reason: &str) {
        self.conn.close(
            wtransport_proto::varint::VarInt::from(error_code),
            reason.as_bytes(),
        );
    }

    fn max_datagram_size(&self) -> Option<usize> {
        self.conn.max_datagram_size()
    }
}

#[derive(Debug)]
struct WTransportSendStreamDriver {
    send: wtransport::SendStream,
}

impl WTransportSendStreamDriver {
    fn new(send: wtransport::SendStream) -> Self {
        Self { send }
    }
}

#[async_trait]
impl SendStreamDriver for WTransportSendStreamDriver {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        let size = self.send.write(buf).await?;
        Ok(size)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        self.send.write_all(buf).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ClosedStreamError> {
        self.send.finish().await.map_err(|_| ClosedStreamError)?;
        Ok(())
    }

    async fn abort(mut self: Box<Self>) -> Result<(), ClosedStreamError> {
        // Not async, but the trait requires the method to be async, because
        // other implementations might abort asynchronously
        self.send
            .reset(wtransport_proto::varint::VarInt::from_u32(0))
            .map_err(|_| ClosedStreamError)?;
        Ok(())
    }
}

#[derive(Debug)]
struct WTransportRecvStreamDriver {
    recv: wtransport::RecvStream,
}

impl WTransportRecvStreamDriver {
    fn new(recv: wtransport::RecvStream) -> Self {
        Self { recv }
    }
}

#[async_trait]
impl RecvStreamDriver for WTransportRecvStreamDriver {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        let size = self.recv.read(buf).await?;
        Ok(size)
    }
}

impl From<wtransport::error::ConnectionError> for ConnectionError {
    fn from(value: wtransport::error::ConnectionError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::ConnectingError> for ConnectionError {
    fn from(value: wtransport::error::ConnectingError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::StreamOpeningError> for ConnectionError {
    fn from(value: wtransport::error::StreamOpeningError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::StreamReadError> for ReadError {
    fn from(value: wtransport::error::StreamReadError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::StreamWriteError> for WriteError {
    fn from(value: wtransport::error::StreamWriteError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::SendDatagramError> for SendDatagramError {
    fn from(value: wtransport::error::SendDatagramError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::config::InvalidIdleTimeout> for ConnectionError {
    fn from(_: wtransport::config::InvalidIdleTimeout) -> Self {
        Self("Invalid max_idle_timeout".to_string())
    }
}
