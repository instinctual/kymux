use crate::cert::{Certificate, PrivateKey, RootCertStore};
use crate::error::*;
use crate::{
    Connection, ConnectionDriver, RecvStream, RecvStreamDriver, SendStream, SendStreamDriver,
};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

impl From<quinn::Connection> for Connection {
    fn from(value: quinn::Connection) -> Self {
        Self::new(QuinnConnectionDriver::wrap(value))
    }
}

#[derive(Default)]
pub struct QuinnServerOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
}

pub struct QuinnServer {
    endpoint: quinn::Endpoint,
}

impl QuinnServer {
    pub fn start_on_addr(
        addr: SocketAddr,
        cert: Certificate,
        key: PrivateKey,
        options: &QuinnServerOptions,
    ) -> Result<Self, ConnectionError> {
        let cert_chain = vec![cert.0];
        let mut config = quinn::ServerConfig::with_single_cert(cert_chain, key.0)?;

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(
            options
                .max_idle_timeout
                .map(quinn::IdleTimeout::try_from)
                .transpose()
                .map_err(|_| ConnectionError("Invalid max_idle_timeout".to_string()))?,
        );
        transport_config.keep_alive_interval(options.keep_alive_interval);
        config.transport_config(Arc::new(transport_config));

        let endpoint = quinn::Endpoint::server(config, addr)?;
        Ok(Self { endpoint })
    }

    pub fn start(
        port: u16,
        cert: Certificate,
        key: PrivateKey,
        options: &QuinnServerOptions,
    ) -> Result<Self, ConnectionError> {
        let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);
        Self::start_on_addr(addr, cert, key, options)
    }

    pub async fn accept(&self) -> Result<Connection, ConnectionError> {
        let connecting = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| ConnectionError("Endpoint closed".to_string()))?;
        let connection = connecting.await?;
        Ok(connection.into())
    }

    pub fn reject_new_connections(&self) {
        self.endpoint.reject_new_connections();
    }

    pub fn close(&self, error_code: u32, reason: &str) {
        let var_int = quinn::VarInt::from_u32(error_code);
        self.endpoint.close(var_int, reason.as_bytes());
    }

    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await;
    }
}

#[derive(Default)]
pub struct QuinnClientOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct QuinnConnectionDriver {
    conn: quinn::Connection,
}

impl QuinnConnectionDriver {
    fn wrap(conn: quinn::Connection) -> Self {
        Self { conn }
    }

    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        certs: Option<RootCertStore>,
        options: &QuinnClientOptions,
    ) -> Result<Connection, ConnectionError> {
        let mut config = if let Some(certs) = certs {
            quinn::ClientConfig::with_root_certificates(certs.0)
        } else {
            quinn::ClientConfig::with_native_roots()
        };

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(
            options
                .max_idle_timeout
                .map(quinn::IdleTimeout::try_from)
                .transpose()
                .map_err(|_| ConnectionError("Invalid max_idle_timeout".to_string()))?,
        );
        transport_config.keep_alive_interval(options.keep_alive_interval);
        config.transport_config(Arc::new(transport_config));

        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        let endpoint = quinn::Endpoint::client(bind_addr)?;
        let conn = endpoint.connect_with(config, addr, server_name)?.await?;
        Ok(conn.into())
    }
}

#[async_trait]
impl ConnectionDriver for QuinnConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        let quinn_send = self.conn.open_uni().await?;
        let send_driver = QuinnSendStreamDriver::new(quinn_send);
        let send = SendStream::new(send_driver);
        Ok(send)
    }

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (quinn_send, quinn_recv) = self.conn.open_bi().await?;
        let send_driver = QuinnSendStreamDriver::new(quinn_send);
        let recv_driver = QuinnRecvStreamDriver::new(quinn_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        let quinn_recv = self.conn.accept_uni().await?;
        let recv_driver = QuinnRecvStreamDriver::new(quinn_recv);
        let recv = RecvStream::new(recv_driver);
        Ok(recv)
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (quinn_send, quinn_recv) = self.conn.accept_bi().await?;
        let send_driver = QuinnSendStreamDriver::new(quinn_send);
        let recv_driver = QuinnRecvStreamDriver::new(quinn_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        let datagram = self.conn.read_datagram().await?;
        Ok(datagram)
    }

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        self.conn.send_datagram(data)?;
        Ok(())
    }

    async fn closed(&self) -> Result<(), ConnectionError> {
        match self.conn.closed().await {
            quinn::ConnectionError::LocallyClosed
            | quinn::ConnectionError::ApplicationClosed(_) => Ok(()),
            err => Err(err.into()),
        }
    }

    fn close(&self, error_code: u32, reason: &str) {
        let var_int = quinn::VarInt::from_u32(error_code);
        self.conn.close(var_int, reason.as_bytes());
    }

    fn max_datagram_size(&self) -> Option<usize> {
        self.conn.max_datagram_size()
    }
}

#[derive(Debug)]
struct QuinnSendStreamDriver {
    send: quinn::SendStream,
}

impl QuinnSendStreamDriver {
    fn new(send: quinn::SendStream) -> Self {
        Self { send }
    }
}

#[async_trait]
impl SendStreamDriver for QuinnSendStreamDriver {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        let size = self.send.write(buf).await?;
        Ok(size)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        self.send.write_all(buf).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), WriteError> {
        self.send.finish().await?;
        Ok(())
    }

    async fn abort(mut self: Box<Self>) -> Result<(), UnknownStreamError> {
        // Not async, but the trait requires the method to be async, because
        // other implementations might abort asynchronously
        self.send.reset(quinn::VarInt::from_u32(0))?;
        Ok(())
    }
}

#[derive(Debug)]
struct QuinnRecvStreamDriver {
    recv: quinn::RecvStream,
}

impl QuinnRecvStreamDriver {
    fn new(recv: quinn::RecvStream) -> Self {
        Self { recv }
    }
}

#[async_trait]
impl RecvStreamDriver for QuinnRecvStreamDriver {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        let size = self.recv.read(buf).await?;
        Ok(size)
    }
}

impl From<quinn::ConnectionError> for ConnectionError {
    fn from(value: quinn::ConnectionError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::ConnectError> for ConnectionError {
    fn from(value: quinn::ConnectError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::ReadError> for ReadError {
    fn from(value: quinn::ReadError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::WriteError> for WriteError {
    fn from(value: quinn::WriteError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::SendDatagramError> for SendDatagramError {
    fn from(value: quinn::SendDatagramError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::UnknownStream> for UnknownStreamError {
    fn from(_: quinn::UnknownStream) -> Self {
        Self
    }
}
