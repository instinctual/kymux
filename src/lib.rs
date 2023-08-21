use crate::error::*;

use bytes::Bytes;
use std::net::SocketAddr;

mod error;

pub struct Endpoint {
}

impl Endpoint {
    pub fn client(bind_addr: SocketAddr) -> std::io::Result<Self> {

    }

    pub fn server(config: ServerConfig, bind_addr: SocketAddr) -> std::io::Result<Self> {

    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<Connection, ConnectionError> {

    }

    pub async fn accept(&self) -> Option<Connection> {

    }
}

pub struct ServerConfig {
}

pub struct Connection {
}

impl Connection {
    pub async fn open_uni(&self) -> Result<SendStream, ConnectionError> {

    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {

    }

    pub async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {

    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {

    }

    pub async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {

    }

    pub async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {

    }

    pub fn close(&self, error_code: u64, reason: &str) {

    }

    pub fn max_datagram_size(&self) -> Option<usize> {

    }
}

pub struct SendStream {
}

impl SendStream {
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {

    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {

    }

    pub async fn close(&self) -> Result<(), WriteError> {

    }

    pub fn abort(&self) -> Result<(), UnknownStreamError> {

    }
}

pub struct RecvStream {
}

impl RecvStream {
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {

    }

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError> {

    }
}
