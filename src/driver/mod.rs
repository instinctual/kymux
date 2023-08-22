use crate::error::*;
use crate::{RecvStream, SendStream};

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(feature = "quinn")]
mod quinn;

#[async_trait]
pub trait ConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError>;

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError>;

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError>;

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError>;

    fn close(&self, error_code: u32, reason: &str);

    fn max_datagram_size(&self) -> Option<usize>;
}

#[async_trait]
pub trait SendStreamDriver {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError>;

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError>;

    async fn close(&mut self) -> Result<(), WriteError>;

    fn abort(&mut self) -> Result<(), UnknownStreamError>;
}

#[async_trait]
pub trait RecvStreamDriver {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError>;

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError>;
}
