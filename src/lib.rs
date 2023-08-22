use crate::driver::{ConnectionDriver, RecvStreamDriver, SendStreamDriver};
use crate::error::*;

use bytes::Bytes;

mod driver;
mod error;

// This abtraction does not handle the QUIC or WebTransport connection
// creation, but only streams and datagram once the connection is created

pub struct Connection {
    driver: Box<dyn ConnectionDriver>,
}

impl Connection {
    pub(crate) fn new<T: ConnectionDriver + 'static>(driver: T) -> Self {
        Self {
            driver: Box::new(driver),
        }
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

    pub fn close(&self, error_code: u32, reason: &str) {
        self.driver.close(error_code, reason)
    }

    pub fn max_datagram_size(&self) -> Option<usize> {
        self.driver.max_datagram_size()
    }
}

pub struct SendStream {
    driver: Box<dyn SendStreamDriver>,
}

impl SendStream {
    pub(crate) fn new<T: SendStreamDriver + 'static>(driver: T) -> Self {
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

    pub fn abort(&mut self) -> Result<(), UnknownStreamError> {
        self.driver.abort()
    }
}

pub struct RecvStream {
    driver: Box<dyn RecvStreamDriver>,
}

impl RecvStream {
    pub(crate) fn new<T: RecvStreamDriver + 'static>(driver: T) -> Self {
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
