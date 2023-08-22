use crate::error::*;
use crate::{
    Connection, ConnectionDriver, RecvStream, RecvStreamDriver, SendStream, SendStreamDriver,
};

use async_trait::async_trait;
use bytes::Bytes;

impl From<quinn::Connection> for Connection {
    fn from(value: quinn::Connection) -> Self {
        Self::new(QuinnConnectionDriver::new(value))
    }
}

struct QuinnConnectionDriver {
    conn: quinn::Connection,
}

impl QuinnConnectionDriver {
    fn new(conn: quinn::Connection) -> Self {
        Self { conn }
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

    fn close(&self, error_code: u32, reason: &str) {
        let var_int = quinn::VarInt::from_u32(error_code);
        self.conn.close(var_int, reason.as_bytes());
    }

    fn max_datagram_size(&self) -> Option<usize> {
        self.conn.max_datagram_size()
    }
}

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

    fn abort(&mut self) -> Result<(), UnknownStreamError> {
        self.send.reset(quinn::VarInt::from_u32(0))?;
        Ok(())
    }
}

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

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError> {
        let size = self.recv.read_exact(buf).await?;
        Ok(size)
    }
}

impl From<quinn::ConnectionError> for ConnectionError {
    fn from(value: quinn::ConnectionError) -> Self {
        Self::Generic {
            msg: format!("{value:?}"),
        }
    }
}

impl From<quinn::ReadError> for ReadError {
    fn from(value: quinn::ReadError) -> Self {
        Self::Generic {
            msg: format!("{value:?}"),
        }
    }
}

impl From<quinn::ReadExactError> for ReadExactError {
    fn from(value: quinn::ReadExactError) -> Self {
        match value {
            quinn::ReadExactError::FinishedEarly => Self::FinishedEarly,
            quinn::ReadExactError::ReadError(error) => Self::ReadError(error.into()),
        }
    }
}

impl From<quinn::WriteError> for WriteError {
    fn from(value: quinn::WriteError) -> Self {
        Self::Generic {
            msg: format!("{value:?}"),
        }
    }
}

impl From<quinn::SendDatagramError> for SendDatagramError {
    fn from(value: quinn::SendDatagramError) -> Self {
        match value {
            quinn::SendDatagramError::UnsupportedByPeer | quinn::SendDatagramError::Disabled => {
                Self::Unsupported
            }
            quinn::SendDatagramError::TooLarge => Self::TooLarge,
            quinn::SendDatagramError::ConnectionLost(error) => Self::ConnectionError(error.into()),
        }
    }
}

impl From<quinn::UnknownStream> for UnknownStreamError {
    fn from(_: quinn::UnknownStream) -> Self {
        Self
    }
}
