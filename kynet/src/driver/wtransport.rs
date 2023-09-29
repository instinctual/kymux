use crate::error::*;
use crate::{
    Connection, ConnectionDriver, RecvStream, RecvStreamDriver, SendStream, SendStreamDriver,
};

use async_trait::async_trait;
use bytes::Bytes;
#[allow(unused)]
use log::{debug, error, info, warn};

impl From<wtransport::Connection> for Connection {
    fn from(value: wtransport::Connection) -> Self {
        Self::new(WTransportConnectionDriver::new(value))
    }
}

#[derive(Debug)]
struct WTransportConnectionDriver {
    conn: wtransport::Connection,
}

impl WTransportConnectionDriver {
    fn new(conn: wtransport::Connection) -> Self {
        Self { conn }
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
        self.conn.closed().await; // does not report any error
        Ok(())
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
        self.send.write(buf).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), WriteError> {
        self.send.finish().await?;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<(), UnknownStreamError> {
        // Not async, but the trait requires the method to be async, because
        // other implementations might abort asynchronously
        self.send
            .reset(wtransport_proto::varint::VarInt::from_u32(0));
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
