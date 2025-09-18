use crate::serial::{Deserializer, Serializer};
use kyproto_types::ProtocolError;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

pub struct IpcSend<T> {
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    serializer: Box<dyn Serializer<Packet = T> + Send>,
}

impl<T> IpcSend<T> {
    pub(crate) fn new(
        writer: impl AsyncWrite + Send + Unpin + 'static,
        serializer: impl Serializer<Packet = T> + Send + 'static,
    ) -> Self {
        Self {
            writer: Box::new(writer),
            serializer: Box::new(serializer),
        }
    }

    pub async fn send(&mut self, packet: T) -> Result<(), ProtocolError> {
        self.serializer
            .write(packet, &mut self.writer)
            .await
            .map_err(ProtocolError::new)
    }
}

pub struct IpcRecv<T> {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    deserializer: Box<dyn Deserializer<Packet = T> + Send>,
}

impl<T> IpcRecv<T> {
    pub(crate) fn new(
        reader: impl AsyncRead + Send + Unpin + 'static,
        deserializer: impl Deserializer<Packet = T> + Send + 'static,
    ) -> Self {
        Self {
            reader: Box::new(reader),
            deserializer: Box::new(deserializer),
        }
    }

    pub async fn recv(&mut self) -> Result<Option<T>, ProtocolError> {
        self.deserializer
            .read(&mut self.reader)
            .await
            .map_err(ProtocolError::new)
    }
}

pub struct Ipc<TX, RX> {
    send: IpcSend<TX>,
    recv: IpcRecv<RX>,
}

impl<TX, RX> Ipc<TX, RX> {
    pub(crate) fn new(
        tcp: TcpStream,
        serializer: impl Serializer<Packet = TX> + Send + 'static,
        deserializer: impl Deserializer<Packet = RX> + Send + 'static,
    ) -> Self {
        let (reader, writer) = tcp.into_split();
        let send = IpcSend::new(writer, serializer);
        let recv = IpcRecv::new(reader, deserializer);
        Self { send, recv }
    }

    pub async fn send(&mut self, packet: TX) -> Result<(), ProtocolError> {
        self.send.send(packet).await.map_err(ProtocolError::new)
    }

    pub async fn recv(&mut self) -> Result<Option<RX>, ProtocolError> {
        self.recv.recv().await.map_err(ProtocolError::new)
    }

    pub fn into_split(self) -> (IpcSend<TX>, IpcRecv<RX>) {
        (self.send, self.recv)
    }
}
