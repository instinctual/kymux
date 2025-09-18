use crate::ipc::{Ipc, IpcRecv, IpcSend};
use crate::serial;
use crate::KyComAddr;

use async_trait::async_trait;
use kyproto_types::av::*;
use kyproto_types::input::*;
use kyproto_types::metrics::*;
use kyproto_types::ProtocolError;
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[async_trait]
pub trait IpcEndpoint {
    type Ipc;

    fn id(&self) -> u16;
    async fn ready(self) -> Result<Self::Ipc, ProtocolError>;
}

async fn ready(tcp_stream: &mut TcpStream, endpoint_id: u16) -> Result<(), ProtocolError> {
    tcp_stream
        .write_u16(endpoint_id)
        .await
        .map_err(ProtocolError::new)?;
    tcp_stream.read_u8().await.map_err(ProtocolError::new)?;
    Ok(())
}

pub struct VideoClientEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl VideoClientEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl IpcEndpoint for VideoClientEndpoint {
    type Ipc = IpcRecv<AVPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready(mut self) -> Result<Self::Ipc, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = IpcRecv::new(self.tcp_stream, serial::av::AVPacketDeserializer);
        Ok(ipc)
    }
}

pub struct VideoServerEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl VideoServerEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl IpcEndpoint for VideoServerEndpoint {
    type Ipc = IpcSend<AVPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready(mut self) -> Result<Self::Ipc, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = IpcSend::new(self.tcp_stream, serial::av::AVPacketSerializer);
        Ok(ipc)
    }
}

pub struct InputEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl InputEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl IpcEndpoint for InputEndpoint {
    type Ipc = Ipc<InputPacket, InputPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready(mut self) -> Result<Self::Ipc, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = Ipc::new(
            self.tcp_stream,
            serial::input::InputPacketSerializer,
            serial::input::InputPacketDeserializer,
        );
        Ok(ipc)
    }
}

pub struct MetricsClientEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl MetricsClientEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl IpcEndpoint for MetricsClientEndpoint {
    type Ipc = IpcRecv<MetricsPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready(mut self) -> Result<Self::Ipc, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = IpcRecv::new(self.tcp_stream, serial::metrics::MetricsPacketDeserializer);
        Ok(ipc)
    }
}

pub struct MetricsServerEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl MetricsServerEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl IpcEndpoint for MetricsServerEndpoint {
    type Ipc = IpcSend<MetricsPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready(mut self) -> Result<Self::Ipc, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = IpcSend::new(self.tcp_stream, serial::metrics::MetricsPacketSerializer);
        Ok(ipc)
    }
}
