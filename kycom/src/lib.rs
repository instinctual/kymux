use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use kyproto::error::ProtocolError;
use kyproto::{
    AVPacket, AVPacketHeader, CodecPacket, MediaPacket, ProtocolEndpoint, VideoClientEndpoint,
    VideoServerEndpoint,
};
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type EndpointMap = HashMap<u16, oneshot::Sender<TcpStream>>;

pub struct KyComAddr {
    pub addr: SocketAddr,
    pub endpoint_id: u16,
}

impl KyComAddr {
    fn new(addr: SocketAddr, endpoint_id: u16) -> Self {
        Self { addr, endpoint_id }
    }

    pub fn url(&self) -> String {
        format!("kycom://{}/{:X}", self.addr, self.endpoint_id)
    }
}

pub struct KyCom {
    addr: SocketAddr,
    pending_endpoints: Arc<Mutex<EndpointMap>>,
    listen_task: JoinHandle<()>,
}

impl KyCom {
    pub async fn start_on_addr(addr: SocketAddr) -> Result<Self> {
        let pending_endpoints = Arc::new(Mutex::new(HashMap::new()));

        let listener = TcpListener::bind(addr).await?;
        let pending_endpoints2 = pending_endpoints.clone();
        let listen_task = tokio::spawn(async move {
            if let Err(err) = Self::listen(listener, pending_endpoints2).await {
                error!("TcpListener error: {err}");
            }
        });

        Ok(Self {
            addr,
            pending_endpoints,
            listen_task,
        })
    }

    pub async fn start_on_port(port: u16) -> Result<Self> {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
        Self::start_on_addr(addr).await
    }

    pub fn register<T>(&self, endpoint: T) -> Result<Forwarder<T>>
    where
        T: ProtocolEndpoint,
    {
        let rx = {
            let endpoint_id = endpoint.id();
            let mut pending_endpoints = self.pending_endpoints.lock().unwrap();
            match pending_endpoints.entry(endpoint_id) {
                Entry::Occupied(_) => {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        "Endpoint {endpoint_id} already pending",
                    ));
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.insert(tx);
                    rx
                }
            }
        };

        Ok(Forwarder {
            addr: self.addr,
            rx,
            endpoint,
        })
    }

    async fn listen(
        listener: TcpListener,
        pending_endpoints: Arc<Mutex<EndpointMap>>,
    ) -> Result<()> {
        loop {
            let (tcp_stream, _) = listener.accept().await?;
            let pending_endpoints = pending_endpoints.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::handle_stream(tcp_stream, pending_endpoints).await {
                    error!("TcpStream error: {err}");
                }
            });
        }
    }

    async fn handle_stream(
        mut tcp_stream: TcpStream,
        pending_endpoints: Arc<Mutex<EndpointMap>>,
    ) -> Result<()> {
        let endpoint_id = tcp_stream.read_u16().await?;
        info!("TCP connection for endpoint {endpoint_id:X}");
        let mut pending_endpoints = pending_endpoints.lock().unwrap();
        if let Some(tx) = pending_endpoints.remove(&endpoint_id) {
            // Ignore error (if the receiver is dropped)
            let _ = tx.send(tcp_stream);
        } else {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Connection received for unknown endpoint id: {endpoint_id:X}"),
            ));
        }

        Ok(())
    }
}

impl Drop for KyCom {
    fn drop(&mut self) {
        self.listen_task.abort();
    }
}

pub struct Forwarder<T: ProtocolEndpoint> {
    addr: SocketAddr,
    rx: oneshot::Receiver<TcpStream>,
    endpoint: T,
}

impl<T: ProtocolEndpoint> Forwarder<T> {
    pub fn addr(&self) -> KyComAddr {
        KyComAddr::new(self.addr, self.endpoint.id())
    }

    async fn tcp_stream(rx: oneshot::Receiver<TcpStream>) -> Result<TcpStream> {
        let tcp_stream = rx.await.map_err(|_| {
            Error::new(
                ErrorKind::ConnectionAborted,
                "TcpStream sender dropped".to_string(),
            )
        })?;

        Ok(tcp_stream)
    }
}

impl Forwarder<VideoClientEndpoint> {
    pub async fn forward(self) -> Result<()> {
        let mut tcp_stream = Self::tcp_stream(self.rx).await?;

        let mut protocol = self.endpoint.ready().await.map_err(to_io_error)?;
        tcp_stream.write(&[0]).await?;

        while let Some(packet) = protocol.recv().await.map_err(to_io_error)? {
            match packet {
                AVPacket::Codec(packet) => {
                    let header = packet.header.serialize();
                    tcp_stream.write_all(&header).await?;
                }
                AVPacket::Media(packet) => {
                    let header = packet.header.serialize();
                    tcp_stream.write_all(&header).await?;
                    tcp_stream.write_all(&packet.payload).await?;
                }
            }
        }

        Ok(())
    }
}

impl Forwarder<VideoServerEndpoint> {
    pub async fn forward(self) -> Result<()> {
        let mut tcp_stream = Self::tcp_stream(self.rx).await?;

        let mut protocol = self.endpoint.ready().await.map_err(to_io_error)?;
        tcp_stream.write(&[0]).await?;

        let mut header = [0; 12];
        loop {
            if let Err(err) = tcp_stream.read_exact(&mut header).await {
                if err.kind() == ErrorKind::UnexpectedEof {
                    return Ok(()); // EOF
                }
            }

            // XXX The header serialization format is specific to the IPC
            // (KyCom), but for convenience we use the same in KyProto
            // protocols implementation, so the code is shared
            let header = AVPacketHeader::deserialize(&header);
            let packet = match header {
                AVPacketHeader::Media(header) => {
                    let mut buf = BytesMut::zeroed(header.size as usize);

                    tcp_stream.read_exact(&mut buf).await?;

                    AVPacket::Media(MediaPacket {
                        header,
                        payload: buf.freeze(),
                    })
                }
                AVPacketHeader::Codec(header) => AVPacket::Codec(CodecPacket { header }),
            };

            protocol.send(packet).await.map_err(to_io_error)?;
        }
    }
}

// We could not implement From<ProtocolError> for Error, because both are
// defined in other crates
fn to_io_error(err: ProtocolError) -> Error {
    Error::new(ErrorKind::InvalidData, format!("{err}"))
}
