use crate::ipc::{Ipc, IpcRecv, IpcSend};
use crate::serial;
use crate::KyComAddr;

use async_trait::async_trait;
#[allow(unused)]
use log::{debug, error, info, warn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type EndpointMap = HashMap<u16, oneshot::Sender<TcpStream>>;

pub struct KyCom {
    addr: SocketAddr,
    pending_endpoints: Arc<Mutex<EndpointMap>>,
    listen_task: JoinHandle<()>,
    runner: Option<Runner>,
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
            runner: None,
        })
    }

    pub async fn start_on_port(port: u16) -> Result<Self> {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
        Self::start_on_addr(addr).await
    }

    pub fn register<T>(&self, endpoint: T) -> Result<TcpForwarder<T>>
    where
        T: kyproto::ProtocolEndpoint,
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

        Ok(TcpForwarder {
            addr: self.addr,
            rx,
            endpoint,
        })
    }

    pub fn forward_async<Endpoint>(
        &mut self,
        forwarder: TcpForwarder<Endpoint>,
    ) -> Result<KyComAddr>
    where
        Endpoint: kyproto::ProtocolEndpoint + Send + 'static,
        TcpForwarder<Endpoint>: Forwarder,
    {
        let addr = forwarder.addr();

        if self.runner.is_none() {
            // Create on first use
            self.runner = Some(Runner::new());
        }

        let runner = self.runner.as_mut().unwrap();
        runner.forward_async(forwarder)?;
        Ok(addr)
    }

    pub fn register_and_forward<Endpoint>(&mut self, endpoint: Endpoint) -> Result<KyComAddr>
    where
        Endpoint: kyproto::ProtocolEndpoint + Send + 'static,
        TcpForwarder<Endpoint>: Forwarder,
    {
        let forwarder = self.register(endpoint)?;
        self.forward_async(forwarder)
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

    pub fn stop(self) {
        // drop self
    }
}

impl Drop for KyCom {
    fn drop(&mut self) {
        self.listen_task.abort();
    }
}

struct Task {
    join_handle: JoinHandle<()>,
}

impl Drop for Task {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

struct Runner {
    tasks: Vec<Task>,
}

impl Runner {
    fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    fn forward_async<T>(&mut self, forwarder: T) -> Result<()>
    where
        T: Forwarder + Send + 'static,
    {
        // Clean up finished tasks
        self.tasks.retain(|task| !task.join_handle.is_finished());

        let forwarder_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("Unknown");
        debug!("Spawning {forwarder_name} forwarder");

        let join_handle = tokio::spawn(async move {
            if let Err(err) = forwarder.forward().await {
                info!("{forwarder_name} forwarder ended with result {err:?}");
            }
        });

        self.tasks.push(Task { join_handle });

        Ok(())
    }
}

pub async fn forward_protocol_send<T>(
    mut proto: kyproto::ProtocolSend<T>,
    mut ipc: IpcRecv<T>,
) -> Result<()> {
    while let Some(packet) = ipc.recv().await? {
        proto.send(packet).await.map_err(to_io_error)?;
    }

    Ok(())
}

pub async fn forward_protocol_recv<T>(
    mut proto: kyproto::ProtocolRecv<T>,
    mut ipc: IpcSend<T>,
) -> Result<()> {
    while let Some(packet) = proto.recv().await.map_err(to_io_error)? {
        ipc.send(packet).await?;
    }

    Ok(())
}

pub async fn forward_protocol_bi<TX: Send + 'static, RX: Send + 'static>(
    proto_send: kyproto::ProtocolSend<RX>,
    proto_recv: kyproto::ProtocolRecv<TX>,
    ipc: Ipc<TX, RX>,
) -> Result<()> {
    let (ipc_send, ipc_recv) = ipc.into_split();
    let send_task = tokio::spawn(async move { forward_protocol_send(proto_send, ipc_recv).await });
    let recv_task = tokio::spawn(async move { forward_protocol_recv(proto_recv, ipc_send).await });
    let (send_result, recv_result) = tokio::join!(send_task, recv_task);
    let _ = send_result?;
    let _ = recv_result?;

    Ok(())
}

pub struct TcpForwarder<T: kyproto::ProtocolEndpoint> {
    addr: SocketAddr,
    rx: oneshot::Receiver<TcpStream>,
    endpoint: T,
}

impl<T: kyproto::ProtocolEndpoint> TcpForwarder<T> {
    pub fn addr(&self) -> KyComAddr {
        KyComAddr::new(self.addr, self.endpoint.id())
    }

    async fn start(self) -> Result<(TcpStream, T::Protocol)> {
        let mut tcp_stream = self.rx.await.map_err(|_| {
            Error::new(
                ErrorKind::ConnectionAborted,
                "TcpStream sender dropped".to_string(),
            )
        })?;

        let protocol = self.endpoint.ready().await.map_err(to_io_error)?;
        tcp_stream.write_all(&[0]).await?;

        Ok((tcp_stream, protocol))
    }
}

impl<T> TcpForwarder<T>
where
    T: kyproto::ProtocolEndpoint<Protocol = kyproto::ProtocolRecv<kyproto::AVPacket>>,
{
    async fn forward_client_av_packets(self) -> Result<()> {
        let (tcp_stream, protocol) = self.start().await?;
        let ipc = IpcSend::new(tcp_stream, serial::av::AVPacketSerializer);
        forward_protocol_recv(protocol, ipc).await
    }
}

impl<T> TcpForwarder<T>
where
    T: kyproto::ProtocolEndpoint<Protocol = kyproto::ProtocolSend<kyproto::AVPacket>>,
{
    pub async fn forward_server_av_packets(self) -> Result<()> {
        let (tcp_stream, protocol) = self.start().await?;
        let ipc = IpcRecv::new(tcp_stream, serial::av::AVPacketDeserializer);
        forward_protocol_send(protocol, ipc).await
    }
}

#[async_trait]
pub trait Forwarder {
    async fn forward(self) -> Result<()>;
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::VideoClientEndpoint> {
    async fn forward(self) -> Result<()> {
        self.forward_client_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::VideoServerEndpoint> {
    async fn forward(self) -> Result<()> {
        self.forward_server_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::AudioClientEndpoint> {
    async fn forward(self) -> Result<()> {
        self.forward_client_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::AudioServerEndpoint> {
    async fn forward(self) -> Result<()> {
        self.forward_server_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::InputEndpoint> {
    async fn forward(self) -> Result<()> {
        let (tcp_stream, (protocol_send, protocol_recv)) = self.start().await?;
        let ipc = Ipc::new(
            tcp_stream,
            serial::input::InputPacketSerializer,
            serial::input::InputPacketDeserializer,
        );
        forward_protocol_bi(protocol_send, protocol_recv, ipc).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::MetricsServerEndpoint> {
    async fn forward(self) -> Result<()> {
        let (tcp_stream, protocol) = self.start().await?;
        let ipc = IpcRecv::new(tcp_stream, serial::metrics::MetricsPacketDeserializer);
        forward_protocol_send(protocol, ipc).await
    }
}

// We could not implement From<ProtocolError> for Error, because both are
// defined in other crates
fn to_io_error(err: kyproto::ProtocolError) -> Error {
    Error::new(ErrorKind::InvalidData, format!("{err}"))
}
