use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::{self, JoinHandle},
};

mod client_listener;
mod control;
mod endpoint;
mod error;
mod io_utils;
mod router;
mod stream;

pub use endpoint::EndpointDesc;
pub use error::{Error, Result};
pub use stream::{StreamDirection, StreamOwner, StreamType};

use client_listener::ClientListener;
use control::{ControlMsg, ControlTask};
use endpoint::{Endpoint, EndpointBuilder};
use router::Router;

const KYMUX_LOCAL_CLIENTS_PORT: u16 = 9090;

pub struct ServerConfig {
    pub addr: SocketAddr,
    pub cert_chain: Vec<rustls::Certificate>,
    pub private_key: rustls::PrivateKey,
    pub client_listener_port: u16,
}

impl ServerConfig {
    pub fn new(
        addr: SocketAddr,
        cert_chain: Vec<rustls::Certificate>,
        private_key: rustls::PrivateKey,
    ) -> Self {
        Self {
            addr,
            cert_chain,
            private_key,
            client_listener_port: KYMUX_LOCAL_CLIENTS_PORT,
        }
    }

    pub fn client_listener_port(&mut self, client_listener_port: u16) -> &mut Self {
        self.client_listener_port = client_listener_port;
        self
    }
}

pub struct ClientConfig {
    pub addr: SocketAddr,
    pub roots: rustls::RootCertStore,
    pub server_name: String,
    pub client_listener_port: u16,
}

impl ClientConfig {
    pub fn new(addr: SocketAddr, roots: rustls::RootCertStore, server_name: &str) -> Self {
        Self {
            addr,
            server_name: server_name.into(),
            roots,
            client_listener_port: KYMUX_LOCAL_CLIENTS_PORT,
        }
    }

    pub fn client_listener_port(&mut self, client_listener_port: u16) -> &mut Self {
        self.client_listener_port = client_listener_port;
        self
    }
}

pub(crate) struct State {
    endpoint_builders: HashMap<u64, EndpointBuilder>,
    endpoints: HashMap<u64, Endpoint>,
    pending_endpoints: HashMap<u64, oneshot::Sender<()>>,
}

impl State {
    fn new() -> Self {
        Self {
            endpoint_builders: HashMap::new(),
            endpoints: HashMap::new(),
            pending_endpoints: HashMap::new(),
        }
    }

    pub(crate) async fn start_endpoint(&mut self, endpoint_id: u64) -> Result<()> {
        let Some(builder) = self.endpoint_builders.get(&endpoint_id) else {
            warn!("Trying to start unknown endoint {endpoint_id:X}");
            return Err(Error::EndpointUnknown { id: endpoint_id });
        };

        if !builder.ready() {
            return Ok(());
        }

        let builder = self.endpoint_builders.remove(&endpoint_id).unwrap();
        self.endpoints.insert(endpoint_id, builder.build().await?);

        Ok(())
    }
}

pub struct Connecting {
    connection: quinn::Connection,
    endpoint: quinn::Endpoint,
    ctrlchan_tx: quinn::SendStream,
    ctrlchan_rx: quinn::RecvStream,
    client_listener_port: u16,
}

impl Connecting {
    pub async fn complete_connection(mut self) -> Result<Connection> {
        io_utils::write_msg(&mut self.ctrlchan_tx, ControlMsg::ConnectionAccepted)
            .await
            .map_err(|err| {
                error!("Failed to send Hello on ControlChan: {err:?}");
                Error::EndpointCtrlChanOpenFailed
            })?;

        Connection::new(
            self.connection,
            self.endpoint,
            self.ctrlchan_tx,
            self.ctrlchan_rx,
            self.client_listener_port,
        )
        .await
    }
}

// Accept a single connection
pub struct ConnectionListener {
    endpoint: quinn::Endpoint,
    client_listener_port: u16,
}

impl ConnectionListener {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        // Setup quinn to accept connections
        let mut quinn_config =
            quinn::ServerConfig::with_single_cert(config.cert_chain, config.private_key)?;

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(5)));

        quinn_config.transport_config(Arc::new(transport_config));

        let endpoint = quinn::Endpoint::server(quinn_config, config.addr)
            .map_err(|err| Error::EndpointCreateFailed { source: err })?;

        Ok(Self {
            endpoint,
            client_listener_port: config.client_listener_port,
        })
    }

    pub async fn accept(self) -> Result<Connecting> {
        let connecting = self
            .endpoint
            .accept()
            .await
            .ok_or(Error::EndpointAcceptFailed)?;

        info!("Waiting for QUIC connection");
        let connection = connecting.await.map_err(|err| {
            error!("Failed to accept connection: {err:?}");
            Error::EndpointAcceptFailed
        })?;

        info!("Got new peer {addr}", addr = connection.remote_address());

        // Wait for control channel + Read Authentication request
        debug!("Server: Open control channel");
        let (ctrlchan_tx, mut ctrlchan_rx) = connection.accept_bi().await.map_err(|err| {
            error!("Failed to open ControlChan: {err:?}");
            Error::EndpointCtrlChanOpenFailed
        })?;

        let msg: ControlMsg = io_utils::read_msg(&mut ctrlchan_rx).await?;
        if let ControlMsg::Authenticate = msg {
            debug!("ControlChan: got Authenticate");
        } else {
            error!(
                "ControlChan: Authenticate was expected, but another message has been received instead"
            );
            return Err(Error::EndpointCtrlChanOpenFailed);
        }

        Ok(Connecting {
            connection,
            endpoint: self.endpoint,
            ctrlchan_tx,
            ctrlchan_rx,
            client_listener_port: self.client_listener_port,
        })
    }

    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await
    }
}

struct Task {
    handle: JoinHandle<()>,
    stop_tx: oneshot::Sender<()>,
}

pub struct Connection {
    _conn: quinn::Connection,
    endpoint: quinn::Endpoint,

    ctrlchan_tx: mpsc::Sender<ControlMsg>,

    state: Arc<Mutex<State>>,
    client_listening_addr: SocketAddr,

    task: Option<Task>,
}

impl Connection {
    async fn new(
        conn: quinn::Connection,
        endpoint: quinn::Endpoint,
        ctrlchan_tx: quinn::SendStream,
        ctrlchan_rx: quinn::RecvStream,
        client_listener_port: u16,
    ) -> Result<Self> {
        let state = Arc::new(Mutex::new(State::new()));

        // Start control task
        let (msg_tx, msg_rx) = mpsc::channel(32);

        let ctrl_task = ControlTask::new(
            state.clone(),
            Box::new(ctrlchan_tx),
            Box::new(ctrlchan_rx),
            msg_tx.clone(),
            msg_rx,
        );

        let mut router = Router::new(conn.clone());
        router.start();
        let router = Arc::new(router);

        // Listen for local clients
        let (mut client_listener, client_listening_addr) = ClientListener::new(
            router.clone(),
            state.clone(),
            msg_tx.clone(),
            client_listener_port,
        )
        .await
        .ok_or(Error::EndpointClientListenFailed)?;

        // Run all tasks concurrently. Each task's run() is expected to end only if something
        // wrong has happened.
        let (stop_task_tx, stop_task_rx) = oneshot::channel();

        let task_handle = task::spawn(async move {
            tokio::select! {
                _ = stop_task_rx => {
                    debug!("Connection task stop requested");
                }
                ret = ctrl_task.run() => {
                    debug!("ControlChan task completed: {ret:?}");
                }
                ret = client_listener.run() => {
                    debug!("ClientListener task completed: {ret:?}");
                }
            }

            debug!("Connection task stopped");
        });

        Ok(Self {
            _conn: conn,
            endpoint,
            ctrlchan_tx: msg_tx,
            state,
            client_listening_addr,
            task: Some(Task {
                handle: task_handle,
                stop_tx: stop_task_tx,
            }),
        })
    }

    pub async fn connect(config: ClientConfig) -> Result<Self> {
        // Connect to Quic socket
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

        let mut endpoint = quinn::Endpoint::client(bind_addr)
            .map_err(|err| Error::EndpointCreateFailed { source: err })?;

        let quinn_config = quinn::ClientConfig::with_root_certificates(config.roots);
        endpoint.set_default_client_config(quinn_config);

        let connecting = endpoint
            .connect(config.addr, &config.server_name)
            .map_err(|err| {
                error!("Failed to connect (Idle => Connecting): {err:?}");
                Error::EndpointConnectFailed
            })?;

        let conn = connecting.await.map_err(|err| {
            error!("Failed to connect (Connecting => Connected): {err:?}");
            Error::EndpointConnectFailed
        })?;

        info!("Connected to peer {addr}", addr = conn.remote_address());

        // Open control channel with Authentication request
        debug!("Client: Accept control channel");
        let (mut ctrlchan_tx, mut ctrlchan_rx) = conn.open_bi().await.map_err(|err| {
            error!("Failed to accept ControlChan: {err:?}");
            Error::EndpointCtrlChanOpenFailed
        })?;

        io_utils::write_msg(&mut ctrlchan_tx, ControlMsg::Authenticate)
            .await
            .map_err(|err| {
                error!("Failed to send Hello on ControlChan: {err:?}");
                Error::EndpointCtrlChanOpenFailed
            })?;

        // Wait for connection ack
        let msg: ControlMsg = io_utils::read_msg(&mut ctrlchan_rx).await?;
        if let ControlMsg::ConnectionAccepted = msg {
            debug!("ControlChan: got ConnectionAccepted");
        } else {
            error!(
                "ControlChan: ConnectionAccepted was expected, but another message has been received instead"
            );
            return Err(Error::EndpointConnectRejected);
        }

        // Apply an offset when Kymux is used as the Connection intiator.
        // It allows the Client and the Host to run on the same machine.
        Self::new(
            conn,
            endpoint,
            ctrlchan_tx,
            ctrlchan_rx,
            config.client_listener_port + 1,
        )
        .await
    }

    pub async fn stop(&mut self) -> Result<()> {
        // Stop main task
        let Some(task) = self.task.take() else {
            return Err(Error::EndpointStopped);
        };

        debug!("Stop main task");
        let ret = task.stop_tx.send(());
        if ret.is_err() {
            // Can happen if task has stop by itself
            debug!("Failed to send stop to main task");
        }

        task.handle.await.map_err(|err| {
            warn!("Failed to join main task: {err:?}");
            Error::EndpointStopFailed
        })?;

        debug!("Main task stopped");

        // Stop endpoints
        {
            let _endpoints: Vec<_> = {
                let mut state = self.state.lock().await;
                state.endpoints.drain().map(|(_, v)| v).collect()
            };

            // TODO stop tasks explicitly?
        }

        Ok(())
    }

    pub async fn register_endpoint(
        &mut self,
        type_: StreamType,
        direction: StreamDirection,
    ) -> Result<u64> {
        let (register_tx, register_rx) = oneshot::channel();
        let id: u64 = rand::random();

        // Send endpoint registration
        let desc = EndpointDesc {
            id,
            owner: StreamOwner::Local,
            type_,
            direction,
        };

        let endpoint_builder = EndpointBuilder::new(desc);

        {
            let mut state = self.state.lock().await;
            state.pending_endpoints.insert(id, register_tx);
        }

        // Wait for peer to acknowledge the registration
        self.ctrlchan_tx
            .send(ControlMsg::RegisterEndpoint {
                id,
                type_: desc.type_,
                dir: desc.direction,
            })
            .await
            .map_err(|_| Error::EndpointStopped)?;

        register_rx.await.map_err(|_| {
            error!("Couldn't get RegisterEndpoint completion");
            Error::EndpointStopped
        })?;

        // Store endpoint
        {
            let mut state = self.state.lock().await;
            let ret = state.endpoint_builders.insert(id, endpoint_builder);
            if ret.is_some() {
                error!("Trying to register endpoint {id:X} twice");
                return Err(Error::FatalError);
            }
        }

        debug!("Local endpoint 0x{id:X} registered");

        Ok(id)
    }

    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await
    }

    pub async fn endpoints(&self) -> Result<Vec<EndpointDesc>> {
        let state = self.state.lock().await;

        Ok(state
            .endpoints
            .values()
            .map(|endpoint| endpoint.desc())
            .cloned()
            .collect())
    }

    pub fn client_listening_addr(&self) -> SocketAddr {
        self.client_listening_addr
    }

    pub fn get_uri_for_endpoint(&self, id: u64) -> Result<String> {
        let port = self.client_listening_addr.port();

        // Only TCP is supported for now
        Ok(format!("kymux://127.0.0.1:{port}/{id:X}"))
    }
}
