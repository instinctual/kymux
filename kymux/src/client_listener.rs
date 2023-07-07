use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use log::{debug, error, info, warn};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};

use crate::control::ControlMsg;
use crate::io_utils;
use crate::{Error, Result};
use crate::{Router, State};

pub(crate) struct ClientListener {
    router: Arc<Router>,
    state: Arc<Mutex<State>>,
    listener: TcpListener,
    ctrlchan_tx: mpsc::Sender<ControlMsg>,
}

impl ClientListener {
    pub(crate) async fn new(
        router: Arc<Router>,
        state: Arc<Mutex<State>>,
        ctrlchan_tx: mpsc::Sender<ControlMsg>,
        port: u16,
    ) -> Option<(Self, SocketAddr)> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

        let ret = TcpListener::bind(&addr).await;
        let listener = match ret {
            Ok(listener) => listener,
            Err(e) => {
                error!("Bind failed for addr {addr}: {e:?}");
                return None;
            }
        };

        Some((
            Self {
                router,
                state,
                listener,
                ctrlchan_tx,
            },
            addr,
        ))
    }

    async fn handle_client(&mut self, mut client: TcpStream) -> Result<()> {
        // Get Handshake
        let endpoint_id = io_utils::read_endpoint_id(&mut client).await?;

        {
            // Update endpoint
            let mut state = self.state.lock().await;
            let Some(endpoint_builder) = state.endpoint_builders.get_mut(&endpoint_id) else {
                warn!("Received connection to unknown stream {endpoint_id:X}");
                return Ok(());
            };

            endpoint_builder.set_client(client)?;

            let ky_channel = self.router.register(endpoint_id).await?;
            endpoint_builder.set_ky_channel(ky_channel)?;
        }

        // Notify to peer that our local client is connected
        info!("Got client for endpoint {endpoint_id:X}");

        self.ctrlchan_tx
            .send(ControlMsg::ClientConnected { endpoint_id })
            .await
            .map_err(|_| Error::ChannelClosed)?;

        {
            // Must be called after ClientConnected is sent to avoid a deadlock:
            // - start_endpoint() waits on ky_channel.recv();
            // - on the other size, open_uni() will be called only after
            //   ClientConnected is received.
            let mut state = self.state.lock().await;
            state.start_endpoint(endpoint_id).await?;
        }

        Ok(())
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        loop {
            let (client, addr) = self.listener.accept().await.map_err(|err| {
                error!("Got error while accepting local client: {err:?}");
                Error::IOError { source: err }
            })?;

            debug!("Got client. Address: {addr}");

            let ret = self.handle_client(client).await;
            if let Err(err) = ret {
                warn!("Error while handling a new client: {err:?}");
            }
        }
    }
}
