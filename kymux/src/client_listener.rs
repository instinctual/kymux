use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use log::{debug, error, info, warn};
use tokio::io::AsyncReadExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};

use crate::control::ControlMsg;
use crate::io_utils;
use crate::stream::stream_id_to_u64;
use crate::{Error, Result};
use crate::{State, StreamDirection, StreamOwner, StreamPair};

const CLIENT_HELLO_MSG: u8 = 0;

pub(crate) struct ClientListener {
    conn: quinn::Connection,
    state: Arc<Mutex<State>>,
    listener: TcpListener,
    ctrlchan_tx: mpsc::Sender<ControlMsg>,
}

impl ClientListener {
    pub(crate) async fn new(
        conn: quinn::Connection,
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
                conn,
                state,
                listener,
                ctrlchan_tx,
            },
            addr,
        ))
    }

    async fn handle_client(&mut self, mut client: TcpStream) -> Result<()> {
        // Get Handshake
        let mut b = [0u8; 8];
        client.read_exact(&mut b).await?;

        let endpoint_id = u64::from_be_bytes(b);

        // Get endpoint desc. Release the lock because some blocking
        // operation will be done later.
        let endpoint_desc = {
            let mut state = self.state.lock().await;
            let Some(endpoint) = state.endpoints.get_mut(&endpoint_id) else {
                warn!("Received connection to unknown stream {endpoint_id:X}");
                return Ok(());
            };

            *endpoint.desc()
        };

        // Open Quic stream if required
        let mut stream_pair = None;

        if endpoint_desc.owner == StreamOwner::Local {
            let (mut tx, rx) = match endpoint_desc.direction {
                StreamDirection::Bi => match self.conn.open_bi().await {
                    Ok((tx, rx)) => (tx, Some(rx)),
                    Err(err) => {
                        warn!("Failed to open {endpoint_desc:?} stream: {err:?}");
                        return Err(Error::StreamOpenFailed {
                            desc: endpoint_desc,
                        });
                    }
                },
                StreamDirection::Uni => match self.conn.open_uni().await {
                    Ok(tx) => (tx, None),
                    Err(err) => {
                        warn!("Failed to open {endpoint_desc:?} stream: {err:?}");
                        return Err(Error::StreamOpenFailed {
                            desc: endpoint_desc,
                        });
                    }
                },
            };

            // Send a message to allow peer to get notified of stream creation
            io_utils::write_msg(&mut tx, CLIENT_HELLO_MSG).await?;

            let stream_id = stream_id_to_u64(tx.id());
            stream_pair = Some(StreamPair { tx: Some(tx), rx });

            // Notify that stream is opened
            self.ctrlchan_tx
                .send(ControlMsg::StreamOpened {
                    endpoint_id,
                    stream_id,
                })
                .await
                .map_err(|_| Error::ChannelClosed)?;
        }

        {
            // Update endpoint
            let mut state = self.state.lock().await;
            let Some(endpoint) = state.endpoints.get_mut(&endpoint_id) else {
                warn!("Received connection to unknown stream {endpoint_id:X}");
                return Ok(());
            };

            if let Some(stream_pair) = stream_pair {
                endpoint.set_quic_stream(stream_pair).await?;
            }

            endpoint.set_client(client).await?;
        }

        // Notify to peer that our local client is connected
        info!("Got client for endpoint {endpoint_id:X}");

        self.ctrlchan_tx
            .send(ControlMsg::ClientConnected { endpoint_id })
            .await
            .map_err(|_| Error::ChannelClosed)?;

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
