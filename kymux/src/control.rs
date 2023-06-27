use std::sync::Arc;

use log::{debug, error, warn};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, Mutex};

use crate::io_utils;
use crate::{EndpointBuilder, EndpointDesc, State, StreamOwner, StreamType};
use crate::{Error, Result};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ControlMsg {
    Authenticate,
    ConnectionAccepted,
    RegisterEndpoint { id: u16, type_: StreamType },
    EndpointRegistered { endpoint_id: u16 },
    ClientConnected { endpoint_id: u16 },
}

pub(crate) struct ControlTask {
    state: Arc<Mutex<State>>,

    stream_tx: Box<dyn AsyncWrite + Unpin + Send>,
    stream_rx: Box<dyn AsyncRead + Unpin + Send>,

    channel_tx: mpsc::Sender<ControlMsg>,
    channel_rx: mpsc::Receiver<ControlMsg>,
}

impl ControlTask {
    pub(crate) fn new(
        state: Arc<Mutex<State>>,
        stream_tx: Box<dyn AsyncWrite + Unpin + Send>,
        stream_rx: Box<dyn AsyncRead + Unpin + Send>,
        channel_tx: mpsc::Sender<ControlMsg>,
        channel_rx: mpsc::Receiver<ControlMsg>,
    ) -> Self {
        Self {
            state,
            stream_tx,
            stream_rx,
            channel_tx,
            channel_rx,
        }
    }

    async fn rx_task(
        state: Arc<Mutex<State>>,
        mut stream_rx: Box<dyn AsyncRead + Unpin + Send>,
        channel_tx: mpsc::Sender<ControlMsg>,
    ) -> Result<()> {
        loop {
            let msg: ControlMsg = io_utils::read_msg(&mut stream_rx).await?;
            Self::handle_msg(msg, &state, &channel_tx).await?;
        }
    }

    async fn tx_task(
        mut stream_tx: Box<dyn AsyncWrite + Unpin + Send>,
        mut channel_rx: mpsc::Receiver<ControlMsg>,
    ) -> Result<()> {
        while let Some(msg) = channel_rx.recv().await {
            io_utils::write_msg(&mut stream_tx, msg).await?;
        }

        Ok(())
    }

    pub(crate) async fn run(self) -> Result<()> {
        let ret = tokio::try_join!(
            Self::rx_task(self.state, self.stream_rx, self.channel_tx),
            Self::tx_task(self.stream_tx, self.channel_rx)
        );

        if let Err(err) = ret {
            warn!("ControlChan task completion result: {err:?}");
        }

        Ok(())
    }

    async fn handle_msg(
        msg: ControlMsg,
        state: &Arc<Mutex<State>>,
        channel_tx: &mpsc::Sender<ControlMsg>,
    ) -> Result<()> {
        match msg {
            ControlMsg::Authenticate | ControlMsg::ConnectionAccepted => {
                error!("{msg:?} received after connection handshake");
                return Err(Error::InvalidControlMsg);
            }
            ControlMsg::RegisterEndpoint { id, type_ } => {
                let endpoint_builder = EndpointBuilder::new(EndpointDesc {
                    id,
                    owner: StreamOwner::Peer,
                    type_,
                });

                {
                    let mut state = state.lock().await;
                    state.endpoint_builders.insert(id, endpoint_builder);
                }

                debug!("Received register endpoint {id:X}: Done");
                channel_tx
                    .send(ControlMsg::EndpointRegistered { endpoint_id: id })
                    .await
                    .map_err(|_| Error::ChannelClosed)?;
            }
            ControlMsg::EndpointRegistered { endpoint_id } => {
                debug!("Peer endpoint 0x{endpoint_id:X} registered");

                let tx = {
                    let mut state = state.lock().await;
                    match state.pending_endpoints.remove(&endpoint_id) {
                        Some(tx) => tx,
                        _ => {
                            error!("Got 'EndpointRegistered' for unknown endpoint {endpoint_id:X}");
                            return Err(Error::InvalidControlMsg);
                        }
                    }
                };

                tx.send(()).map_err(|_| Error::ChannelClosed)?;
            }
            ControlMsg::ClientConnected { endpoint_id } => {
                let mut state = state.lock().await;
                let Some(endpoint_builder) = state.endpoint_builders.get_mut(&endpoint_id) else {
                    error!("Got ClientConnected for unknown stream {endpoint_id:X}");
                    return Err(Error::InvalidControlMsg);
                };

                debug!("Peer has notified that its local client is connected");
                endpoint_builder.peer_client_connected()?;

                state.start_endpoint(endpoint_id).await?;
            }
        }

        Ok(())
    }
}
