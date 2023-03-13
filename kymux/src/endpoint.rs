use core::future::Future;

use log::{debug, error, warn};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};

use crate::stream::stream_id_to_u64;
use crate::{Error, Result, StreamDirection, StreamOwner, StreamPair, StreamType};

#[derive(Clone, Copy, Debug)]
pub struct EndpointDesc {
    pub id: u64,
    pub owner: StreamOwner,
    pub type_: StreamType,
    pub direction: StreamDirection,
}

#[derive(Debug)]
struct Task {
    name: &'static str,
    stop_tx: oneshot::Sender<()>,
    handle: JoinHandle<()>,
}

impl Task {
    fn start<F>(name: &'static str, future: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (stop_tx, stop_rx) = oneshot::channel();

        let handle = task::spawn(async move {
            tokio::select! {
                _ = stop_rx => {}
                _ = future => {}
            }
        });

        Self {
            name,
            stop_tx,
            handle,
        }
    }

    pub(crate) async fn stop(self) {
        let ret = self.stop_tx.send(());
        if ret.is_err() {
            // Can happen if task has stop by itself
            debug!("Failed to send stop to task '{name}'", name = self.name);
        }

        let ret = self.handle.await;
        if let Err(err) = ret {
            warn!("Failed to join task '{name}': {err:?}", name = self.name);
        }
    }
}

#[derive(Debug)]
pub(crate) struct EndpointBuilder {
    desc: EndpointDesc,

    // Mux stream client
    client: Option<TcpStream>,
    peer_client_connected: bool,

    // Quic
    quic_stream: Option<StreamPair>,
}

impl EndpointBuilder {
    pub(crate) fn new(desc: EndpointDesc) -> Self {
        Self {
            desc,
            client: None,
            peer_client_connected: false,
            quic_stream: None,
        }
    }

    pub(crate) fn desc(&self) -> &EndpointDesc {
        &self.desc
    }

    pub(crate) fn set_client(&mut self, client: TcpStream) -> Result<()> {
        if self.client.is_some() {
            error!("Try to set client more than once");
            return Err(Error::FatalError);
        }

        self.client = Some(client);

        Ok(())
    }

    pub(crate) fn peer_client_connected(&mut self) -> Result<()> {
        if self.peer_client_connected {
            error!("Try to notify that the peer is connected more than once");
            return Err(Error::FatalError);
        }

        self.peer_client_connected = true;

        Ok(())
    }

    pub(crate) fn set_quic_stream(&mut self, quic_stream: StreamPair) -> Result<()> {
        if self.quic_stream.is_some() {
            error!("Try to set quic stream more than once");
            return Err(Error::FatalError);
        }

        self.quic_stream = Some(quic_stream);

        Ok(())
    }

    pub(crate) fn ready(&self) -> bool {
        self.peer_client_connected && self.client.is_some() && self.quic_stream.is_some()
    }

    pub(crate) async fn build(mut self) -> Result<Endpoint> {
        if !self.peer_client_connected {
            return Err(Error::EndpointBuilderNotReady);
        }

        let Some(client) = self.client.take() else {
            return Err(Error::EndpointBuilderNotReady);
        };

        let Some(quic_stream) = self.quic_stream.take() else {
            return Err(Error::EndpointBuilderNotReady);
        };

        Endpoint::new(self.desc, client, quic_stream).await
    }
}

#[derive(Debug)]
pub(crate) struct Endpoint {
    // Description
    desc: EndpointDesc,

    // Tasks
    rx_task: Option<Task>,
    tx_task: Option<Task>,
}

impl Endpoint {
    pub(crate) fn desc(&self) -> &EndpointDesc {
        &self.desc
    }

    pub(crate) async fn new(
        desc: EndpointDesc,
        mut client: TcpStream,
        mut quic_stream: StreamPair,
    ) -> Result<Self> {
        debug!("Endpoint {id:X} ready: start routing", id = desc.id);

        // Send sync notification to client
        let sync = [0u8];
        client.write_all(&sync).await?;

        // Forward data
        let (client_rx, client_tx) = client.into_split();

        let rx_task = if let Some(quic_stream_rx) = quic_stream.rx.take() {
            let future = Self::rx_task(quic_stream_rx, client_tx);
            Some(Task::start("Rx task", future))
        } else {
            None
        };

        let tx_task = if let Some(quic_stream_tx) = quic_stream.tx.take() {
            let future = Self::tx_task(quic_stream_tx, client_rx);
            Some(Task::start("Tx task", future))
        } else {
            None
        };

        Ok(Self {
            desc,
            rx_task,
            tx_task,
        })
    }

    pub(crate) async fn stop_task(&mut self) -> Result<()> {
        if let Some(rx_task) = self.rx_task.take() {
            rx_task.stop().await;
            debug!("Endpoint {id:X} Rx task stopped", id = self.desc.id);
        }

        if let Some(tx_task) = self.tx_task.take() {
            tx_task.stop().await;
            debug!("Endpoint {id:X} Tx task stopped", id = self.desc.id);
        }

        Ok(())
    }

    async fn rx_task(mut quic_rx: quinn::RecvStream, mut client_tx: OwnedWriteHalf) {
        let ret = tokio::io::copy(&mut quic_rx, &mut client_tx).await;
        debug!("Quic -> Client: {ret:?}");
    }

    async fn tx_task(mut quic_tx: quinn::SendStream, mut client_rx: OwnedReadHalf) {
        let ret = tokio::io::copy(&mut client_rx, &mut quic_tx).await;
        debug!("Client -> Quic: {ret:?}");

        let ret = quic_tx.reset(quinn::VarInt::from_u32(0));
        if let Err(err) = ret {
            warn!(
                "Fail to reset Quic stream {id}: {err:?}",
                id = stream_id_to_u64(quic_tx.id())
            );
        }
    }
}
