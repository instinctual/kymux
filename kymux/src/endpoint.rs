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
pub(crate) struct Endpoint {
    // Description
    desc: EndpointDesc,

    // Mux stream client
    client: Option<TcpStream>,
    peer_client_connected: bool,

    // Quic
    stream_id: Option<u64>,
    quic_stream: Option<StreamPair>,

    // Tasks
    rx_task: Option<Task>,
    tx_task: Option<Task>,
}

impl Endpoint {
    pub(crate) fn desc(&self) -> &EndpointDesc {
        &self.desc
    }

    pub(crate) fn stream_id(&self) -> Option<u64> {
        self.stream_id
    }

    pub(crate) fn new(desc: EndpointDesc) -> Self {
        Self {
            desc,
            client: None,
            peer_client_connected: false,
            stream_id: None,
            quic_stream: None,
            rx_task: None,
            tx_task: None,
        }
    }

    async fn start_task(&mut self) -> Result<()> {
        if self.client.is_none() || !self.peer_client_connected || self.quic_stream.is_none() {
            return Ok(());
        }

        debug!("Endpoint {id:X} ready: start routing", id = self.desc.id);

        let mut client = self.client.take().ok_or(Error::EndpointAlreadyStarted)?;
        let mut quic_stream = self
            .quic_stream
            .take()
            .ok_or(Error::EndpointAlreadyStarted)?;
        self.stream_id = None;

        // Send sync notification to client
        let sync = [0u8];
        client.write_all(&sync).await?;

        // Forward data
        let (client_rx, client_tx) = client.into_split();

        if let Some(quic_stream_rx) = quic_stream.rx.take() {
            let future = Self::rx_task(quic_stream_rx, client_tx);
            self.rx_task = Some(Task::start("Rx task", future));
        }

        if let Some(quic_stream_tx) = quic_stream.tx.take() {
            let future = Self::tx_task(quic_stream_tx, client_rx);
            self.tx_task = Some(Task::start("Tx task", future));
        }

        Ok(())
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

    async fn run_task_wrapped(&mut self) -> Result<()> {
        if let Err(err) = self.start_task().await {
            warn!(
                "Failed to run task for endpoint {desc:?}: {err:?}",
                desc = self.desc
            );
            return Err(err);
        };

        Ok(())
    }

    pub(crate) async fn set_client(&mut self, client: TcpStream) -> Result<()> {
        if self.client.is_some() {
            error!("Try to set client more than once");
            return Err(Error::FatalError);
        }

        self.client = Some(client);

        self.run_task_wrapped().await
    }

    pub(crate) async fn peer_client_connected(&mut self) -> Result<()> {
        if self.peer_client_connected {
            error!("Try to notify that the peer is connected more than once");
            return Err(Error::FatalError);
        }

        self.peer_client_connected = true;

        self.run_task_wrapped().await
    }

    pub(crate) async fn set_stream_id(&mut self, stream_id: u64) -> Result<()> {
        if self.stream_id.is_some() {
            error!("Try to set stream id more than once");
            return Err(Error::FatalError);
        }

        self.stream_id = Some(stream_id);

        Ok(())
    }

    pub(crate) async fn set_quic_stream(&mut self, quic_stream: StreamPair) -> Result<()> {
        if self.quic_stream.is_some() {
            error!("Try to set quic stream more than once");
            return Err(Error::FatalError);
        }

        self.quic_stream = Some(quic_stream);

        self.run_task_wrapped().await
    }
}
