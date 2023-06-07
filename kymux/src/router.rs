#![allow(dead_code)]

use crate::error::{Error, Result};
use crate::io_utils;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use bytes::{BufMut, Bytes, BytesMut};
use quinn::{RecvStream, SendStream};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

type ClientMap = HashMap<u64, RouterClient>;

#[derive(Debug)]
pub enum KyRecvMsg {
    AcceptUni(RecvStream),
    AcceptBi(SendStream, RecvStream),
    Datagram(Bytes),
}

#[derive(Debug)]
pub(crate) struct RouterClient {
    tx: mpsc::Sender<KyRecvMsg>,
}

#[derive(Debug)]
struct TaskWrapper {
    join_handle: tokio::task::JoinHandle<()>,
    tx: oneshot::Sender<()>,
    name: String,
}

#[derive(Debug)]
pub(crate) struct Router {
    conn: quinn::Connection,
    clients: Arc<Mutex<ClientMap>>,
    tasks: Mutex<Vec<TaskWrapper>>,
}

impl Router {
    pub fn new(conn: quinn::Connection) -> Self {
        Self {
            conn,
            clients: Arc::new(Mutex::new(HashMap::new())),
            tasks: Default::default(),
        }
    }

    fn spawn_task<F>(task_entry: F, name: String) -> TaskWrapper
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let task_name = name.clone();

        let join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = rx => {}
                ret = task_entry => {
                    if let Err(err) = ret {
                        error!("{task_name} failed: {err:?}");
                    }
                }
            }
        });

        TaskWrapper {
            tx,
            join_handle,
            name,
        }
    }

    pub async fn start(&mut self) {
        // Accept Uni
        let accept_uni_task = Self::spawn_task(
            Self::accept_channels_uni(self.conn.clone(), self.clients.clone()),
            "Accept uni".into(),
        );

        // Accept Bi
        let accept_bi_task = Self::spawn_task(
            Self::accept_channels_bi(self.conn.clone(), self.clients.clone()),
            "Accept bi".into(),
        );

        // Read Datagrams
        let receive_task = Self::spawn_task(
            Self::recv_channels_datagrams(self.conn.clone(), self.clients.clone()),
            "Recv datagrams".into(),
        );

        let mut router_tasks = self.tasks.lock().await;
        *router_tasks = vec![accept_uni_task, accept_bi_task, receive_task];
    }

    pub async fn stop(&self) -> Result<()> {
        let tasks = {
            let mut tasks = self.tasks.lock().await;
            std::mem::take(&mut *tasks)
        };

        for task in tasks {
            let ret = task.tx.send(());
            if ret.is_err() {
                warn!("Task {} seems to be already stopped", task.name);
            }

            let ret = task.join_handle.await;
            if let Err(err) = ret {
                warn!("Task {} ended with error {err:?}", task.name);
            }
        }

        Ok(())
    }

    pub async fn register(&self, endpoint_id: u64) -> Result<KyChannel> {
        let (tx, rx) = mpsc::channel(16);

        let mut clients = self.clients.lock().await;
        match clients.entry(endpoint_id) {
            Entry::Occupied(_) => return Err(Error::KyChannelAlreadyRegistered { endpoint_id }),
            Entry::Vacant(entry) => entry.insert(RouterClient { tx }),
        };

        Ok(KyChannel::new(
            endpoint_id,
            self.conn.clone(),
            self.clients.clone(),
            rx,
        ))
    }

    async fn accept_channels_uni(
        conn: quinn::Connection,
        clients: Arc<Mutex<ClientMap>>,
    ) -> Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            match io_utils::read_endpoint_id(&mut recv).await {
                Ok(endpoint_id) => {
                    let mut clients = clients.lock().await;
                    let client = clients
                        .get_mut(&endpoint_id)
                        .ok_or(Error::KyChannelUnknownId { endpoint_id })?;
                    client
                        .tx
                        .send(KyRecvMsg::AcceptUni(recv))
                        .await
                        .map_err(|e| {
                            Error::KyChannelSendError(format!("Could not send AcceptUni: {}", e))
                        })?;
                }
                Err(err) => {
                    // This is expected if the stream is already reset
                    debug!("accept_channels_uni: Read endpoint error: {err:?}");
                }
            }
        }
    }

    async fn accept_channels_bi(
        conn: quinn::Connection,
        clients: Arc<Mutex<ClientMap>>,
    ) -> Result<()> {
        loop {
            let (send, mut recv) = conn.accept_bi().await?;
            match io_utils::read_endpoint_id(&mut recv).await {
                Ok(endpoint_id) => {
                    let mut clients = clients.lock().await;
                    let client = clients
                        .get_mut(&endpoint_id)
                        .ok_or(Error::KyChannelUnknownId { endpoint_id })?;
                    client
                        .tx
                        .send(KyRecvMsg::AcceptBi(send, recv))
                        .await
                        .map_err(|e| {
                            Error::KyChannelSendError(format!("Could not send AcceptBi: {}", e))
                        })?;
                }
                Err(err) => {
                    // This is expected if the stream is already reset
                    debug!("accept_channels_bi: Read endpoint error: {err:?}");
                }
            }
        }
    }

    async fn recv_channels_datagrams(
        conn: quinn::Connection,
        clients: Arc<Mutex<ClientMap>>,
    ) -> Result<()> {
        loop {
            let datagram = conn.read_datagram().await?;
            if datagram.len() >= 8 {
                let endpoint_id = u64::from_be_bytes((&datagram[..8]).try_into().unwrap());
                let mut clients = clients.lock().await;
                if let Some(client) = clients.get_mut(&endpoint_id) {
                    client
                        .tx
                        .send(KyRecvMsg::Datagram(datagram))
                        .await
                        .map_err(|e| {
                            Error::KyChannelSendError(format!("Could not send Datagram: {}", e))
                        })?;
                } else {
                    // Not a hard error, datagrams may be delivered at any
                    // time, including once the endpoint has been removed
                    warn!("Received a datagram with an unknown id: {endpoint_id:X}");
                }
            } else {
                warn!("Datagram endpoint id too short, dropping");
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct KyChannel {
    endpoint_id: u64,
    conn: quinn::Connection,
    rx: mpsc::Receiver<KyRecvMsg>,
    dropper: KyChannelDropper,
}

#[derive(Debug)]
pub(crate) struct KyChannelSend {
    endpoint_id: u64,
    conn: quinn::Connection,
    dropper: Arc<KyChannelDropper>,
}

#[derive(Debug)]
pub(crate) struct KyChannelRecv {
    endpoint_id: u64,
    conn: quinn::Connection,
    rx: mpsc::Receiver<KyRecvMsg>,
    dropper: Arc<KyChannelDropper>,
}

#[derive(Debug)]
struct KyChannelDropper {
    endpoint_id: u64,
    clients: Arc<Mutex<ClientMap>>, // to implement Drop
}

impl KyChannelSend {
    async fn open_uni_(conn: &quinn::Connection, endpoint_id: u64) -> Result<SendStream> {
        let mut send = conn.open_uni().await?;
        io_utils::write_endpoint_id(&mut send, endpoint_id).await?;
        Ok(send)
    }

    async fn open_bi_(
        conn: &quinn::Connection,
        endpoint_id: u64,
    ) -> Result<(SendStream, RecvStream)> {
        let (mut send, recv) = conn.open_bi().await?;
        io_utils::write_endpoint_id(&mut send, endpoint_id).await?;
        Ok((send, recv))
    }

    async fn send_datagram_(conn: &quinn::Connection, data: Bytes) -> Result<()> {
        conn.send_datagram(data)?;
        Ok(())
    }

    fn write_datagram_header_(endpoint_id: u64, buf: &mut BytesMut) {
        buf.put_u64(endpoint_id);
    }

    fn max_datagram_size_(conn: &quinn::Connection) -> Option<usize> {
        conn.max_datagram_size()
    }

    pub async fn open_uni(&self) -> Result<SendStream> {
        Self::open_uni_(&self.conn, self.endpoint_id).await
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        Self::open_bi_(&self.conn, self.endpoint_id).await
    }

    pub async fn send_datagram(&self, data: Bytes) -> Result<()> {
        Self::send_datagram_(&self.conn, data).await
    }

    pub fn write_datagram_header(&self, buf: &mut BytesMut) {
        Self::write_datagram_header_(self.endpoint_id, buf);
    }

    pub fn max_datagram_size(&self) -> Option<usize> {
        Self::max_datagram_size_(&self.conn)
    }
}

impl KyChannelRecv {
    async fn recv_(rx: &mut mpsc::Receiver<KyRecvMsg>) -> Result<KyRecvMsg> {
        rx.recv()
            .await
            .ok_or_else(|| Error::KyChannelRecvError("Ky channel closed".to_string()))
    }

    pub async fn recv(&mut self) -> Result<KyRecvMsg> {
        Self::recv_(&mut self.rx).await
    }
}

impl KyChannel {
    pub fn new(
        endpoint_id: u64,
        conn: quinn::Connection,
        clients: Arc<Mutex<ClientMap>>,
        rx: mpsc::Receiver<KyRecvMsg>,
    ) -> Self {
        Self {
            endpoint_id,
            conn,
            rx,
            dropper: KyChannelDropper {
                endpoint_id,
                clients,
            },
        }
    }

    pub async fn open_uni(&self) -> Result<SendStream> {
        KyChannelSend::open_uni_(&self.conn, self.endpoint_id).await
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        KyChannelSend::open_bi_(&self.conn, self.endpoint_id).await
    }

    pub async fn recv(&mut self) -> Result<KyRecvMsg> {
        KyChannelRecv::recv_(&mut self.rx).await
    }

    pub async fn send_datagram(&self, data: Bytes) -> Result<()> {
        KyChannelSend::send_datagram_(&self.conn, data).await
    }

    pub fn write_datagram_header(&self, buf: &mut BytesMut) {
        KyChannelSend::write_datagram_header_(self.endpoint_id, buf);
    }

    pub fn max_datagram_size(&self) -> Option<usize> {
        KyChannelSend::max_datagram_size_(&self.conn)
    }

    pub fn into_split(self) -> (KyChannelRecv, KyChannelSend) {
        let KyChannel {
            endpoint_id,
            conn,
            rx,
            dropper,
        } = self;

        let dropper = Arc::new(dropper);

        let send = KyChannelSend {
            endpoint_id,
            conn: conn.clone(),
            dropper: dropper.clone(),
        };

        let recv = KyChannelRecv {
            endpoint_id,
            conn,
            rx,
            dropper,
        };

        (recv, send)
    }
}

impl Drop for KyChannelDropper {
    fn drop(&mut self) {
        // No async drop
        let clients = self.clients.clone();
        let endpoint_id = self.endpoint_id;
        tokio::spawn(async move {
            // Remove registration
            let mut clients = clients.lock().await;
            let ret = clients.remove(&endpoint_id);
            // An id must not be reused, so it's not racy
            assert!(ret.is_some());
        });
    }
}
