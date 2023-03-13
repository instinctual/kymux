#![allow(dead_code)]

use crate::error::{Error, Result};
use crate::io_utils;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use quinn::{RecvStream, SendStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex;

type ClientMap = HashMap<u64, RouterClient>;

#[derive(Debug)]
pub enum KyRecvMsg {
    AcceptUni(RecvStream),
    AcceptBi(SendStream, RecvStream),
}

#[derive(Debug)]
pub(crate) struct RouterClient {
    tx: mpsc::Sender<KyRecvMsg>,
}

#[derive(Debug)]
pub(crate) struct Router {
    conn: quinn::Connection,
    clients: Arc<Mutex<ClientMap>>,
}

impl Router {
    pub fn new(conn: quinn::Connection) -> Self {
        Self {
            conn,
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn start(&mut self) {
        let conn = self.conn.clone();
        let clients = self.clients.clone();
        tokio::spawn(async move {
            if let Err(err) = Self::accept_channels_uni(conn, clients).await {
                error!("Accept uni failed: {err:?}");
            }
        });

        let conn = self.conn.clone();
        let clients = self.clients.clone();
        tokio::spawn(async move {
            if let Err(err) = Self::accept_channels_bi(conn, clients).await {
                error!("Accept bi failed: {err:?}");
            }
        });
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

    pub async fn open_uni(&self) -> Result<SendStream> {
        Self::open_uni_(&self.conn, self.endpoint_id).await
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        Self::open_bi_(&self.conn, self.endpoint_id).await
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
