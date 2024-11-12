use std::mem;
use std::ops::DerefMut;
use std::sync::Mutex;

use async_trait::async_trait;
use kyproto::{
    AudioClientEndpoint, AudioServerEndpoint, InputEndpoint, ProtocolEndpoint, VideoClientEndpoint,
    VideoServerEndpoint,
};
use log::info;
use tokio::sync;
use tokio::task::JoinHandle;

use kycom::Forwarder;
use log::{debug, warn};

use crate::{Error, Result};

const KYMUX_LOCAL_CLIENTS_RANGE: u16 = 10;

struct Task {
    name: &'static str,
    stop_tx: sync::oneshot::Sender<()>,
    join_handle: JoinHandle<()>,
}

impl Task {
    async fn stop(self) {
        let _ = self.stop_tx.send(());

        let ret = self.join_handle.await;
        if let Err(err) = ret {
            warn!("{} task ended with result: {err:?}", self.name);
        }
    }
}

pub(crate) struct IpcHandler {
    pub(crate) kycom: kycom::KyCom,
    tasks: Mutex<Vec<Task>>,
}

impl IpcHandler {
    pub(crate) async fn new(local_clients_port: u16) -> Result<Self> {
        for i in 0..KYMUX_LOCAL_CLIENTS_RANGE {
            let port = local_clients_port + i;

            let kycom = kycom::KyCom::start_on_port(local_clients_port).await;
            match kycom {
                Ok(kycom) => {
                    return Ok(Self {
                        kycom,
                        tasks: Default::default(),
                    })
                }
                Err(err) => {
                    log::warn!("Fail to set IpcHandler on port {port}: {err:?}");
                }
            }
        }
        Err(Error::IpcNoPortAvailable)
    }

    pub(crate) async fn stop(&self) -> Result<()> {
        let tasks = {
            let mut tasks = self.tasks.lock()?;
            mem::take(tasks.deref_mut())
        };

        for t in tasks {
            debug!("Stopping {} forwarder", t.name);
            t.stop().await;
        }

        Ok(())
    }

    pub(crate) fn register_and_forward<Endpoint>(&self, endpoint: Endpoint) -> Result<String>
    where
        Endpoint: ProtocolEndpoint + Send + 'static,
        Forwarder<Endpoint>: ForwarderProtocol,
    {
        let forwarder = self.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.forward(forwarder)?;
        Ok(uri)
    }

    pub(crate) fn forward<T>(&self, forwarder: T) -> Result<()>
    where
        T: ForwarderProtocol + Send + 'static,
    {
        debug!("Spawning {} forwarder", T::NAME);

        let (stop_tx, stop_rx) = sync::oneshot::channel();

        let join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = stop_rx => {
                    info!("Stopping {} forwarder", T::NAME);
                },
                ret = forwarder.forward() => {
                    if let Err(err) = ret {
                        info!("{} forwarder ended with result {err:?}", T::NAME);
                    }
                }
            }
        });

        let mut tasks = self.tasks.lock()?;
        tasks.push(Task {
            name: T::NAME,
            stop_tx,
            join_handle,
        });

        Ok(())
    }
}

#[async_trait]
pub trait ForwarderProtocol {
    const NAME: &'static str;

    async fn forward(self) -> std::io::Result<()>;
}

#[async_trait]
impl ForwarderProtocol for Forwarder<VideoClientEndpoint> {
    const NAME: &'static str = "VideoClientEndpoint";

    async fn forward(self) -> std::io::Result<()> {
        self.forward().await
    }
}

#[async_trait]
impl ForwarderProtocol for Forwarder<VideoServerEndpoint> {
    const NAME: &'static str = "VideoServerEndpoint";

    async fn forward(self) -> std::io::Result<()> {
        self.forward().await
    }
}

#[async_trait]
impl ForwarderProtocol for Forwarder<AudioClientEndpoint> {
    const NAME: &'static str = "AudioClientEndpoint";

    async fn forward(self) -> std::io::Result<()> {
        self.forward().await
    }
}

#[async_trait]
impl ForwarderProtocol for Forwarder<AudioServerEndpoint> {
    const NAME: &'static str = "AudioServerEndpoint";

    async fn forward(self) -> std::io::Result<()> {
        self.forward().await
    }
}

#[async_trait]
impl ForwarderProtocol for Forwarder<InputEndpoint> {
    const NAME: &'static str = "InputEndpoint";

    async fn forward(self) -> std::io::Result<()> {
        self.forward().await
    }
}
