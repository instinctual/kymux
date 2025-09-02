use kyproto::{AudioProtocol, MetricsClientEndpoint, ProtocolEndpoint, VideoProtocol};
use log::info;
use std::sync::Arc;
use tokio::sync;
use tokio::task::JoinHandle;

use kycom::{Forwarder, TcpForwarder};
use log::{debug, warn};

use crate::{Error, Result};

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

pub struct IpcHandler {
    kycom: kycom::KyCom,
    tasks: Vec<Task>,
}

impl IpcHandler {
    pub async fn new(local_ports: std::ops::Range<u16>) -> Result<Self> {
        for port in local_ports {
            let kycom = kycom::KyCom::start_on_port(port).await;
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

    pub async fn stop(&mut self) {
        let tasks = std::mem::take(&mut self.tasks);

        for task in tasks {
            debug!("Stopping {} forwarder", task.name);
            task.stop().await;
        }
    }

    pub fn register_and_forward<Endpoint>(&mut self, endpoint: Endpoint) -> Result<String>
    where
        Endpoint: ProtocolEndpoint + Send + 'static,
        TcpForwarder<Endpoint>: Forwarder,
    {
        let forwarder = self.kycom.register(endpoint)?;
        let uri = forwarder.addr().url();

        self.forward(forwarder)?;
        Ok(uri)
    }

    fn forward<T>(&mut self, forwarder: T) -> Result<()>
    where
        T: Forwarder + Send + 'static,
    {
        let forwarder_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("Unknown");
        debug!("Spawning {} forwarder", forwarder_name);

        let (stop_tx, stop_rx) = sync::oneshot::channel();

        let join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = stop_rx => {
                    info!("Stopping {} forwarder", forwarder_name);
                },
                ret = forwarder.forward() => {
                    if let Err(err) = ret {
                        info!("{} forwarder ended with result {err:?}", forwarder_name);
                    }
                }
            }
        });

        self.tasks.push(Task {
            name: forwarder_name,
            stop_tx,
            join_handle,
        });

        Ok(())
    }
}

pub struct IPCForwardableConnection {
    inner: Arc<kyproto::Connection>,
    ipc: IpcHandler,
}

impl IPCForwardableConnection {
    pub async fn new(
        connection: Arc<kyproto::Connection>,
        local_ports: std::ops::Range<u16>,
    ) -> Result<Self> {
        Ok(Self {
            inner: connection,
            ipc: IpcHandler::new(local_ports).await?,
        })
    }

    pub async fn stop(&mut self) {
        self.ipc.stop().await
    }

    pub async fn closed(&self) -> Result<()> {
        self.inner.closed().await?;
        Ok(())
    }

    pub async fn register_and_forward_video_endpoint(
        &mut self,
        id: Option<u16>,
        video_protocol: VideoProtocol,
    ) -> Result<(u16, String)> {
        let endpoint = self
            .inner
            .register_video_endpoint(id, video_protocol)
            .await?;
        let id = endpoint.id();

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok((id, uri))
    }

    pub fn connect_and_forward_video_endpoint(
        &mut self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<String> {
        let endpoint = self.inner.connect_video_endpoint(id, video_protocol)?;

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok(uri)
    }

    pub async fn register_and_forward_audio_endpoint(
        &mut self,
        id: Option<u16>,
        audio_protocol: AudioProtocol,
    ) -> Result<(u16, String)> {
        let endpoint = self
            .inner
            .register_audio_endpoint(id, audio_protocol)
            .await?;
        let id = endpoint.id();

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok((id, uri))
    }

    pub fn connect_and_forward_audio_endpoint(
        &mut self,
        id: u16,
        audio_protocol: AudioProtocol,
    ) -> Result<String> {
        let endpoint = self.inner.connect_audio_endpoint(id, audio_protocol)?;

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok(uri)
    }

    pub async fn register_and_forward_input_endpoint(
        &mut self,
        id: Option<u16>,
    ) -> Result<(u16, String)> {
        let endpoint = self.inner.register_input_endpoint(id).await?;
        let id = endpoint.id();

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok((id, uri))
    }

    pub fn connect_and_forward_input_endpoint(&mut self, id: u16) -> Result<String> {
        let endpoint = self.inner.connect_input_endpoint(id)?;

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok(uri)
    }

    pub fn connect_metrics_endpoint(&mut self, id: u16) -> Result<MetricsClientEndpoint> {
        Ok(self.inner.connect_metrics_endpoint(id)?)
    }
}
