use std::mem;
use std::ops::DerefMut;
use std::sync::Mutex;

use log::info;
use tokio::sync;
use tokio::task::JoinHandle;

use kycom::ForwarderProtocol;
use log::{debug, warn};

use crate::Result;

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
        let kycom = kycom::KyCom::start_on_port(local_clients_port).await?;

        Ok(Self {
            kycom,
            tasks: Default::default(),
        })
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
