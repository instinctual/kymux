use crate::error::EndpointAlreadyRegistered;
use crate::task::Task;
use crate::util::{KyArc, KyMutex};

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use kynet::error::*;
use kynet::{RecvStream, SendStream};
#[allow(unused)]
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Error)]
#[error("control error: {0}")]
pub(crate) struct ControlError(String);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ControlMsg {
    RequestStart { endpoint_id: u16 },
}

type WaiterMap = HashMap<u16, oneshot::Sender<()>>;

pub(crate) struct Control {
    channel_tx: mpsc::Sender<ControlMsg>,
    waiters: KyArc<KyMutex<WaiterMap>>,
    tasks: Vec<Task>,
}

impl Control {
    pub(crate) fn start(stream_tx: SendStream, stream_rx: RecvStream) -> Self {
        let waiters = KyArc::new(KyMutex::new(HashMap::new()));

        let waiters2 = waiters.clone();
        let rx_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::recv_msgs(stream_rx, waiters2).await {
                    error!("{err:?}");
                }
            },
            "control recv_msgs".to_string(),
        );

        let (channel_tx, channel_rx) = mpsc::channel(8);
        let tx_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::send_msgs(channel_rx, stream_tx).await {
                    error!("{err:?}");
                }
            },
            "control send_msgs".to_string(),
        );

        let tasks = vec![rx_task, tx_task];
        Self {
            channel_tx,
            waiters,
            tasks,
        }
    }

    pub(crate) fn register_start_request_receiver(
        &self,
        endpoint_id: u16,
    ) -> Result<oneshot::Receiver<()>, EndpointAlreadyRegistered> {
        let mut waiters = self.waiters.lock();
        match waiters.entry(endpoint_id) {
            Entry::Occupied(_) => return Err(EndpointAlreadyRegistered { endpoint_id }),
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(tx);
                Ok(rx)
            }
        }
    }

    pub(crate) fn control_msg_sender(&self) -> &mpsc::Sender<ControlMsg> {
        &self.channel_tx
    }

    pub(crate) fn stop(&mut self) {
        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            let name = task.name.clone();
            let ret = task.cancel();
            if ret.is_err() {
                warn!("Task {name} seems to be already stopped");
            }
        }
    }

    async fn send_msgs(
        mut channel_rx: mpsc::Receiver<ControlMsg>,
        mut stream_tx: SendStream,
    ) -> Result<(), ControlError> {
        while let Some(msg) = channel_rx.recv().await {
            Self::send_msg(&mut stream_tx, &msg).await?;
        }

        Ok(())
    }

    async fn send_msg(tx: &mut SendStream, msg: &ControlMsg) -> Result<(), ControlError> {
        let buf = rmp_serde::to_vec(&msg)?;
        let len = u32::try_from(buf.len()).unwrap();

        tx.write_all(&len.to_be_bytes()).await?;
        tx.write_all(&buf).await?;
        Ok(())
    }

    async fn recv_msgs(
        mut rx: RecvStream,
        waiters: KyArc<KyMutex<WaiterMap>>,
    ) -> Result<(), ControlError> {
        while let Some(msg) = Self::recv_msg(&mut rx).await? {
            if let Err(err) = Self::handle_msg(&msg, &waiters) {
                // not fatal
                error!("Could not handle control message: {err}");
            }
        }

        Ok(())
    }

    async fn recv_msg(rx: &mut RecvStream) -> Result<Option<ControlMsg>, ControlError> {
        let mut buf = [0u8; 4];
        let res = rx.read_exact(&mut buf).await;
        if let Err(ReadExactError::EndOfStream) = res {
            return Ok(None);
        }
        res?;

        let len = u32::from_be_bytes(buf);
        let mut buf = vec![0u8; len as usize];
        rx.read_exact(&mut buf).await?;

        let msg = rmp_serde::from_slice(&buf)?;

        Ok(Some(msg))
    }

    fn handle_msg(
        msg: &ControlMsg,
        waiters: &KyArc<KyMutex<WaiterMap>>,
    ) -> Result<(), ControlError> {
        match msg {
            ControlMsg::RequestStart { endpoint_id } => {
                let mut waiters = waiters.lock();
                if let Some(sender) = waiters.remove(endpoint_id) {
                    sender
                        .send(())
                        .map_err(|_| ControlError(format!("Start request sender error")))?;
                } else {
                    Err(ControlError(format!(
                        "Received unexpected start request for endpoint {endpoint_id}"
                    )))?;
                }
            }
        }

        Ok(())
    }
}

impl Drop for Control {
    fn drop(&mut self) {
        self.stop();
    }
}

// ControlError is internal, there is no need to expose a structured error, so
// convert any error to a String
macro_rules! impl_control_error_from {
    ($t:ty) => {
        impl From<$t> for ControlError {
            fn from(err: $t) -> Self {
                Self(format!("{err:?}"))
            }
        }
    };
}
impl_control_error_from!(ReadExactError);
impl_control_error_from!(WriteError);
impl_control_error_from!(rmp_serde::decode::Error);
impl_control_error_from!(rmp_serde::encode::Error);
