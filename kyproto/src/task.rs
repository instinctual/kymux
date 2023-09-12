use crate::runtime;

use std::future::Future;

#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::sync::oneshot;

pub(crate) struct Task {
    pub(crate) name: String,
    tx: oneshot::Sender<()>,
}

impl Task {
    pub(crate) fn spawn_task<F>(task: F, name: String) -> Self
    where
        F: Future<Output = ()> + runtime::NonWasmSend + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let task_name = name.clone();

        runtime::spawn(async move {
            tokio::select! {
                _ = rx => {
                    debug!("Task {task_name} interrupted");
                }
                _ = task => {
                    debug!("Task {task_name} terminated");
                }
            }
        });

        Task { name, tx }
    }

    pub(crate) fn cancel(self) -> Result<(), ()> {
        self.tx.send(())
    }
}
