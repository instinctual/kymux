use crate::runtime;

use std::future::Future;

use kyutil::*;
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::sync::oneshot;

pub(crate) struct Task {
    pub(crate) name: String,
    tx: oneshot::Sender<()>,
}

impl Task {
    pub(crate) fn spawn_task<F, S>(task: F, name: S) -> Self
    where
        F: Future<Output = ()> + KySend + 'static,
        S: Into<String>,
    {
        let name = name.into();
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
