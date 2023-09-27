use crate::runtime;

use std::future::Future;

#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::sync::oneshot;

pub(crate) struct Task {
    pub(crate) name: String,
    tx: oneshot::Sender<()>,
}

macro_rules! impl_spawn_task {
    ($task:expr, $name:expr) => {{
        let (tx, rx) = oneshot::channel();
        let task_name = $name.clone();

        runtime::spawn(async move {
            tokio::select! {
                _ = rx => {
                    debug!("Task {task_name} interrupted");
                }
                _ = $task => {
                    debug!("Task {task_name} terminated");
                }
            }
        });

        Task { name: $name, tx }
    }};
}

impl Task {
    #[cfg(all(feature = "tokio-rt", not(target_family = "wasm")))]
    pub(crate) fn spawn_task<F, S>(task: F, name: S) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
        S: Into<String>,
    {
        let name = name.into();
        impl_spawn_task!(task, name)
    }

    #[cfg(all(feature = "js", target_family = "wasm"))]
    pub(crate) fn spawn_task<F, S>(task: F, name: S) -> Self
    where
        F: Future<Output = ()> + 'static,
        S: Into<String>,
    {
        let name = name.into();
        impl_spawn_task!(task, name)
    }

    pub(crate) fn cancel(self) -> Result<(), ()> {
        self.tx.send(())
    }
}
