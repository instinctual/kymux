// Project Kyber: task.rs
// Copyright © 2022-2026 Kyber SAS
// SPDX-License-Identifier: LicenseRef-Kyber-Commercial OR AGPL-3.0-or-later
//
// This file is both under dual license: AGPLv3 and a Commercial one.
//
// ----
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use crate::runtime;

use std::future::Future;

use kymux_util::*;
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::sync::oneshot;

pub(crate) struct Task {
    pub(crate) name: String,
    tx: oneshot::Sender<()>,
    handle: runtime::JoinHandle<()>,
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

        let handle = runtime::spawn(async move {
            tokio::select! {
                _ = rx => {
                    debug!("Task {task_name} interrupted");
                }
                _ = task => {
                    debug!("Task {task_name} terminated");
                }
            }
        });

        Task { name, tx, handle }
    }

    pub(crate) fn cancel(self) -> Result<(), ()> {
        // Sending the cooperative signal and then aborting without yielding
        // guarantees the transport task cannot observe a connection close as
        // an error during an intentional local shutdown. The signal retains
        // the graceful path for runtimes that poll it immediately; abort is
        // the deterministic backstop on every supported runtime.
        let result = self.tx.send(());
        self.handle.abort();
        result
    }
}
