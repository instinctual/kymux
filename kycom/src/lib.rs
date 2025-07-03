use std::net::SocketAddr;

#[allow(unused)]
use log::{debug, error, info, warn};

mod client;
mod ipc;
mod serial;
mod server;

pub use client::{
    InputEndpoint, IpcEndpoint, MetricsClientEndpoint, MetricsServerEndpoint, VideoClientEndpoint,
    VideoServerEndpoint,
};
pub use ipc::{Ipc, IpcRecv, IpcSend};
pub use server::{Forwarder, KyCom, TcpForwarder};

pub struct KyComAddr {
    pub addr: SocketAddr,
    pub endpoint_id: u16,
}

impl KyComAddr {
    fn new(addr: SocketAddr, endpoint_id: u16) -> Self {
        Self { addr, endpoint_id }
    }

    pub fn url(&self) -> String {
        format!("kymux://{}/{:X}", self.addr, self.endpoint_id)
    }
}
