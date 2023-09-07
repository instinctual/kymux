#![allow(unused)] // TODO remove

use crate::protocol::driver::{ProtocolRecvDriver, ProtocolSendDriver};

use async_trait::async_trait;
use thiserror::Error;

mod av;
mod driver;

#[derive(Debug, Error)]
#[error("Protoocol error: {0}")]
pub struct ProtocolError(String);

pub struct ProtocolSend<T> {
    driver: Box<dyn ProtocolSendDriver<Packet = T>>,
}

impl<T> ProtocolSend<T> {
    pub async fn send(&mut self, packet: T) -> Result<(), ProtocolError> {
        self.driver.send(packet).await
    }
}

pub struct ProtocolRecv<T> {
    driver: Box<dyn ProtocolRecvDriver<Packet = T>>,
}

impl<T> ProtocolRecv<T> {
    pub async fn recv(&mut self) -> Result<Option<T>, ProtocolError> {
        self.driver.recv().await
    }
}
