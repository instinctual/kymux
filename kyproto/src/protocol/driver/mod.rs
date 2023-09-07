use crate::protocol::ProtocolError;

use async_trait::async_trait;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub(crate) trait ProtocolSendDriver {
    type Packet;

    async fn send(&mut self, packet: Self::Packet) -> Result<(), ProtocolError>;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub(crate) trait ProtocolRecvDriver {
    type Packet;

    async fn recv(&mut self) -> Result<Option<Self::Packet>, ProtocolError>;
}
