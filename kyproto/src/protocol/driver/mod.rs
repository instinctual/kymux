use crate::error::ProtocolError;
use crate::router;

use async_trait::async_trait;
use kynet::error::*;

pub(crate) mod av;
pub(crate) mod input;
pub(crate) mod util;

#[cfg(target_family = "wasm")]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub(crate) trait ProtocolSendDriver {
    type Packet;

    async fn send(&mut self, packet: Self::Packet) -> Result<(), ProtocolError>;
}

#[cfg(not(target_family = "wasm"))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub(crate) trait ProtocolSendDriver: Send {
    type Packet;

    async fn send(&mut self, packet: Self::Packet) -> Result<(), ProtocolError>;
}

#[cfg(target_family = "wasm")]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub(crate) trait ProtocolRecvDriver {
    type Packet;

    async fn recv(&mut self) -> Result<Option<Self::Packet>, ProtocolError>;
}

#[cfg(not(target_family = "wasm"))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub(crate) trait ProtocolRecvDriver: Send {
    type Packet;

    async fn recv(&mut self) -> Result<Option<Self::Packet>, ProtocolError>;
}

macro_rules! impl_protocol_error_from {
    ($t:ty) => {
        impl From<$t> for ProtocolError {
            fn from(value: $t) -> Self {
                Self(format!("{value:?}"))
            }
        }
    };
}

impl_protocol_error_from!(ConnectionError);
impl_protocol_error_from!(SendDatagramError);
impl_protocol_error_from!(ReadError);
impl_protocol_error_from!(ReadExactError);
impl_protocol_error_from!(WriteError);
impl_protocol_error_from!(UnknownStreamError);
impl_protocol_error_from!(router::RouterError);
