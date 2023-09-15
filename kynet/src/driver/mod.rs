use crate::error::*;
use crate::{RecvStream, SendStream};

use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
mod quinn;
#[cfg(all(feature = "quinn", target_family = "wasm"))]
compile_error!("Quinn is not available for wasm");

#[cfg(all(feature = "webtransport-js", target_family = "wasm"))]
mod webtransport_js;
#[cfg(all(feature = "webtransport-js", not(target_family = "wasm")))]
compile_error!("WebTransportJS is only available for wasm");

#[cfg(all(feature = "wtransport", not(target_family = "wasm")))]
mod wtransport;
#[cfg(all(feature = "wtransport", target_family = "wasm"))]
compile_error!("WTransport is not available for wasm");

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ConnectionDriver: Debug {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError>;

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError>;

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError>;

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError>;

    async fn closed(&self) -> Result<(), ConnectionError>;

    fn close(&self, error_code: u32, reason: &str);

    fn max_datagram_size(&self) -> Option<usize>;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait SendStreamDriver: Debug {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError>;

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        let mut offset = 0;
        while offset < buf.len() {
            let w = self.write(&buf[offset..]).await?;
            assert!(w <= buf.len() - offset);
            offset += w;
            if offset == buf.len() {
                break;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), WriteError>;

    async fn abort(self: Box<Self>) -> Result<(), UnknownStreamError>;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait RecvStreamDriver: Debug {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError>;

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError> {
        let mut offset = 0;
        while let Some(r) = self.read(&mut buf[offset..]).await? {
            assert!(r <= buf.len() - offset);
            offset += r;
            if offset == buf.len() {
                break;
            }
        }
        Ok(())
    }
}
