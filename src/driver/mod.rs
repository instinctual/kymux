use crate::error::*;
use crate::{RecvStream, SendStream};

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(all(feature = "quinn", not(target_arch = "wasm32")))]
mod quinn;
#[cfg(all(feature = "quinn", target_arch = "wasm32"))]
compile_error!("Quinn is not available for wasm32");

#[cfg(all(feature = "webtransport-js", target_arch = "wasm32"))]
mod webtransport_js;
#[cfg(all(feature = "webtransport-js", not(target_arch = "wasm32")))]
compile_error!("WebTransportJS is only available for wasm32");

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError>;

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError>;

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError>;

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError>;

    fn close(&self, error_code: u32, reason: &str);

    fn max_datagram_size(&self) -> Option<usize>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SendStreamDriver {
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

    async fn abort(&mut self) -> Result<(), UnknownStreamError>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RecvStreamDriver {
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
