use crate::error::ProtocolError;
use crate::router::KyChannel;
use crate::runtime::{self, Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug)]
pub struct ClockSyncResult {
    pub t1: i64, // client emission
    pub t2: i64, // server reception
    pub t3: i64, // server emission
    pub t4: i64, // client reception
}

impl ClockSyncResult {
    pub fn new(t1: u64, t2: u64, t3: u64, t4: u64) -> Self {
        let t1 = i64::try_from(t1).unwrap();
        let t2 = i64::try_from(t2).unwrap();
        let t3 = i64::try_from(t3).unwrap();
        let t4 = i64::try_from(t4).unwrap();
        Self { t1, t2, t3, t4 }
    }

    pub fn offset_micros(&self) -> i64 {
        (self.t2 - self.t1 + self.t3 - self.t4) / 2
    }

    pub fn delay_micros(&self) -> i64 {
        (self.t4 - self.t1) - (self.t3 - self.t2)
    }
}

pub struct ClockSyncClientProtocol {
    ky_channel: KyChannel,
    next_id: u32,
}

impl ClockSyncClientProtocol {
    pub(crate) fn new(ky_channel: KyChannel) -> Self {
        Self {
            ky_channel,
            next_id: 0,
        }
    }

    pub async fn sync(&mut self) -> Result<Option<ClockSyncResult>, ProtocolError> {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);

        let mut buf = BytesMut::with_capacity(14);
        buf.put_u16(self.ky_channel.endpoint_id());
        buf.put_u32(req_id);

        let t1 = now_micros()?;
        buf.put_u64(t1);

        self.ky_channel.send_datagram(buf.freeze()).await?;

        let deadline = t1 + 200000; // 200 ms timeout

        loop {
            let now = now_micros()?;
            if now >= deadline {
                // timeout
                return Ok(None);
            }
            let timeout = Duration::from_micros(deadline - now);
            if let Ok(response) = runtime::timeout(timeout, self.ky_channel.recv_datagram()).await {
                let mut response = response?;
                // endpoint_id: 2 bytes
                // req_id: 4 bytes
                // t1: 8 bytes
                // t2: 8 bytes
                // t3: 8 bytes
                assert!(response.len() == 30);
                let t4 = now_micros()?;
                let endpoint_id = response.get_u16();
                assert!(endpoint_id == self.ky_channel.endpoint_id());
                let id = response.get_u32();
                if req_id == id {
                    let t1 = response.get_u64();
                    let t2 = response.get_u64();
                    let t3 = response.get_u64();
                    return Ok(Some(ClockSyncResult::new(t1, t2, t3, t4)));
                }
            } else {
                // timeout
                return Ok(None);
            }
        }
    }
}

pub struct ClockSyncServerProtocol {
    ky_channel: KyChannel,
}

impl ClockSyncServerProtocol {
    pub(crate) fn new(ky_channel: KyChannel) -> Self {
        Self { ky_channel }
    }

    pub async fn serve(&mut self) -> Result<(), ProtocolError> {
        loop {
            let datagram = self.ky_channel.recv_datagram().await?;
            // endpoint_id: 2 bytes
            // req_id: 4 bytes
            // t1: 8 bytes
            assert!(datagram.len() == 14);
            let t2 = now_micros()?;

            let mut buf = BytesMut::with_capacity(30);
            buf.extend_from_slice(&datagram);
            buf.put_u64(t2);

            let t3 = now_micros()?;

            buf.put_u64(t3);
            self.ky_channel.send_datagram(buf.freeze()).await?;
        }
    }
}

fn now_micros() -> Result<u64, ProtocolError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProtocolError("Timestamp failed".to_string()))?
        .as_micros();
    let signed =
        i64::try_from(now).map_err(|_| ProtocolError("Invalid 63-bit timestamp".to_string()))?;
    signed
        .try_into()
        .map_err(|_| ProtocolError("Invalid 64-bit timestamp".to_string()))
}
