use crate::protocol::{MediaPacket, Packet};
use crate::router::KyChannelSend;
use crate::{Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use bytes::{BufMut, BytesMut};
use tokio::net::tcp::OwnedReadHalf;

pub(super) struct Sender;

impl Sender {
    pub(super) fn new() -> Self {
        Self
    }

    pub(super) async fn send_video(
        &self,
        ky_channel_tx: KyChannelSend,
        mut client_rx: OwnedReadHalf,
    ) -> Result<()> {
        let mut kypacket_seq = 0;
        let mut group_seq = u32::MAX; // so that the first group is 0

        let mut stream = ky_channel_tx.open_uni().await?;
        loop {
            let packet = Packet::read(&mut client_rx).await?;
            match packet {
                Packet::Codec(packet) => {
                    // Send over QUIC stream:
                    //  - kypacket_seq: 32 bits (meaningless)
                    //  - group_seq: 32 bits (meaningless)
                    //  - kypacket: 16 bytes (no payload)
                    let mut buf = BytesMut::with_capacity(8 + packet.header.len());
                    buf.put_u32(0); // meaningless
                    buf.put_u32(0); // meaningless
                    buf.put(&packet.header[..]);
                    stream.write_all(&buf).await?;
                }
                Packet::Media(packet) => {
                    if packet.header.is_config {
                        // Send over QUIC stream
                        //  - kypacket_seq: 32 bits
                        //  - group_seq: 32 bits
                        //  - kypacket: 16 bytes + payload
                        group_seq = if group_seq == u32::MAX {
                            0
                        } else {
                            group_seq + 1
                        };
                        let mut buf = BytesMut::with_capacity(8 + packet.data.len());
                        buf.put_u32(kypacket_seq);
                        buf.put_u32(group_seq);
                        buf.put(&packet.data[..]);
                        stream.write_all(&buf).await?;
                    } else {
                        self.send_datagrams(kypacket_seq, group_seq, packet, &ky_channel_tx)
                            .await?;
                    }
                    kypacket_seq = if kypacket_seq == u32::MAX {
                        0
                    } else {
                        kypacket_seq + 1
                    };
                }
            }
        }
    }

    async fn send_datagrams(
        &self,
        kypacket_seq: u32,
        group_seq: u32,
        packet: MediaPacket,
        ky_channel_tx: &KyChannelSend,
    ) -> Result<()> {
        let mut offset = 0;
        let mut datagram_number = 0;
        while offset < packet.data.len() {
            // The max_datagram_size may change over time, so call it for
            // every packet
            if let Some(max_datagram_size) = ky_channel_tx.max_datagram_size() {
                const HEADER_SIZE: usize = 20;
                assert!(max_datagram_size > HEADER_SIZE);
                // Datagram header:
                //  - endpoint id (to be written explicitly): 64 bits
                //  - kypacket_seq: 32 bits
                //  - group_seq: 32 bits (incremented on each config packet)
                //  - last datagram of ky-packet flag (end): 1 bit
                //  - datagram number: 31 bits
                //  - kypacket segment
                let payload_size =
                    std::cmp::min(max_datagram_size - HEADER_SIZE, packet.data.len() - offset);
                let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload_size);

                assert!(payload_size < 1 << 16);

                let end = offset + payload_size == packet.data.len();
                let datagram_number_and_end = datagram_number | if end { 1 << 31 } else { 0 };

                ky_channel_tx.write_datagram_header(&mut buf);
                buf.put_u32(kypacket_seq);
                buf.put_u32(group_seq);
                buf.put_u32(datagram_number_and_end);
                buf.put(&packet.data[offset..offset + payload_size]);

                debug!(
                    "#### send datagram {:?}:{:?} (group={:?})",
                    kypacket_seq, datagram_number, group_seq
                );
                ky_channel_tx.send_datagram(buf.freeze()).await?;

                offset += payload_size;
                datagram_number += 1;
            } else {
                return Err(Error::KymuxProtocolError(
                    "Datagrams not supported".to_string(),
                ));
            }
        }

        Ok(())
    }
}
