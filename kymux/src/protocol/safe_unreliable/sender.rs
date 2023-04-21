use crate::protocol::{MediaPacket, Packet};
use crate::router::KyChannelSend;
use crate::{Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use bytes::{BufMut, BytesMut};
use raptorq;
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
        let max_datagram_size = ky_channel_tx
            .max_datagram_size()
            .ok_or_else(|| Error::KymuxProtocolError("Datagrams not supported".to_string()))?;
        const HEADER_SIZE: usize = 32;
        assert!(max_datagram_size > HEADER_SIZE);
        assert!(max_datagram_size < 0x10000);
        // Datagram header:
        //  - endpoint id (to be written explicitly): 64 bits
        //  - kypacket_seq: 32 bits
        //  - group_seq: 32 bits (incremented on each config packet)
        //  - raptorq Object Transmission Information: 96 bits
        //  - raptorq payload id: 32 bits
        //  - kypacket segment

        let max_payload_size = (max_datagram_size - HEADER_SIZE) as u16;
        let encoder = raptorq::Encoder::with_defaults(&packet.data, max_payload_size);
        let oti = encoder.get_config();

        let kypacket_size = packet.data.len();
        let symbol_size = oti.symbol_size() as usize;

        // div_ceil() not stabilized yet
        let source_symbols = (kypacket_size + symbol_size - 1) / symbol_size;

        // Add 30% repair packets (at least 2 packets)
        let repair_symbols = ((source_symbols as f32 * 0.3).ceil() as u32).max(2);

        let oti = oti.serialize();

        for encoded_packet in encoder.get_encoded_packets(repair_symbols).into_iter() {
            let (payload_id, data) = encoded_packet.split();
            let raw_payload_id = payload_id.serialize();
            let mut buf = BytesMut::with_capacity(HEADER_SIZE + data.len());
            ky_channel_tx.write_datagram_header(&mut buf);
            buf.put_u32(kypacket_seq);
            buf.put_u32(group_seq);
            buf.put(&oti[..]);
            buf.put(&raw_payload_id[..]);
            buf.put(&data[..]);

            debug!(
                "#### send datagram {:?}:{:?} (group={:?})",
                kypacket_seq, payload_id, group_seq
            );
            ky_channel_tx.send_datagram(buf.freeze()).await?;
        }

        Ok(())
    }
}
