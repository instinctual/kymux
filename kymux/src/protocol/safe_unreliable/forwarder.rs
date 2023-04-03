use super::receiver::{DatagramMsg, RecvMsg, StreamMsg};
use crate::protocol::seq::Sequencer;
use crate::protocol::{MediaPacket, Packet};
use crate::{Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, warn};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

pub(super) struct Forwarder {
    client_tx: OwnedWriteHalf,
    next_kypacket_seq: u64,

    group_sequencer: Sequencer<u32>,
    kypacket_sequencer: Sequencer<u32>,

    pending_groups: Vec<PendingGroup>,

    deadline: Option<Instant>,
}

#[derive(Debug)]
pub(super) struct DatagramPacket {
    packet: Packet,
    kypacket_seq: u64,
    instant: Instant, // assemble() timestamp
}

#[derive(Debug)]
pub(super) struct StreamPacket {
    packet: Packet,
    kypacket_seq: u64,
}

#[derive(Debug)]
enum NextPacket {
    None,
    Ready(PacketRef),
    Deadline(Instant),
}

#[derive(Debug)]
struct PacketRef {
    pending_group_index: usize,
    config_packet: bool,
}

#[derive(Debug)]
enum ProcessResult {
    Sent,
    Deadline(Instant),
    None,
}

impl Forwarder {
    const MAX_BUFFERING: Duration = Duration::from_millis(50);

    pub(super) fn new(client_tx: OwnedWriteHalf) -> Self {
        Self {
            client_tx,
            next_kypacket_seq: 0,
            group_sequencer: Sequencer::new(),
            kypacket_sequencer: Sequencer::new(),
            pending_groups: Vec::new(),
            deadline: None,
        }
    }

    pub(super) async fn forward_to_client(
        &mut self,
        mut rx: mpsc::Receiver<RecvMsg>,
    ) -> Result<()> {
        loop {
            let deadline = self.deadline;

            // all select! branches are cancel-safe
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(RecvMsg::Stream(msg)) => self.on_new_stream_msg(msg).await?,
                        Some(RecvMsg::Datagram(msg)) => self.on_new_datagram_msg(msg).await?,
                        None => return Err(Error::KymuxProtocolError("Could not receive message".to_string())),
                    }
                }
                Some(_) = async move {
                    match deadline {
                        Some(instant) => {
                            tokio::time::sleep_until(instant).await;
                            Some(())
                        },
                        None => None,
                    }
                } => {
                    self.deadline = None;
                    self.on_deadline().await?;
                }
            }
        }
    }

    async fn on_new_stream_msg(&mut self, msg: StreamMsg) -> Result<()> {
        let StreamMsg {
            packet,
            raw_kypacket_seq,
            raw_group_seq,
        } = msg;

        if let Packet::Codec(packet) = packet {
            debug!("===== write CODEC PACKET");
            self.client_tx.write_all(&packet.header).await?;
        } else {
            let kypacket_seq = self.kypacket_sequencer.seq(raw_kypacket_seq);
            let group_seq = self.group_sequencer.seq(raw_group_seq);

            if kypacket_seq >= self.next_kypacket_seq {
                self.insert_stream_packet(
                    group_seq,
                    StreamPacket {
                        packet,
                        kypacket_seq,
                    },
                );
            }
        }

        self.process().await
    }

    async fn on_new_datagram_msg(&mut self, msg: DatagramMsg) -> Result<()> {
        let group_seq = self.group_sequencer.seq(msg.raw_group_seq);
        let kypacket_seq = self.kypacket_sequencer.seq(msg.raw_kypacket_seq);

        if kypacket_seq >= self.next_kypacket_seq {
            debug!("=== RECV datagram {}:{}", kypacket_seq, msg.datagram_number);
            let index = self.prepare_pending_group(group_seq);
            let pending_group = &mut self.pending_groups[index];
            pending_group.insert_datagram(kypacket_seq, msg.data, msg.datagram_number, msg.end);

            self.process().await
        } else {
            debug!(
                "=== RECV datagram {}:{:?} (dropped)",
                kypacket_seq, msg.datagram_number
            );
            Ok(())
        }
    }

    async fn on_deadline(&mut self) -> Result<()> {
        self.deadline = None;
        self.process().await
    }

    async fn process(&mut self) -> Result<()> {
        let mut has_more = true;
        while has_more {
            has_more = match self.process_next_packet().await? {
                ProcessResult::None => {
                    self.deadline = None;
                    false
                }
                ProcessResult::Deadline(instant) => {
                    self.deadline = Some(instant);
                    false
                }
                ProcessResult::Sent => true,
            }
        }

        Ok(())
    }

    async fn write_media_packet(&mut self, kypacket_seq: u64, packet: Packet) -> Result<()> {
        if let Packet::Media(packet) = packet {
            debug!("===== SEND kypacket {kypacket_seq} to client");
            for i in self.next_kypacket_seq..kypacket_seq {
                warn!("Missing packet {i}");
            }
            self.client_tx.write_all(&packet.data).await?;
            self.next_kypacket_seq = kypacket_seq + 1;
            Ok(())
        } else {
            panic!("Unexpected non-media packet");
        }
    }

    fn next_packet(&self) -> NextPacket {
        for (pending_group_index, pending_group) in self.pending_groups.iter().enumerate() {
            // Ignore pending groups without config packet
            if !pending_group.config_packet.is_none() {
                if let ConfigPacket::Ready(packet) = &pending_group.config_packet {
                    if packet.kypacket_seq == self.next_kypacket_seq {
                        // The config packet is the next expected packet
                        return NextPacket::Ready(PacketRef {
                            pending_group_index,
                            config_packet: true,
                        });
                    }
                }

                let datagrams = &pending_group.datagrams;
                if !datagrams.is_empty() {
                    let mut ready = false;
                    if datagrams[0].kypacket_seq == self.next_kypacket_seq {
                        ready = true;
                    } else if let Some(min_instant) = self.min_instant() {
                        let deadline = min_instant + Self::MAX_BUFFERING;
                        if deadline <= Instant::now() {
                            ready = true;
                        } else {
                            return NextPacket::Deadline(deadline);
                        }
                    }
                    if ready {
                        // Send either the datagram or the previous config packet if not sent yet
                        return NextPacket::Ready(PacketRef {
                            pending_group_index,
                            config_packet: pending_group.config_packet.is_ready(),
                        });
                    }
                }
            }
        }

        NextPacket::None
    }

    fn min_instant(&self) -> Option<Instant> {
        // Minimum of all datagrams instants across all pending groups
        self.pending_groups
            .iter()
            .filter_map(|pending_group| {
                pending_group
                    .datagrams
                    .iter()
                    .map(|datagram| datagram.instant)
                    .min()
            })
            .min()
    }

    async fn process_next_packet(&mut self) -> Result<ProcessResult> {
        let result = match self.next_packet() {
            NextPacket::None => ProcessResult::None,
            NextPacket::Deadline(instant) => ProcessResult::Deadline(instant),
            NextPacket::Ready(packet_ref) => {
                if packet_ref.pending_group_index > 0 {
                    self.pending_groups.drain(..packet_ref.pending_group_index);
                }

                let pending_group = &mut self.pending_groups[0];

                assert!(!pending_group.config_packet.is_none());

                if packet_ref.config_packet {
                    assert!(pending_group.config_packet.is_ready());
                    let config_packet = pending_group.config_packet.consume();
                    self.write_media_packet(config_packet.kypacket_seq, config_packet.packet)
                        .await?;
                    ProcessResult::Sent
                } else {
                    assert!(!pending_group.datagrams.is_empty());
                    let datagram = pending_group.datagrams.remove(0);
                    pending_group.drop_expired_segments(datagram.kypacket_seq);
                    self.write_media_packet(datagram.kypacket_seq, datagram.packet)
                        .await?;
                    ProcessResult::Sent
                }
            }
        };

        Ok(result)
    }

    fn prepare_pending_group(&mut self, group_seq: u64) -> usize {
        let index = self
            .pending_groups
            .binary_search_by_key(&group_seq, |pending_group| pending_group.group_seq);

        match index {
            Ok(index) => index,
            Err(index) => {
                let pending_group = PendingGroup::new(group_seq);
                self.pending_groups.insert(index, pending_group);
                index
            }
        }
    }

    fn insert_stream_packet(&mut self, group_seq: u64, packet: StreamPacket) {
        let index = self.prepare_pending_group(group_seq);

        let pending_group = &mut self.pending_groups[index];
        assert!(pending_group.config_packet.is_none());
        pending_group.config_packet = ConfigPacket::Ready(packet);
    }
}

#[derive(Debug)]
enum ConfigPacket {
    None,
    Ready(StreamPacket),
    Sent,
}

impl ConfigPacket {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    #[allow(dead_code)]
    fn is_sent(&self) -> bool {
        matches!(self, Self::Sent)
    }

    fn consume(&mut self) -> StreamPacket {
        assert!(self.is_ready());
        let ready_state = std::mem::replace(self, Self::Sent);
        if let Self::Ready(packet) = ready_state {
            packet
        } else {
            panic!("Attempting to consume a non-ready config packet");
        }
    }
}

#[derive(Debug)]
struct PendingGroup {
    group_seq: u64,
    config_packet: ConfigPacket,
    datagrams: Vec<DatagramPacket>,
    segments: Vec<DatagramSegments>,
}

impl PendingGroup {
    fn new(group_seq: u64) -> Self {
        Self {
            group_seq,
            config_packet: ConfigPacket::None,
            datagrams: Vec::new(),
            segments: Vec::new(),
        }
    }

    fn insert_datagram(&mut self, kypacket_seq: u64, data: Bytes, datagram_number: u32, end: bool) {
        let index = self
            .datagrams
            .binary_search_by_key(&kypacket_seq, |datagram| datagram.kypacket_seq);
        // If the kypacket having this kypacket_seq is not already re-assembled
        if index.is_err() {
            let index = self.prepare_datagram_segments(kypacket_seq);

            let is_complete = {
                let segments = &mut self.segments[index];
                segments.set_segment(datagram_number as usize, data, end);
                segments.is_complete()
            };

            if is_complete {
                let segments = self.segments.remove(index);
                let datagram_packet = segments.assemble();
                self.insert_datagram_packet(datagram_packet);
            }
        }
    }

    fn prepare_datagram_segments(&mut self, kypacket_seq: u64) -> usize {
        let index = self
            .segments
            .binary_search_by_key(&kypacket_seq, |segments| segments.kypacket_seq);
        match index {
            Ok(index) => index,
            Err(index) => {
                let segments = DatagramSegments::new(kypacket_seq);
                self.segments.insert(index, segments);
                index
            }
        }
    }

    fn drop_expired_segments(&mut self, until_kypacket_seq: u64) {
        let index = self
            .segments
            .binary_search_by_key(&until_kypacket_seq, |segments| segments.kypacket_seq);
        let index = index.unwrap_or_else(|index| index);
        self.segments.drain(..index);
    }

    fn insert_datagram_packet(&mut self, packet: DatagramPacket) {
        let datagram_index = self
            .datagrams
            .binary_search_by_key(&packet.kypacket_seq, |datagram| datagram.kypacket_seq);
        if let Err(datagram_index) = datagram_index {
            self.datagrams.insert(datagram_index, packet);
        }
        // else it is a duplicate, ignore
    }
}

#[derive(Debug)]
struct DatagramSegments {
    kypacket_seq: u64,
    total_segments: Option<usize>, // number of segments in the kypacket
    segments: Vec<Option<Bytes>>,  // datagram i at index i
    segments_count: usize,         // number of non-None datagrams in segments
}

impl DatagramSegments {
    fn new(kypacket_seq: u64) -> Self {
        Self {
            kypacket_seq,
            total_segments: None,
            segments: Vec::new(),
            segments_count: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.total_segments == Some(self.segments_count)
    }

    fn merge_segments(segments: &Vec<Option<Bytes>>) -> Bytes {
        let mut bytes = BytesMut::new();
        for segment in segments {
            let segment = segment.as_ref().expect("Unexpected None segment");
            bytes.put_slice(segment);
        }
        bytes.freeze()
    }

    fn set_segment(&mut self, index: usize, segment: Bytes, end: bool) {
        if index >= self.segments.len() {
            self.segments.resize(index + 1, None);
        }
        assert!(self.segments[index].is_none());
        self.segments[index] = Some(segment);
        self.segments_count += 1;
        if end {
            assert!(self.total_segments.is_none());
            self.total_segments = Some(index + 1)
        }
    }

    fn assemble(self) -> DatagramPacket {
        assert!(self.is_complete());
        let data = Self::merge_segments(&self.segments);

        // TODO for now, the kypacket header is sent "as is" over datagrams.
        // In the future, they might be rewritten (we don't need the same data,
        // for example size is redundant)
        let header = Packet::parse_media_packet_header(&data);

        let packet = Packet::Media(MediaPacket {
            data,
            header,
        });

        DatagramPacket {
            packet,
            kypacket_seq: self.kypacket_seq,
            instant: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let d0 = {
            let mut buf = BytesMut::with_capacity(128);
            buf.put_u32(0);
            let flags = 0b101 << 61; // media + key
            buf.put_u64(0x1234567 | flags);
            buf.put_u32(17); // kypacket_size
            buf.put([1, 3, 5, 7, 9, 11, 13].as_ref()); // random data
            buf.freeze()
        };

        let d1 = Bytes::from([2, 4, 6, 8, 10, 12].as_ref());
        let d2 = Bytes::from([0xF0, 0xE0, 0xD0, 0xC0].as_ref());

        let kypacket_seq = 42;
        let mut datagram_segments = DatagramSegments::new(kypacket_seq);

        assert!(!datagram_segments.is_complete());
        datagram_segments.set_segment(1, d1, false);
        assert!(!datagram_segments.is_complete());
        datagram_segments.set_segment(2, d2, true);
        assert!(!datagram_segments.is_complete());
        datagram_segments.set_segment(0, d0, false);
        assert!(datagram_segments.is_complete());

        let datagram_packet = datagram_segments.assemble();
        assert_eq!(datagram_packet.kypacket_seq, 42);

        if let Packet::Media(packet) = datagram_packet.packet {
            assert_eq!(packet.pts, 0x1234567);
            assert!(!packet.is_config);
            assert!(packet.is_key);
            assert_eq!(packet.data.len(), 33);

            // header
            assert_eq!(
                &packet.data[..16],
                [0, 0, 0, 0, 0xA0, 0, 0, 0, 1, 0x23, 0x45, 0x67, 0, 0, 0, 17]
            );
            // payload
            assert_eq!(
                &packet.data[16..],
                [1, 3, 5, 7, 9, 11, 13, 2, 4, 6, 8, 10, 12, 0xF0, 0xE0, 0xD0, 0xC0]
            );
        } else {
            panic!("Not a media packet");
        }
    }
}
