use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;

#[derive(Debug)]
pub enum AVPacket {
    Codec(CodecPacket),
    Media(MediaPacket),
}

#[derive(Debug)]
pub struct CodecPacket {
    pub codec: u32,
    pub rotation: u8,
}

#[derive(Debug)]
pub struct MediaPacket {
    pub header: MediaPacketHeader,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct MediaPacketHeader {
    pub is_config: bool,
    pub is_key: bool,
    pub pts: u64,
    pub size: u32,
}

impl CodecPacket {
    pub(crate) fn serialize(&self, buf: &mut [u8]) {
        assert!(buf.len() == 12);

        BigEndian::write_u32(&mut buf[..4], self.codec);
        buf[4] = self.rotation;
        buf[5..].fill(0);
    }

    pub(crate) fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == 12);

        let codec = BigEndian::read_u32(&buf[..4]);
        let rotation = buf[4];
        Self { codec, rotation }
    }
}

impl MediaPacketHeader {
    pub(crate) fn serialize(&self, buf: &mut [u8]) {
        assert!(buf.len() == 12);

        assert!(self.pts & !0x1F_FF_FF_FF_FF_FF_FF_FF == 0);
        let mut pts_and_flags = self.pts;
        pts_and_flags |= 1 << 63; // media packet
        if self.is_config {
            pts_and_flags |= 1 << 62;
        }
        if self.is_key {
            pts_and_flags |= 1 << 61;
        }

        BigEndian::write_u64(&mut buf[..8], pts_and_flags);
        BigEndian::write_u32(&mut buf[8..], self.size);
    }

    pub(crate) fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == 12);

        assert!(buf[0] & 0x80 != 0); // media packet
        let pts_and_flags = BigEndian::read_u64(&buf[..8]);
        let is_config = (pts_and_flags & (1 << 62)) != 0;
        let is_key = (pts_and_flags & (1 << 61)) != 0;
        let pts = pts_and_flags & ((1 << 61) - 1);
        let size = BigEndian::read_u32(&buf[8..]);

        MediaPacketHeader {
            is_config,
            is_key,
            pts,
            size,
        }
    }
}
