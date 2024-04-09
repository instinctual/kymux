use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;

#[derive(Debug)]
pub enum AVPacket {
    Codec(CodecPacket),
    Media(MediaPacket),
}

#[derive(Debug)]
pub enum AVPacketHeader {
    Codec(CodecPacketHeader),
    Media(MediaPacketHeader),
}

impl AVPacketHeader {
    pub const SERIALIZED_SIZE: usize = 12;
}

#[derive(Debug)]
pub struct CodecPacket {
    pub header: CodecPacketHeader,
}

#[derive(Debug)]
pub struct CodecPacketHeader {
    pub codec: u32,
    pub rotation: u8,
    pub frame_size: u16,
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

impl CodecPacketHeader {
    pub fn serialize_to(&self, buf: &mut [u8]) {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);

        BigEndian::write_u32(&mut buf[..4], self.codec);
        buf[4] = self.rotation;
        BigEndian::write_u16(&mut buf[5..7], self.frame_size);
        buf[7..].fill(0);
    }

    pub fn serialize(&self) -> [u8; AVPacketHeader::SERIALIZED_SIZE] {
        let mut buf = [0; AVPacketHeader::SERIALIZED_SIZE];
        self.serialize_to(&mut buf);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);
        assert!(buf[0] & 0x80 == 0); // codec packet

        let codec = BigEndian::read_u32(&buf[..4]);
        let rotation = buf[4];
        let frame_size = BigEndian::read_u16(&buf[5..7]);
        Self {
            codec,
            rotation,
            frame_size,
        }
    }
}

impl MediaPacketHeader {
    pub fn serialize_to(&self, buf: &mut [u8]) {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);

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

    pub fn serialize(&self) -> [u8; AVPacketHeader::SERIALIZED_SIZE] {
        let mut buf = [0; AVPacketHeader::SERIALIZED_SIZE];
        self.serialize_to(&mut buf);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);
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

impl AVPacketHeader {
    pub fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);

        let is_codec = buf[0] & 0x80 == 0;
        if is_codec {
            Self::Codec(CodecPacketHeader::deserialize(buf))
        } else {
            Self::Media(MediaPacketHeader::deserialize(buf))
        }
    }
}
