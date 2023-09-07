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
