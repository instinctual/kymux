use crate::protocol::av::{AVPacket, AVPacketHeader, CodecPacket, MediaPacket};
use crate::protocol::ProtocolError;

use bytes::BytesMut;
use kynet::error::ReadExactError;
use kynet::RecvStream;

pub(crate) async fn read_packet(recv: &mut RecvStream) -> Result<Option<AVPacket>, ProtocolError> {
    let mut header = [0; 12];
    let res = recv.read_exact(&mut header).await;
    if let Err(ReadExactError::EndOfStream) = res {
        return Ok(None);
    }
    res?;

    let header = AVPacketHeader::deserialize(&header);
    let packet = match header {
        AVPacketHeader::Media(header) => {
            let mut buf = BytesMut::zeroed(header.size as usize);

            recv.read_exact(&mut buf).await?;

            AVPacket::Media(MediaPacket {
                header,
                payload: buf.freeze(),
            })
        }
        AVPacketHeader::Codec(header) => AVPacket::Codec(CodecPacket { header }),
    };

    Ok(Some(packet))
}
