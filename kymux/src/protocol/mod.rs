use async_trait::async_trait;
use log::{debug, warn};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::router::{KyChannel, KyRecvMsg};
use crate::stream::stream_id_to_u64;
use crate::{EndpointDesc, Error, Result, StreamOwner};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

pub mod gopstream;

#[async_trait]
pub(crate) trait Protocol {
    async fn forward(&mut self, ky_channel: KyChannel, client: TcpStream) -> Result<()>;
}

pub(crate) struct SimpleBiProtocol {
    desc: EndpointDesc,
}

impl SimpleBiProtocol {
    pub(crate) fn new(desc: EndpointDesc) -> Self {
        Self { desc }
    }
}

#[async_trait]
impl Protocol for SimpleBiProtocol {
    async fn forward(&mut self, mut ky_channel: KyChannel, client: TcpStream) -> Result<()> {
        let (tx, rx) = if self.desc.owner == StreamOwner::Local {
            ky_channel.open_bi().await?
        } else if let KyRecvMsg::AcceptBi(tx, rx) = ky_channel.recv().await? {
            (tx, rx)
        } else {
            return Err(Error::KymuxProtocolError(
                "Unexpected message, expected AcceptBi".to_string(),
            ));
        };

        let (client_rx, client_tx) = client.into_split();

        let rx_task = async move {
            let ret = rx_task(rx, client_tx).await;
            debug!("Quic -> Client: {ret:?}");
        };

        let tx_task = async move {
            let ret = tx_task(tx, client_rx).await;
            debug!("Client -> Quic: {ret:?}");
        };

        tokio::join!(rx_task, tx_task);
        Ok(())
    }
}

pub(crate) struct SimpleUniProtocol {
    desc: EndpointDesc,
}

impl SimpleUniProtocol {
    pub(crate) fn new(desc: EndpointDesc) -> Self {
        Self { desc }
    }
}

#[async_trait]
impl Protocol for SimpleUniProtocol {
    async fn forward(&mut self, mut ky_channel: KyChannel, client: TcpStream) -> Result<()> {
        let (client_rx, client_tx) = client.into_split();

        if self.desc.owner == StreamOwner::Local {
            let tx = ky_channel.open_uni().await?;
            tx_task(tx, client_rx).await?;
        } else if let KyRecvMsg::AcceptUni(rx) = ky_channel.recv().await? {
            rx_task(rx, client_tx).await?;
        } else {
            return Err(Error::KymuxProtocolError(
                "Unexpected message, expected AcceptUni".to_string(),
            ));
        }

        Ok(())
    }
}

async fn rx_task(mut quic_rx: quinn::RecvStream, mut client_tx: OwnedWriteHalf) -> Result<()> {
    tokio::io::copy(&mut quic_rx, &mut client_tx).await?;
    Ok(())
}

async fn tx_task(mut quic_tx: quinn::SendStream, mut client_rx: OwnedReadHalf) -> Result<()> {
    let ret = tokio::io::copy(&mut client_rx, &mut quic_tx).await;

    {
        let ret = quic_tx.reset(quinn::VarInt::from_u32(0));
        if let Err(err) = ret {
            warn!(
                "Fail to reset Quic stream {id}: {err:?}",
                id = stream_id_to_u64(quic_tx.id())
            );
        }
    }

    ret?;
    Ok(())
}

#[derive(Debug)]
pub(crate) enum Packet {
    Codec(CodecPacket),
    Media(MediaPacket),
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct CodecPacket {
    header: Vec<u8>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct MediaPacket {
    data: Vec<u8>, // includes header and payload
    header: MediaPacketHeader,
}

#[derive(Debug)]
pub(crate) struct MediaPacketHeader {
    stream_id: u32,
    is_config: bool,
    is_key: bool,
    pts: u64,
    size: u32,
}

impl Packet {
    pub(crate) async fn read(input: &mut (impl AsyncReadExt + Unpin)) -> Result<Packet> {
        let mut buf = vec![0u8; 16];
        input.read_exact(&mut buf).await?;
        assert!(buf.len() == 16);

        let is_media_packet = buf[4] & 0x80 != 0;
        let packet = if is_media_packet {
            let header = Self::parse_media_packet_header(&buf);

            debug!(
                "[MEDIA stream_id={}] is_config={} is_key={} pts={} size={}",
                header.stream_id, header.is_config, header.is_key, header.pts, header.size
            );

            buf.resize(16 + header.size as usize, 0);
            input.read_exact(&mut buf[16..]).await?;

            Packet::Media(MediaPacket { data: buf, header })
        } else {
            Packet::Codec(CodecPacket { header: buf })
        };

        Ok(packet)
    }

    pub(crate) fn parse_media_packet_header(buf: &[u8]) -> MediaPacketHeader {
        assert!(buf[4] & 0x80 != 0); // media packet
        let stream_id = u32::from_be_bytes(buf[..4].try_into().unwrap());
        let pts_and_flags = u64::from_be_bytes(buf[4..12].try_into().unwrap());
        let is_config = (pts_and_flags & 0x40_00_00_00_00_00_00_00) != 0;
        let is_key = (pts_and_flags & 0x20_00_00_00_00_00_00_00) != 0;
        let pts = pts_and_flags & 0x1F_FF_FF_FF_FF_FF_FF_FF;
        let size = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        MediaPacketHeader {
            stream_id,
            is_config,
            is_key,
            pts,
            size,
        }
    }
}
