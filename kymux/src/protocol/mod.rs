#![allow(dead_code)]

use async_trait::async_trait;
use log::{debug, warn};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::router::{KyChannel, KyRecvMsg};
use crate::stream::stream_id_to_u64;
use crate::{EndpointDesc, Error, Result, StreamOwner};
use tokio::net::TcpStream;

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
