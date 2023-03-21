#[allow(unused_imports)]
use log::{debug, error, info, warn};

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::router::{KyChannel, KyRecvMsg};
use crate::stream::stream_id_to_u64;
use crate::{Error, Result, StreamOwner, StreamType};

#[derive(Clone, Copy, Debug)]
pub struct EndpointDesc {
    pub id: u64,
    pub owner: StreamOwner,
    pub type_: StreamType,
}

#[derive(Debug)]
pub(crate) struct EndpointBuilder {
    desc: EndpointDesc,

    // Mux stream client
    client: Option<TcpStream>,
    peer_client_connected: bool,

    // Quic
    ky_channel: Option<KyChannel>,
}

impl EndpointBuilder {
    pub(crate) fn new(desc: EndpointDesc) -> Self {
        Self {
            desc,
            client: None,
            peer_client_connected: false,
            ky_channel: None,
        }
    }

    pub(crate) fn set_client(&mut self, client: TcpStream) -> Result<()> {
        if self.client.is_some() {
            error!("Try to set client more than once");
            return Err(Error::FatalError);
        }

        self.client = Some(client);

        Ok(())
    }

    pub(crate) fn peer_client_connected(&mut self) -> Result<()> {
        if self.peer_client_connected {
            error!("Try to notify that the peer is connected more than once");
            return Err(Error::FatalError);
        }

        self.peer_client_connected = true;

        Ok(())
    }

    pub(crate) fn set_ky_channel(&mut self, ky_channel: KyChannel) -> Result<()> {
        if self.ky_channel.is_some() {
            error!("Try to set ky_channel more than once");
            return Err(Error::FatalError);
        }

        self.ky_channel = Some(ky_channel);

        Ok(())
    }

    pub(crate) fn ready(&self) -> bool {
        self.peer_client_connected && self.client.is_some() && self.ky_channel.is_some()
    }

    pub(crate) async fn build(mut self) -> Result<Endpoint> {
        if !self.peer_client_connected {
            return Err(Error::EndpointBuilderNotReady);
        }

        let Some(client) = self.client.take() else {
            return Err(Error::EndpointBuilderNotReady);
        };

        let Some(ky_channel) = self.ky_channel.take() else {
            return Err(Error::EndpointBuilderNotReady);
        };

        Endpoint::new(self.desc, client, ky_channel).await
    }
}

#[derive(Debug)]
pub(crate) struct Endpoint {
    // Description
    desc: EndpointDesc,
}

impl Endpoint {
    pub(crate) fn desc(&self) -> &EndpointDesc {
        &self.desc
    }

    pub(crate) async fn new(
        desc: EndpointDesc,
        mut client: TcpStream,
        mut ky_channel: KyChannel,
    ) -> Result<Self> {
        debug!("Endpoint {id:X} ready: start routing", id = desc.id);

        // Send sync notification to client
        let sync = [0u8];
        client.write_all(&sync).await?;

        // Forward data
        let (client_rx, client_tx) = client.into_split();

        let bidir = desc.type_ == StreamType::Input;
        let (quic_stream_tx, quic_stream_rx) = match (desc.owner, bidir) {
            (StreamOwner::Local, true) => {
                let (tx, rx) = ky_channel.open_bi().await?;
                (Some(tx), Some(rx))
            }
            (StreamOwner::Local, false) => {
                let tx = ky_channel.open_uni().await?;
                (Some(tx), None)
            }
            (StreamOwner::Peer, bidir) => match ky_channel.recv().await? {
                KyRecvMsg::AcceptUni(rx) => {
                    assert!(!bidir);
                    (None, Some(rx))
                }
                KyRecvMsg::AcceptBi(tx, rx) => {
                    assert!(bidir);
                    (Some(tx), Some(rx))
                }
            },
        };

        if let Some(rx) = quic_stream_rx {
            tokio::spawn(async move {
                let ret = Self::rx_task(rx, client_tx).await;
                debug!("Quic -> Client: {ret:?}");
            });
        };

        if let Some(tx) = quic_stream_tx {
            tokio::spawn(async move {
                let ret = Self::tx_task(tx, client_rx).await;
                debug!("Client -> Quic: {ret:?}");
            });
        };

        Ok(Self { desc })
    }

    async fn rx_task(mut quic_rx: quinn::RecvStream, mut client_tx: OwnedWriteHalf) {
        let ret = tokio::io::copy(&mut quic_rx, &mut client_tx).await;
        debug!("Quic -> Client: {ret:?}");
    }

    async fn tx_task(mut quic_tx: quinn::SendStream, mut client_rx: OwnedReadHalf) {
        let ret = tokio::io::copy(&mut client_rx, &mut quic_tx).await;
        debug!("Client -> Quic: {ret:?}");

        let ret = quic_tx.reset(quinn::VarInt::from_u32(0));
        if let Err(err) = ret {
            warn!(
                "Fail to reset Quic stream {id}: {err:?}",
                id = stream_id_to_u64(quic_tx.id())
            );
        }
    }
}
