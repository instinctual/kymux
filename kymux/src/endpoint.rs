use log::{debug, error, info};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::protocol::gopstream::GopStreamProtocol;
use crate::protocol::unreliable_fec::UnreliableFecProtocol;
use crate::protocol::{Protocol, SimpleBiProtocol, SimpleUniProtocol};
use crate::router::KyChannel;
use crate::{Error, Result, StreamOwner, StreamType, VideoProtocol};

#[derive(Clone, Copy, Debug)]
pub struct EndpointDesc {
    pub id: u16,
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
        ky_channel: KyChannel,
    ) -> Result<Self> {
        debug!("Endpoint {id:X} ready: start routing", id = desc.id);

        // Send sync notification to client
        let sync = [0u8];
        client.write_all(&sync).await?;

        let mut protocol: Box<dyn Protocol + Send> = match desc.type_ {
            StreamType::Input => Box::new(SimpleBiProtocol::new(desc)),
            StreamType::Video(protocol) => {
                info!("Using video protocol '{protocol:?}'");
                match protocol {
                    VideoProtocol::Reliable => Box::new(SimpleUniProtocol::new(desc)),
                    VideoProtocol::GopStream => Box::new(GopStreamProtocol::new(desc)),
                    VideoProtocol::UnreliableFec => Box::new(UnreliableFecProtocol::new(desc)),
                }
            }
            StreamType::Audio => Box::new(SimpleUniProtocol::new(desc)),
        };

        tokio::spawn(async move {
            let ret = protocol.forward(ky_channel, client).await;
            debug!("Protocol {desc:?}: {ret:?}");
        });

        Ok(Self { desc })
    }
}
