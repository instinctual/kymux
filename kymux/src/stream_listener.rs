use std::sync::Arc;

use log::{debug, warn};
use tokio::sync::Mutex;

use crate::io_utils;
use crate::stream::stream_id_to_u64;
use crate::{Error, Result, State, StreamDirection, StreamOwner, StreamPair};

pub(crate) struct StreamListener {
    conn: quinn::Connection,
    state: Arc<Mutex<State>>,
}

impl StreamListener {
    async fn handle_stream(
        &mut self,
        dir: StreamDirection,
        tx: Option<quinn::SendStream>,
        mut rx: quinn::RecvStream,
    ) -> Result<()> {
        // Consume the hello message sent by the peer to allow us to detect
        // the stream creation
        let endpoint_id: u64 = u64::from_be(io_utils::read_msg(&mut rx).await?);

        let stream_id = stream_id_to_u64(rx.id());
        debug!("Accepted {dir:?} stream {stream_id}");

        // Find the endpoint that is linked with the opened stream.
        // It is possible that the endpoint creation notification hasn't been
        // received on the ControlChan yet.
        let mut state = self.state.lock().await;

        let Some(endpoint_builder) = state.endpoint_builders.get_mut(&endpoint_id) else {
            warn!("Peer quic stream opened but no endpoint are associated for now");
            return Ok(());
        };

        let desc = endpoint_builder.desc();
        debug!(
            "Registered peer endpoint 0x{id:X} stream opened",
            id = desc.id
        );
        assert_eq!(desc.owner, StreamOwner::Peer);
        assert_eq!(desc.direction, dir);

        let pair = StreamPair { tx, rx: Some(rx) };
        endpoint_builder.set_quic_stream(pair)?;

        state.start_endpoint(endpoint_id).await?;

        Ok(())
    }

    pub(crate) fn new(conn: quinn::Connection, state: Arc<Mutex<State>>) -> Self {
        Self { conn, state }
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            let (dir, tx, rx) = tokio::select! {
                ret = self.conn.accept_uni() => {
                    match ret {
                        Ok(rx) => {
                            (StreamDirection::Uni, None, rx)
                        }
                        Err(err) => {
                            warn!("Failed to open Uni stream: {err:?}");
                            return Err(Error::StreamAcceptFailed { dir: StreamDirection::Uni });
                        }
                    }
                }
                ret = self.conn.accept_bi() => {
                    match ret {
                        Ok((tx, rx)) => {
                            (StreamDirection::Bi, Some(tx), rx)
                        }
                        Err(err) => {
                            warn!("Failed to open Bi stream: {err:?}");
                            return Err(Error::StreamAcceptFailed { dir: StreamDirection::Bi });
                        }
                    }
                }
            };

            self.handle_stream(dir, tx, rx).await?;
        }
    }
}
