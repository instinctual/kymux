use std::sync::Arc;

use log::{debug, warn};
use tokio::sync::Mutex;

use crate::io_utils;
use crate::stream::stream_id_to_u64;
use crate::{Error, Result, State, StreamDirection, StreamOwner, StreamPair};

const STREAM_HELLO_MSG: u8 = 0;

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
        let hello: u8 = io_utils::read_msg(&mut rx).await?;
        if hello != STREAM_HELLO_MSG {
            debug!("Expected Hello. Kick client");
            return Ok(());
        }

        let stream_id = stream_id_to_u64(rx.id());
        debug!("Accepted {dir:?} stream {stream_id}");

        // Find the endpoint that is linked with the opened stream.
        // It is possible that the endpoint creation notification hasn't been
        // received on the ControlChan yet.
        let mut state = self.state.lock().await;

        let endpoint = state.endpoints.iter_mut().find_map(|(_, endpoint)| {
            // Only consider endpoints with an opened stream
            let Some(quic_id) = endpoint.stream_id() else {
                return None;
            };

            if quic_id == stream_id {
                Some(endpoint)
            } else {
                None
            }
        });

        let pair = StreamPair { tx, rx: Some(rx) };

        if let Some(endpoint) = endpoint {
            let desc = endpoint.desc();
            debug!(
                "Registered peer endpoint 0x{id:X} stream opened",
                id = desc.id
            );
            assert_eq!(desc.owner, StreamOwner::Peer);
            assert_eq!(desc.direction, dir);

            endpoint.set_quic_stream(pair).await?;
        } else {
            debug!("Peer quic stream opened but no endpoint are associated for now");
            state.pending_streams.insert(stream_id, pair);
        }

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
