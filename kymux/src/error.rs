use std::io;

use thiserror::Error;

use crate::{EndpointDesc, StreamDirection};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Encode failed: {source:?}")]
    EncodeError {
        #[from]
        source: rmp_serde::encode::Error,
    },
    #[error("Decode failed: {source:?}")]
    DecodeError {
        #[from]
        source: rmp_serde::decode::Error,
    },
    #[error("IO error: {source:?}")]
    IOError {
        #[from]
        source: io::Error,
    },
    #[error("Rels error: {source:?}")]
    TlsError {
        #[from]
        source: rustls::Error,
    },
    #[error("Couldn't accept connection on {addr}: {source:?}")]
    TcpAcceptFailed { addr: String, source: io::Error },
    #[error("Couldn't fetch local TCP address: {source:?}")]
    TcpLocalAddrFetchFailed { source: io::Error },
    #[error("Failed to connect to endpoint {desc:?}")]
    StreamOpenFailed { desc: EndpointDesc },
    #[error("Failed to accept {dir:?} stream")]
    StreamAcceptFailed { dir: StreamDirection },
    #[error("Mpsc channel closed")]
    ChannelClosed,
    #[error("Endpoint already started")]
    EndpointAlreadyStarted,

    #[error("Invalid Control message")]
    InvalidControlMsg,

    #[error("Endpoint creation has failed: {source:?}")]
    EndpointCreateFailed { source: io::Error },
    #[error("Endpoint connect has failed")]
    EndpointConnectFailed,
    #[error("Endpoint connect has been rejecter by peer")]
    EndpointConnectRejected,
    #[error("Endpoint accept has failed")]
    EndpointAcceptFailed,
    #[error("Endpoint is stopped")]
    EndpointStopped,
    #[error("Endpoint stop has failed")]
    EndpointStopFailed,
    #[error("Control chan has failed to open")]
    EndpointCtrlChanOpenFailed,
    #[error("Fail to listen for local clients")]
    EndpointClientListenFailed,

    #[error("A fatal error has occured")]
    FatalError,
}

pub type Result<T> = std::result::Result<T, Error>;
