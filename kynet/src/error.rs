use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ConnectionError {
    #[error("generic connection error: {msg}")]
    Generic { msg: String },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SendDatagramError {
    #[error("generic send datagram error: {msg}")]
    Generic { msg: String },
    #[error("datagram unsupported by peer")]
    Unsupported,
    #[error("datagram too large")]
    TooLarge,
    #[error("connection error")]
    ConnectionError(#[from] ConnectionError),
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ReadError {
    #[error("generic read error: {msg}")]
    Generic { msg: String },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ReadExactError {
    #[error("read exact finished early")]
    FinishedEarly,
    #[error("read error")]
    ReadError(#[from] ReadError),
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum WriteError {
    #[error("generic write error: {msg}")]
    Generic { msg: String },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("unknown stream")]
pub struct UnknownStreamError;
