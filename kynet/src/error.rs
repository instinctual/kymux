use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("connection error: {0}")]
pub struct ConnectionError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("send datagram error: {0}")]
pub struct SendDatagramError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("read error: {0}")]
pub struct ReadError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ReadExactError {
    #[error("read exact finished early ({0} bytes read)")]
    FinishedEarly(usize),
    #[error("read error")]
    ReadError(#[from] ReadError),
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("write error: {0}")]
pub struct WriteError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("unknown stream")]
pub struct UnknownStreamError;
