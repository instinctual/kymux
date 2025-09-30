use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unimplemented function")]
    NotImplemented,
    #[error("Kyproto connection error: {source:?}")]
    KyprotoConnectionError {
        #[from]
        source: kyproto::error::ConnectionError,
    },
    #[error("Kyproto protocol error: {source:?}")]
    KyprotoProtocolError {
        #[from]
        source: kyproto::error::ProtocolError,
    },
    #[error("IO Error  {source:?}")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[cfg(feature = "backend-quinn")]
    #[error("Rustls error: {source:?}")]
    TlsError {
        #[from]
        source: rustls::Error,
    },
    #[cfg(feature = "backend-webtransport-js")]
    #[error("WebtransportJS failed to decode hexstring: {source:?}")]
    DecodeHexError {
        #[from]
        source: kyproto::DecodeHexError,
    },
    #[error("Endpoint creation has failed: {source:?}")]
    EndpointCreateFailed { source: std::io::Error },
    #[error("Thread has panicked")]
    ThreadPanicked,
    #[error("No port available for local IPC")]
    IpcNoPortAvailable,
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(_: std::sync::PoisonError<T>) -> Error {
        Error::ThreadPanicked
    }
}

pub type Result<T> = std::result::Result<T, Error>;
