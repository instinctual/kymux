use thiserror::Error;

pub use kynet::error::ConnectionError;

#[derive(Debug, Error, Clone)]
#[error("Protocol error: {0}")]
pub struct ProtocolError(pub String);

impl ProtocolError {
    pub fn new<T: ToString>(value: T) -> Self {
        Self(value.to_string())
    }
}
