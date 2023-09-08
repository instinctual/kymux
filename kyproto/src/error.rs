use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("endpoint already registed: {endpoint_id:X}")]
pub struct EndpointAlreadyRegistered {
    pub endpoint_id: u16,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("protocol start error")]
pub struct ProtocolStartError;
