use thiserror::Error;

#[derive(Debug, Error, Clone)]
#[error("endpoint already registed: {endpoint_id:X}")]
pub struct EndpointAlreadyRegistered {
    pub endpoint_id: u16,
}

#[derive(Debug, Error, Clone)]
#[error("Protoocol error: {0}")]
pub struct ProtocolError(pub String);

#[derive(Debug, Error, Clone)]
#[error("protocol start error")]
pub struct ProtocolStartError;
