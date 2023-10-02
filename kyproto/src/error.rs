use crate::control::ControlError;
use crate::router::EndpointAlreadyRegistered;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Error, Clone)]
#[error("Protoocol error: {0}")]
pub struct ProtocolError(pub String);

impl From<oneshot::error::RecvError> for ProtocolError {
    fn from(err: oneshot::error::RecvError) -> Self {
        Self(err.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for ProtocolError {
    fn from(err: mpsc::error::SendError<T>) -> Self {
        Self(err.to_string())
    }
}

impl From<ControlError> for ProtocolError {
    fn from(err: ControlError) -> Self {
        Self(err.to_string())
    }
}

impl From<EndpointAlreadyRegistered> for ProtocolError {
    fn from(err: EndpointAlreadyRegistered) -> Self {
        Self(err.to_string())
    }
}

#[cfg(all(feature = "js", target_family = "wasm"))]
mod js_error {
    // For convenience, also provide the conversion from these errors to
    // JsValue (containing the error message). They could not yield the
    // original JsValue, which is not stored in the kyproto errors).
    //
    // This implementation could not be done by the client, since neither
    // JsValue nor kyproto errors are defined in the client crate.

    macro_rules! impl_from_jsvalue {
        ($t:ty) => {
            impl From<$t> for wasm_bindgen::JsValue {
                fn from(value: $t) -> Self {
                    Self::from(&value.to_string())
                }
            }
        };
    }

    impl_from_jsvalue!(crate::error::EndpointAlreadyRegistered);
    impl_from_jsvalue!(crate::error::ProtocolError);
}

#[cfg(all(feature = "js", target_family = "wasm"))]
pub use js_error::*;
