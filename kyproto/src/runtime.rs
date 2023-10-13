use std::future::Future;

#[cfg(not(target_family = "wasm"))]
pub(crate) use tokio::time::{Instant, sleep_until};
#[cfg(target_family = "wasm")]
pub(crate) use wasmtimer::{std::Instant, tokio::sleep_until};

#[cfg(all(feature = "tokio-rt", not(target_family = "wasm")))]
pub(crate) fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}
#[cfg(all(feature = "tokio-rt", target_family = "wasm"))]
compile_error!("Tokio runtime is not available for wasm");

#[cfg(all(feature = "js", target_family = "wasm"))]
pub(crate) fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}
#[cfg(all(feature = "js", not(target_family = "wasm")))]
compile_error!("JS runtime is only available for wasm");
