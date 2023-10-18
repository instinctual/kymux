use std::future::Future;

#[cfg(target_family = "wasm")]
pub use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
pub use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(not(target_family = "wasm"))]
pub use tokio::time::{sleep, sleep_until, timeout, timeout_at, Duration, Instant};
#[cfg(target_family = "wasm")]
pub use wasmtimer::{
    std::{Instant, SystemTime, UNIX_EPOCH},
    tokio::{sleep, sleep_until, timeout, timeout_at},
};

#[cfg(all(feature = "tokio-rt", not(target_family = "wasm")))]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}
#[cfg(all(feature = "tokio-rt", target_family = "wasm"))]
compile_error!("Tokio runtime is not available for wasm");

#[cfg(all(feature = "js", target_family = "wasm"))]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}
#[cfg(all(feature = "js", not(target_family = "wasm")))]
compile_error!("JS runtime is only available for wasm");
