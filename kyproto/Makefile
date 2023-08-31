export RUSTFLAGS=--cfg=web_sys_unstable_apis

build-tokio:
	cargo build --features=tokio

build-js:
	cargo build --features=js --target=wasm32-unknown-unknown
