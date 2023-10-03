export RUSTFLAGS=--cfg=web_sys_unstable_apis

build-quinn:
	cargo build --features=tokio-rt,kynet-quinn

build-wtransport:
	cargo build --features=tokio-rt,kynet-wtransport

build-js:
	cargo build --features=js --target=wasm32-unknown-unknown
