export RUSTFLAGS=--cfg=web_sys_unstable_apis

build-quinn:
	cargo build --features=kynet-quinn

build-webtransport-js:
	cargo build --features=kynet-webtransport-js --target=wasm32-unknown-unknown

build-wtransport:
	cargo build --features=kynet-wtransport
