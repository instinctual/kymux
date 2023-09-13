export RUSTFLAGS=--cfg=web_sys_unstable_apis

build-quinn:
	cargo build --features=quinn

build-webtransport-js:
	cargo build --features=webtransport-js --target=wasm32-unknown-unknown

build-wtransport:
	cargo build --features=wtransport
