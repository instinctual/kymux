export RUSTFLAGS=--cfg=web_sys_unstable_apis

build-tokio-rt:
	cargo build --features=tokio-rt

build-js:
	cargo build --features=js --target=wasm32-unknown-unknown
