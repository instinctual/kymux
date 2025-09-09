# KyNet

KyNet is a network transport API abstracting the services exposed by QUIC and
WebTransport:
 - unidirectional streams
 - bidirectional streams
 - datagrams

This allows clients to use the same code whatever the underlying network
protocol.

It includes 3 backends (drivers), exposed behind cargo features:
 - [quinn] to communicate over QUIC
 - [webtransport-js] to communicate over WebTransport from a browser (in WASM)
 - [wtransport] to communicate over WebTransport from a desktop

[quinn]: https://docs.rs/quinn/latest/quinn/
[webtransport-js]: https://developer.mozilla.org/en-US/docs/Web/API/WebTransport
[wtransport]: https://docs.rs/wtransport/0.1.7/wtransport/

These backends are exposed behind cargo features.


## Build

To build the project locally:

```bash
# quinn (non-wasm-only)
cargo build --features=kynet-quinn

# webtransport-js (wasm-only)
export RUSTFLAGS=--cfg=web_sys_unstable_apis
cargo build --features=kynet-webtransport-js --target=wasm32-unknown-unknown

# wtransport (non-wasm-only)
cargo build --features=kynet-wtransport
```

To use _kynet_ as a dependency (adapt the features), add to your `Cargo.toml`:

```toml
[dependencies]
kynet = { version = "0.1", path = "../kynet", features = ["kynet-quinn"] }
```

## Use

Due to the differences in the underlying APIs, _kynet_ does not handle connection
creation. Instead, a `kynet::Connection` must be created from a valid QUIC or
WebTransport connection.

```rust
let web_transport = web_sys::WebTransport::new(url)?;
// wait until it's ready
JsFuture::from(web_transport.ready()).await?;

let conn = kynet::Connection::from(web_transport);

let (send, recv) = conn.open_bi().await?;
send.write_all(b"Hello world").await?;
```

Similarly, it is possible to create a `kynet::Connection` from a
`quinn::Connection` or a `wtransport::Connection`:

```rust
// quinn
let quinn_conn: quinn::Connection = …;
let conn = kynet::Connection::from(quinn_conn);

// wtransport
let wtransport_connection: wtransport::Connection = …;
let conn = kynet::Connection::from(wtransport_conn);
```


## History

Kyber was initially using only QUIC via the quinn library. In order to add
support for WebTransport, both in the browser and on desktop, we needed an
abstraction which exposed streams and datagrams, common to QUIC and
WebTransport.

The API aims to be similar to that of quinn, because it is user-friendly and we
were already using it.
