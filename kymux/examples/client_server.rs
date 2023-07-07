use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KYMUX_PORT: u16 = 9090;
const SERVER_NAME: &str = "kymux_example";

#[derive(Clone)]
struct TlsKey {
    cert_chain: Vec<rustls::Certificate>,
    private_key: rustls::PrivateKey,
    certs_store: rustls::RootCertStore,
}

fn gen_keys(server_name: &str) -> TlsKey {
    let names = vec![server_name.into()];

    // rcgen: Generate Cert + Key
    let (cert_der, private_key) = {
        let cert = rcgen::generate_simple_self_signed(names).unwrap();
        (
            cert.serialize_der().unwrap(),
            cert.serialize_private_key_der(),
        )
    };

    // rustls: Load Cert + key
    let cert = rustls::Certificate(cert_der);
    let private_key = rustls::PrivateKey(private_key);

    let mut certs_store = rustls::RootCertStore::empty();
    certs_store.add(&cert).unwrap();

    TlsKey {
        cert_chain: vec![cert],
        private_key,
        certs_store,
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .format_timestamp_millis()
        .init();

    let keys = gen_keys(SERVER_NAME);

    // Server
    let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), KYMUX_PORT);

    let server_config = kymux::ServerConfig::new(server_addr, keys.cert_chain, keys.private_key);

    let server_accept_task = async move {
        let listener = kymux::ConnectionListener::new(server_config).await.unwrap();
        let connecting = listener.accept().await.unwrap();
        connecting.complete_connection().await.unwrap()
    };

    // Client
    let client_addr = format!("127.0.0.1:{port}", port = KYMUX_PORT)
        .parse()
        .unwrap();

    let client_config = kymux::ClientConfig::new(client_addr, keys.certs_store, SERVER_NAME.into());

    let client = kymux::Connection::connect(client_config);

    // Connect
    let (server_ret, client_ret) = tokio::join!(server_accept_task, client);
    let mut server = server_ret;
    let client = client_ret.unwrap();

    println!("Connected");

    let endpoint = server
        .register_endpoint(None, kymux::StreamType::Input)
        .await
        .unwrap();
    println!("Endpoint {endpoint:X} registered");
    println!(
        "Server({}): {:?}",
        server.client_listening_addr(),
        server.endpoints().await.unwrap()
    );
    println!(
        "Client({}): {:?}",
        client.client_listening_addr(),
        client.endpoints().await.unwrap()
    );

    let client_addr_endpoint1 = client.get_uri_for_endpoint(endpoint).unwrap();
    let server_addr_endpoint1 = server.get_uri_for_endpoint(endpoint).unwrap();

    let endpoint2 = server
        .register_endpoint(None, kymux::StreamType::Input)
        .await
        .unwrap();

    let client_addr_endpoint2 = client.get_uri_for_endpoint(endpoint2).unwrap();
    let server_addr_endpoint2 = server.get_uri_for_endpoint(endpoint2).unwrap();

    // Create host client
    let a = tokio::task::spawn(async move {
        println!("Connect clients");

        let host_client = kymux_client::Client::connect(&server_addr_endpoint1);
        let client_client = kymux_client::Client::connect(&client_addr_endpoint1);

        let (host_client_ret, client_client_ret) = tokio::join!(host_client, client_client);
        let host_client = host_client_ret.unwrap();
        let client_client = client_client_ret.unwrap();

        println!("Clients ready");
        let (_host_rx, mut host_tx) = host_client.split();
        let (mut client_rx, _client_tx) = client_client.split();

        for offset in 0..100 {
            let payload = 0x0123456789u64 + offset;
            host_tx.write_all(&payload.to_ne_bytes()).await.unwrap();

            let mut b = [0; 8];
            client_rx.read_exact(&mut b).await.unwrap();
            let rx_payload = u64::from_ne_bytes(b);
            println!("Got payload 0x{rx_payload:X}");

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Create host client 2
    let b = tokio::task::spawn(async move {
        println!("Connect clients 2");

        let host_client2 = kymux_client::Client::connect(&server_addr_endpoint2);
        let client_client2 = kymux_client::Client::connect(&client_addr_endpoint2);

        let (host_client2_ret, client_client2_ret) = tokio::join!(host_client2, client_client2);
        let host_client2 = host_client2_ret.unwrap();
        let client_client2 = client_client2_ret.unwrap();

        println!("Clients ready");
        let (_host2_rx, mut host2_tx) = host_client2.split();
        let (mut client2_rx, _client2_tx) = client_client2.split();

        for offset in 0..100 {
            let payload = 0xABCDEF6789u64 + offset;
            host2_tx.write_all(&payload.to_ne_bytes()).await.unwrap();

            let mut b = [0; 8];
            client2_rx.read_exact(&mut b).await.unwrap();
            let rx_payload = u64::from_ne_bytes(b);
            println!("Got payload 0x{rx_payload:X}");

            tokio::time::sleep(Duration::from_millis(120)).await;
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Endpoint 3
    let endpoint3 = server
        .register_endpoint(None, kymux::StreamType::Input)
        .await
        .unwrap();

    let client_addr_endpoint3 = client.get_uri_for_endpoint(endpoint3).unwrap();
    let server_addr_endpoint3 = server.get_uri_for_endpoint(endpoint3).unwrap();

    // Create host client 3
    let c = tokio::task::spawn(async move {
        println!("Connect clients 3");

        let host_client = kymux_client::Client::connect(&server_addr_endpoint3);
        let client_client = kymux_client::Client::connect(&client_addr_endpoint3);

        let (host_client_ret, client_client_ret) = tokio::join!(host_client, client_client);
        let host_client2 = host_client_ret.unwrap();
        let client_client2 = client_client_ret.unwrap();

        println!("Clients ready");
        let (_host_rx, mut host_tx) = host_client2.split();
        let (mut client_rx, _client_tx) = client_client2.split();

        for offset in 0..100 {
            let payload = 0xFFFF0000u64 + offset;
            host_tx.write_all(&payload.to_ne_bytes()).await.unwrap();

            let mut b = [0; 8];
            client_rx.read_exact(&mut b).await.unwrap();
            let rx_payload = u64::from_ne_bytes(b);
            println!("Got payload 0x{rx_payload:X}");

            tokio::time::sleep(Duration::from_millis(130)).await;
        }
    });

    let _ = tokio::join!(a, b, c);
}
