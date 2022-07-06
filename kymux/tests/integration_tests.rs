use std::mem::size_of;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use log::info;
use rand::Rng;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task;

use kymux::{StreamDirection, StreamType};

const SERVER_NAME: &str = "kymux_test";
const PORT: u16 = 10000;

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

async fn create_connection() -> (kymux::Connection, kymux::Connection) {
    let keys = gen_keys(SERVER_NAME);

    let server_accept = async move {
        let mut config = kymux::ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT),
            keys.cert_chain,
            keys.private_key,
        );
        config.client_listener_port(9090);

        let listener = kymux::ConnectionListener::new(config).await.unwrap();
        let quic_server = listener.accept().await.unwrap();
        quic_server.complete_connection().await
    };

    let client_connect = async move {
        let mut config = kymux::ClientConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), PORT),
            keys.certs_store,
            SERVER_NAME.into(),
        );
        config.client_listener_port(9091);

        kymux::Connection::connect(config).await
    };

    match tokio::try_join!(server_accept, client_connect) {
        Ok((server, client)) => (server, client),
        _ => {
            panic!("Connect task failed");
        }
    }
}

struct ProducerConfig {
    shutdown_tx: mpsc::UnboundedSender<()>,
    uri: String,
    block_size: usize,
    generate_size: usize,
    completion_rx: oneshot::Receiver<()>,
    write_delay: Option<Duration>,
}

struct ConsumerConfig {
    shutdown_tx: mpsc::UnboundedSender<()>,
    uri: String,
    block_size: usize,
    completion_tx: oneshot::Sender<()>,
    read_delay: Option<Duration>,
}

async fn run_producer(producer_config: ProducerConfig) {
    // Connect to kymux
    let stream = kymux_client::Client::connect(&producer_config.uri)
        .await
        .unwrap();
    let mut stream = stream.into_tcp_stream();

    // Prepare data to send
    let block_count = producer_config.generate_size / producer_config.block_size;
    assert!(producer_config.generate_size % producer_config.block_size == 0);

    let mut block = vec![0u8; producer_config.block_size];
    let mut hasher = Sha1::new();

    // Announce size
    stream
        .write_all(&producer_config.generate_size.to_ne_bytes())
        .await
        .unwrap();

    // Send payload
    for _ in 0..block_count {
        rand::thread_rng().fill(&mut block[..]);
        hasher.update(&block[..]);
        stream.write_all(&block[..]).await.unwrap();

        if let Some(delay) = producer_config.write_delay {
            tokio::time::sleep(delay).await;
        }
    }

    // Send hash (20 bytes)
    let hash = hasher.finalize().to_vec();
    stream.write_all(&hash[..]).await.unwrap();

    // Wait completion
    producer_config.completion_rx.await.unwrap();
    producer_config.shutdown_tx.send(()).unwrap();
}

async fn run_consumer(consumer_config: ConsumerConfig) {
    // Connect to kymux
    let stream = kymux_client::Client::connect(&consumer_config.uri)
        .await
        .unwrap();
    let mut stream = stream.into_tcp_stream();

    // Read size
    let mut raw_size = [0u8; size_of::<usize>()];
    stream.read_exact(&mut raw_size[..]).await.unwrap();
    let receive_size = usize::from_ne_bytes(raw_size);
    info!("Prepare to receive {receive_size} bytes");

    // Prepare reception
    let block_count = receive_size / consumer_config.block_size;
    assert!(receive_size % consumer_config.block_size == 0);

    let mut block = vec![0u8; consumer_config.block_size];
    let mut hasher = Sha1::new();

    // Read payload
    for _ in 0..block_count {
        stream.read_exact(&mut block[..]).await.unwrap();
        hasher.update(&block[..]);

        if let Some(delay) = consumer_config.read_delay {
            tokio::time::sleep(delay).await;
        }
    }

    // Read and compare hash (20 bytes)
    let hash = hasher.finalize().to_vec();

    let mut announced_hash = vec![0u8; 20];
    stream.read_exact(&mut announced_hash[..]).await.unwrap();
    assert_eq!(hash, announced_hash);

    // Send completion
    consumer_config.completion_tx.send(()).unwrap();
    consumer_config.shutdown_tx.send(()).unwrap();
}

enum Actor {
    Client,
    Server,
}

enum TestStep {
    RunTest {
        // Stream configuration
        endpoint_creator: Actor,
        producer: Actor,
        stream_type: StreamType,
        stream_dir: StreamDirection,
        // Producer configuration
        block_size: usize,
        generate_size: usize,
        write_delay: Option<Duration>,
        // Consumer configuration
        read_delay: Option<Duration>,
    },
    Wait {
        duration: Duration,
    },
}

#[tokio::test(flavor = "multi_thread")]
async fn stress_test() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_millis()
        .is_test(true)
        .init();

    let (mut server, mut client) = create_connection().await;
    info!("QUIC connection ready");

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    let mut tasks = vec![];

    // Run tests with multiple configurations
    let configs = [
        // Run a long test that must survive
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            stream_dir: StreamDirection::Bi,
            block_size: 512,
            generate_size: 1024 * 2000,
            write_delay: Some(Duration::from_millis(5)),
            read_delay: Some(Duration::from_millis(10)),
        },
        // Run multiple tests with various combination of (creator, producer)
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            stream_dir: StreamDirection::Bi,
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Client,
            stream_type: StreamType::Input,
            stream_dir: StreamDirection::Bi,
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Client,
            stream_type: StreamType::Input,
            stream_dir: StreamDirection::Bi,
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            stream_dir: StreamDirection::Bi,
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        // Wait and start tests with read/write delays
        TestStep::Wait {
            duration: Duration::from_secs(1),
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Client,
            stream_type: StreamType::Input,
            stream_dir: StreamDirection::Bi,
            block_size: 128,
            generate_size: 128 * 40,
            write_delay: Some(Duration::from_millis(50)),
            read_delay: None,
        },
        TestStep::Wait {
            duration: Duration::from_secs(1),
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            stream_dir: StreamDirection::Bi,
            block_size: 128,
            generate_size: 128 * 40,
            write_delay: None,
            read_delay: Some(Duration::from_millis(50)),
        },
    ];

    for config in configs {
        match config {
            TestStep::RunTest {
                endpoint_creator,
                producer,
                stream_type,
                stream_dir,
                block_size,
                generate_size,
                write_delay,
                read_delay,
            } => {
                // Create the endpoint
                let connection = match endpoint_creator {
                    Actor::Client => &mut client,
                    Actor::Server => &mut server,
                };

                let endpoint = connection
                    .register_endpoint(stream_type, stream_dir)
                    .await
                    .unwrap();

                // Map the producer and the consumer to the correct actors
                let (producer, consumer) = match producer {
                    Actor::Client => (&mut client, &mut server),
                    Actor::Server => (&mut server, &mut client),
                };

                let producer_uri = producer.get_uri_for_endpoint(endpoint).unwrap();
                let consumer_uri = consumer.get_uri_for_endpoint(endpoint).unwrap();

                // Create the configurations
                let (completion_tx, completion_rx) = oneshot::channel();

                let producer_config = ProducerConfig {
                    shutdown_tx: shutdown_tx.clone(),
                    uri: producer_uri,
                    block_size,
                    generate_size,
                    completion_rx,
                    write_delay,
                };

                let consumer_config = ConsumerConfig {
                    shutdown_tx: shutdown_tx.clone(),
                    uri: consumer_uri,
                    block_size,
                    completion_tx,
                    read_delay,
                };

                // Run tasks
                let producer_task = task::spawn(run_producer(producer_config));
                let consumer_task = task::spawn(run_consumer(consumer_config));

                tasks.push(producer_task);
                tasks.push(consumer_task);
            }
            TestStep::Wait { duration } => {
                tokio::time::sleep(duration).await;
            }
        }
    }

    // Run a batch of concurrent tests
    for i in 0..50 {
        // Create the endpoint
        let (connection, stream_type, stream_dir) = if i % 2 == 0 {
            (&mut client, StreamType::AudioVideo, StreamDirection::Uni)
        } else {
            (&mut server, StreamType::Input, StreamDirection::Bi)
        };

        let endpoint = connection
            .register_endpoint(stream_type, stream_dir)
            .await
            .unwrap();

        // Map the producer and the consumer to the correct actors
        let (producer, consumer) = if i % 2 == 0 {
            (&mut client, &mut server)
        } else {
            (&mut server, &mut client)
        };

        let producer_uri = producer.get_uri_for_endpoint(endpoint).unwrap();
        let consumer_uri = consumer.get_uri_for_endpoint(endpoint).unwrap();

        // Create the configurations
        let (completion_tx, completion_rx) = oneshot::channel();

        let block_size = 1024;
        let generate_size = 1024 * 1024 * 10;

        let producer_config = ProducerConfig {
            shutdown_tx: shutdown_tx.clone(),
            uri: producer_uri,
            block_size,
            generate_size,
            completion_rx,
            write_delay: None,
        };

        let consumer_config = ConsumerConfig {
            shutdown_tx: shutdown_tx.clone(),
            uri: consumer_uri,
            block_size,
            completion_tx,
            read_delay: None,
        };

        // Run tasks
        let producer_task = task::spawn(run_producer(producer_config));
        let consumer_task = task::spawn(run_consumer(consumer_config));

        tasks.push(producer_task);
        tasks.push(consumer_task);
    }

    // Wait for all shutdown sender to be dropped.
    // It means that all tasks are finished.
    drop(shutdown_tx);

    let mut task_count = tasks.len();
    info!("Started {task_count} tasks");

    while let Some(_) = shutdown_rx.recv().await {
        task_count -= 1;
        info!("Task stopped. Remaining: {task_count}");
    }

    // All tasks can be joined now
    for t in tasks {
        t.await.unwrap();
    }
}
