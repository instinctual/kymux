use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use log::{error, info, LevelFilter};
use thiserror::Error;

use kymux::StreamType;

/**
 * Command line kymux tool (useful for debugging/testing)
 *
 * To run a server listening on QUIC (UDP) port 1234, accepting one client
 * which will produce a video stream:
 *
 *     kymux --listen 1234 video
 *
 * The kymux client must connect to this server:
 *
 *     kymux --connect <server_ip>:1234
 *
 * The clients of the kymux nodes must connect to TCP on port
 * KYMUX_LOCAL_CLIENTS_PORT (9090). It is currently hardcoded in lib.rs.
 */

#[derive(Error, Debug)]
enum KymuxError {
    #[error("Syntax error: {0}")]
    SyntaxError(String),
    #[error("libkymux error")]
    LibError(#[from] kymux::Error),
    #[error("Certificate error: {0}")]
    CertError(String),
    #[error("Certificate store error: {0}")]
    CertStoreError(#[from] webpki::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), KymuxError> {
    // example: KYMUX_LOG=info,kymux::protocol=debug
    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .parse_env("KYMUX_LOG")
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        Err(KymuxError::SyntaxError(
            "Missing --listen or --connect".to_string(),
        ))?;
    }
    match args[1].as_str() {
        "--listen" => {
            // kymux --listen <quic_port> <type> ...
            // e.g.: kymux --listen 8080 video audio
            let quic_listen_port = args[2].parse().expect("Incorrect QUIC listen port");
            let stream_types = args
                .iter()
                .skip(3)
                .map(|s| match s.as_str() {
                    "video" => Ok(StreamType::Video),
                    "audio" => Ok(StreamType::Audio),
                    s => Err(KymuxError::SyntaxError(format!(
                        "Unexpected stream type: {}",
                        s
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            server(quic_listen_port, stream_types).await?;
        }
        "--connect" => {
            // kymux --connect <quic_ip>:<port>
            // e.g.: kymux --connect ip:8080
            let server_addr = args[2].parse().expect("Incorrect address");
            client(server_addr).await?;
        }
        arg => {
            Err(KymuxError::SyntaxError(format!(
                "Unexpected '{}', expected --listen or --connect",
                arg
            )))?;
        }
    }
    Ok(())
}

async fn server(quic_listen_port: u16, stream_types: Vec<StreamType>) -> Result<(), KymuxError> {
    let certificate = read_cert("kybertest_cert.pem").await?;
    let private_key = read_private_key("kybertest_key.pem").await?;

    let cert_chain = vec![certificate];

    // Start connection listener
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), quic_listen_port);

    info!("kymux: Listening to {addr}");
    let config = kymux::ServerConfig::new(addr, cert_chain, private_key);

    // Kymux initialization over QUIC
    let listener = kymux::ConnectionListener::new(config).await?;
    let connecting = listener.accept().await?;
    let mut connection = connecting.complete_connection().await?;

    for stream_type in stream_types {
        let id = connection.register_endpoint(stream_type).await?;
        let uri = connection.get_uri_for_endpoint(id)?;
        info!("{:?}: {}", stream_type, uri);
    }

    connection.wait_idle().await;

    Ok(())
}

async fn client(server_addr: SocketAddr) -> Result<(), KymuxError> {
    let certificate = read_cert("kybertest_cert.pem").await?;

    let mut certs = rustls::RootCertStore::empty();
    certs.add(&certificate)?;

    let client_config = kymux::ClientConfig::new(server_addr, certs, "kybertest");
    info!(
        "Listening for clients on TCP port {}",
        client_config.client_listener_port
    );

    let connection = kymux::Connection::connect(client_config).await?;

    connection.wait_idle().await;

    Ok(())
}

async fn read_cert(path: &str) -> Result<rustls::Certificate, KymuxError> {
    let cert_path = get_resource_path(path);
    let pem_cert = tokio::fs::read(&cert_path).await?;
    let certificate = match rustls_pemfile::read_one(&mut pem_cert.as_ref())? {
        Some(rustls_pemfile::Item::X509Certificate(cert)) => cert,
        _ => {
            error!("{cert_path} doesn't contain a X509 Certificate");
            return Err(KymuxError::CertError("Invalid cert".to_string()));
        }
    };
    Ok(rustls::Certificate(certificate))
}

async fn read_private_key(path: &str) -> Result<rustls::PrivateKey, KymuxError> {
    let key_path = get_resource_path(path);
    let pem_key = tokio::fs::read(&key_path).await?;
    let private_key = match rustls_pemfile::read_one(&mut pem_key.as_ref())? {
        Some(rustls_pemfile::Item::RSAKey(key)) => key,
        Some(rustls_pemfile::Item::PKCS8Key(key)) => key,
        _ => {
            error!("{key_path} doesn't contain a PKCS1/PKCS8 Key");
            return Err(KymuxError::CertError("Invalid key".to_string()));
        }
    };
    Ok(rustls::PrivateKey(private_key))
}

pub fn get_resource_path(resource_name: &str) -> String {
    if Path::new(resource_name).is_relative() {
        if let Ok(mut path) = std::env::current_exe() {
            path.pop();
            path.push(resource_name);

            path.to_str().unwrap().into()
        } else {
            PathBuf::from(resource_name).to_str().unwrap().into()
        }
    } else {
        resource_name.to_string()
    }
}
