use crate::error::ConnectionError;

use std::path::Path;

use log::error;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Certificate(pub(crate) rustls::Certificate);

#[derive(Debug, Clone)]
pub struct PrivateKey(pub(crate) rustls::PrivateKey);

#[derive(Debug, Clone)]
pub struct RootCertStore(pub(crate) rustls::RootCertStore);

impl Certificate {
    pub fn new(certificate: Vec<u8>) -> Self {
        Self(rustls::Certificate(certificate))
    }

    pub async fn load_pem_file(cert_path: impl AsRef<Path>) -> Result<Self, LoadCertError> {
        let pem_cert = tokio::fs::read(&cert_path).await?;
        let certificate = match rustls_pemfile::read_one(&mut pem_cert.as_ref())? {
            Some(rustls_pemfile::Item::X509Certificate(cert)) => cert,
            _ => {
                error!(
                    "{} does not contain a valid certificate",
                    cert_path.as_ref().to_string_lossy()
                );
                return Err(LoadCertError::Cert(CertError(
                    "Invalid certificate".to_string(),
                )));
            }
        };

        Ok(Self(rustls::Certificate(certificate)))
    }
}

impl From<rustls::Certificate> for Certificate {
    fn from(certificate: rustls::Certificate) -> Self {
        Self(certificate)
    }
}

impl PrivateKey {
    pub fn new(private_key: Vec<u8>) -> Self {
        Self(rustls::PrivateKey(private_key))
    }

    pub async fn load_pem_file(key_path: impl AsRef<Path>) -> Result<Self, LoadCertError> {
        let pem_key = tokio::fs::read(&key_path).await?;
        let private_key = match rustls_pemfile::read_one(&mut pem_key.as_ref())? {
            Some(rustls_pemfile::Item::RSAKey(key)) => key,
            Some(rustls_pemfile::Item::PKCS8Key(key)) => key,
            Some(rustls_pemfile::Item::ECKey(key)) => key,
            _ => {
                error!(
                    "{} does not contain a valid key",
                    key_path.as_ref().to_string_lossy()
                );
                return Err(LoadCertError::Cert(CertError("Invalid key".to_string())));
            }
        };

        Ok(Self(rustls::PrivateKey(private_key)))
    }
}

impl From<rustls::PrivateKey> for PrivateKey {
    fn from(private_key: rustls::PrivateKey) -> Self {
        Self(private_key)
    }
}

impl RootCertStore {
    pub fn empty() -> Self {
        Self(rustls::RootCertStore::empty())
    }

    pub fn with_single_cert(der: &Certificate) -> Result<Self, CertError> {
        let mut certs = Self::empty();
        certs.add(der)?;
        Ok(certs)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn add(&mut self, der: &Certificate) -> Result<(), CertError> {
        self.0.add(&der.0)?;
        Ok(())
    }
}

impl From<rustls::RootCertStore> for RootCertStore {
    fn from(store: rustls::RootCertStore) -> Self {
        Self(store)
    }
}

#[derive(Debug, Error)]
#[error("certificate error: {0}")]
pub struct CertError(pub String);

#[derive(Debug, Error)]
pub enum LoadCertError {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Certificate error")]
    Cert(#[from] CertError),
}

impl From<rustls::Error> for CertError {
    fn from(value: rustls::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<rustls::Error> for ConnectionError {
    fn from(value: rustls::Error) -> Self {
        Self(value.to_string())
    }
}
