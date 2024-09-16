use crate::error::ConnectionError;

use std::path::Path;

use log::error;
use rustls::pki_types;
use thiserror::Error;

#[derive(Debug)]
pub struct Certificate(pub(crate) pki_types::CertificateDer<'static>);

#[derive(Debug)]
pub struct PrivateKey(pub(crate) pki_types::PrivateKeyDer<'static>);

#[derive(Debug, Clone)]
pub struct RootCertStore(pub(crate) rustls::RootCertStore);

impl Certificate {
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

        Ok(Self(certificate))
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl Clone for Certificate {
    fn clone(&self) -> Self {
        Self(self.to_vec().into())
    }
}

impl From<pki_types::CertificateDer<'static>> for Certificate {
    fn from(certificate: pki_types::CertificateDer<'static>) -> Self {
        Self(certificate)
    }
}

impl From<Vec<u8>> for Certificate {
    fn from(vec: Vec<u8>) -> Self {
        let der: pki_types::CertificateDer<'static> = vec.into();
        der.into()
    }
}

impl PrivateKey {
    pub async fn load_pem_file(key_path: impl AsRef<Path>) -> Result<Self, LoadCertError> {
        let pem_key = tokio::fs::read(&key_path).await?;
        let private_key = match rustls_pemfile::read_one(&mut pem_key.as_ref())? {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => pki_types::PrivateKeyDer::Pkcs1(key),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => pki_types::PrivateKeyDer::Pkcs8(key),
            Some(rustls_pemfile::Item::Sec1Key(key)) => pki_types::PrivateKeyDer::Sec1(key),
            _ => {
                error!(
                    "{} does not contain a valid key",
                    key_path.as_ref().to_string_lossy()
                );
                return Err(LoadCertError::Cert(CertError("Invalid key".to_string())));
            }
        };

        Ok(Self(private_key))
    }
}

impl From<pki_types::PrivateKeyDer<'static>> for PrivateKey {
    fn from(private_key: pki_types::PrivateKeyDer<'static>) -> Self {
        Self(private_key)
    }
}

impl Clone for PrivateKey {
    fn clone(&self) -> Self {
        Self(self.0.clone_key())
    }
}

impl RootCertStore {
    pub fn empty() -> Self {
        Self(rustls::RootCertStore::empty())
    }

    pub fn with_single_cert(der: Certificate) -> Result<Self, CertError> {
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

    pub fn add(&mut self, der: Certificate) -> Result<(), CertError> {
        self.0.add(der.0)?;
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

impl From<CertError> for ConnectionError {
    fn from(value: CertError) -> Self {
        Self(value.0)
    }
}
