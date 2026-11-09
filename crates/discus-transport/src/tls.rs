//! Certificates, the peer verifier, and the rustls configs (spec section 14.1).
//!
//! Every node presents a certificate from the configured CA with the configured URI SAN, for
//! example `spiffe://cluster.local/ns/<namespace>/sa/discus`. The certificate proves membership,
//! not the `NodeId`, so one certificate per Deployment is enough. It is used as both a server and
//! a client certificate, so it needs both the server and the client extended key usage. The TLS
//! server name is ignored, because one certificate serves every pod.

use std::path::Path;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, Error, RootCertStore, SignatureScheme};

use crate::provider::quic_provider;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid PEM: {0}")]
    Pem(String),
    #[error("no certificate in the certificate file")]
    NoCertificate,
    #[error("invalid CA certificate: {0}")]
    Ca(String),
    #[error("invalid TLS configuration: {0}")]
    Config(String),
}

/// Every certificate in PEM text. Text without one is an error.
pub fn pem_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Pem(e.to_string()))?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificate);
    }
    Ok(certs)
}

/// The first private key in PEM text.
pub fn pem_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TlsError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|e| TlsError::Pem(e.to_string()))
}

/// A root store with every certificate in PEM text.
pub fn pem_roots(pem: &[u8]) -> Result<RootCertStore, TlsError> {
    let mut roots = RootCertStore::empty();
    for cert in pem_certs(pem)? {
        roots.add(cert).map_err(|e| TlsError::Ca(e.to_string()))?;
    }
    Ok(roots)
}

/// The CA, the node certificate and key, and the SAN that every peer must carry.
pub struct TlsMaterial {
    roots: Arc<RootCertStore>,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    san: String,
}

impl Clone for TlsMaterial {
    fn clone(&self) -> TlsMaterial {
        TlsMaterial {
            roots: self.roots.clone(),
            chain: self.chain.clone(),
            key: self.key.clone_key(),
            san: self.san.clone(),
        }
    }
}

impl std::fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsMaterial")
            .field("san", &self.san)
            .field("chain", &self.chain.len())
            .finish_non_exhaustive()
    }
}

impl TlsMaterial {
    /// Build from PEM text: the CA bundle, the node certificate chain, and its private key.
    pub fn from_pem(
        ca: &[u8],
        cert_chain: &[u8],
        key: &[u8],
        san: &str,
    ) -> Result<TlsMaterial, TlsError> {
        let roots = pem_roots(ca).map_err(|e| match e {
            TlsError::NoCertificate => TlsError::Ca("no certificate in the CA file".into()),
            e => e,
        })?;
        Ok(TlsMaterial {
            roots: Arc::new(roots),
            chain: pem_certs(cert_chain)?,
            key: pem_key(key)?,
            san: san.to_owned(),
        })
    }

    pub fn from_files(
        ca: &Path,
        cert: &Path,
        key: &Path,
        san: &str,
    ) -> Result<TlsMaterial, TlsError> {
        let read = |p: &Path| {
            std::fs::read(p).map_err(|source| TlsError::Read {
                path: p.display().to_string(),
                source,
            })
        };
        TlsMaterial::from_pem(&read(ca)?, &read(cert)?, &read(key)?, san)
    }

    /// The SAN that every peer must carry.
    pub fn san(&self) -> &str {
        &self.san
    }

    pub(crate) fn verifier(&self) -> Arc<PeerVerifier> {
        let provider = quic_provider();
        // The inner verifier only fails to build without roots, and `from_pem` refuses that.
        let client =
            WebPkiClientVerifier::builder_with_provider(self.roots.clone(), provider.clone())
                .build()
                .ok();
        Arc::new(PeerVerifier {
            roots: self.roots.clone(),
            client,
            san: self.san.clone(),
            provider,
        })
    }

    /// The server half: TLS 1.3 only, a client certificate required, and no early data.
    pub fn server_config(&self, alpn: &[&[u8]]) -> rustls::ServerConfig {
        let mut config = rustls::ServerConfig::builder_with_provider(quic_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("the provider supports TLS 1.3")
            .with_client_cert_verifier(self.verifier())
            .with_single_cert(self.chain.clone(), self.key.clone_key())
            .expect("the node key matches its certificate");
        config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        config.max_early_data_size = 0;
        config
    }

    /// The client half: TLS 1.3 only, the node certificate as client certificate, and no early
    /// data.
    pub fn client_config(&self, alpn: &[u8]) -> rustls::ClientConfig {
        let mut config = rustls::ClientConfig::builder_with_provider(quic_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("the provider supports TLS 1.3")
            .dangerous()
            .with_custom_certificate_verifier(self.verifier())
            .with_client_auth_cert(self.chain.clone(), self.key.clone_key())
            .expect("the node key matches its certificate");
        config.alpn_protocols = vec![alpn.to_vec()];
        config.enable_early_data = false;
        config
    }

    /// Check the key against the certificate now, so a bad pair fails at startup with a message.
    pub fn check(&self) -> Result<(), TlsError> {
        rustls::ServerConfig::builder_with_provider(quic_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TlsError::Config(e.to_string()))?
            .with_no_client_auth()
            .with_single_cert(self.chain.clone(), self.key.clone_key())
            .map(|_| ())
            .map_err(|e| TlsError::Config(e.to_string()))
    }
}

/// Checks a peer certificate: the chain to the configured CA, then the URI SAN.
#[derive(Debug)]
pub(crate) struct PeerVerifier {
    roots: Arc<RootCertStore>,
    client: Option<Arc<dyn ClientCertVerifier>>,
    san: String,
    provider: Arc<CryptoProvider>,
}

fn uri_sans(der: &[u8]) -> Vec<String> {
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(der) else {
        return Vec::new();
    };
    let Ok(Some(ext)) = cert.subject_alternative_name() else {
        return Vec::new();
    };
    ext.value
        .general_names
        .iter()
        .filter_map(|n| match n {
            x509_parser::extensions::GeneralName::URI(u) => Some((*u).to_owned()),
            _ => None,
        })
        .collect()
}

impl PeerVerifier {
    /// An empty expected SAN turns this check off. Then every certificate from the CA is a member.
    fn check_san(&self, end_entity: &CertificateDer<'_>) -> Result<(), Error> {
        if self.san.is_empty() || uri_sans(end_entity).iter().any(|s| s == &self.san) {
            Ok(())
        } else {
            Err(Error::General(format!(
                "the peer certificate does not carry the URI SAN {}",
                self.san
            )))
        }
    }
}

impl ServerCertVerifier for PeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let parsed = ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &parsed,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        self.check_san(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl ClientCertVerifier for PeerVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        let inner = self
            .client
            .as_ref()
            .ok_or_else(|| Error::General("no CA for client certificates".into()))?;
        inner.verify_client_cert(end_entity, intermediates, now)?;
        self.check_san(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}
