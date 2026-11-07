//! The crypto provider, trust roots, and `rustls` configuration.
//!
//! The TLS backend feature picks the provider (spec §15.5). Every `rustls` config in Discus is
//! built with `builder_with_provider(provider())`. `main` also installs the provider as the
//! process-wide default, because the OTLP exporter builds its own config.

use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// The crypto provider of the TLS backend feature.
pub fn provider() -> Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| {
            #[cfg(feature = "tls-aws-lc")]
            let p = rustls::crypto::aws_lc_rs::default_provider();
            #[cfg(feature = "tls-ring")]
            let p = rustls::crypto::ring::default_provider();
            #[cfg(feature = "tls-openssl")]
            let p = rustls_openssl::default_provider();
            Arc::new(p)
        })
        .clone()
}

/// Make the backend provider the process-wide `rustls` default, for libraries that build a TLS
/// config on their own. Later calls do nothing.
pub fn install_default() {
    let _ = CryptoProvider::install_default(provider().as_ref().clone());
}

/// The trust roots for outbound HTTPS without a configured CA.
pub fn trust_roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    #[cfg(feature = "roots-system")]
    {
        let found = rustls_native_certs::load_native_certs();
        for e in &found.errors {
            tracing::warn!(error = %e, "could not read a system trust root source");
        }
        let (added, skipped) = roots.add_parsable_certificates(found.certs);
        if skipped > 0 {
            tracing::warn!(
                added,
                skipped,
                "skipped system trust roots that do not parse"
            );
        }
    }
    #[cfg(feature = "roots-webpki")]
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    roots
}

fn invalid(path: &Path, what: &str, e: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{}: {what}: {e}", path.display()),
    )
}

/// Every certificate in a PEM file.
pub fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| invalid(path, "cannot read certificates", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(path, "bad certificate", e))?;
    if certs.is_empty() {
        return Err(invalid(
            path,
            "no certificate",
            "the file has no CERTIFICATE block",
        ));
    }
    Ok(certs)
}

/// The first private key in a PEM file.
pub fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| invalid(path, "cannot read the private key", e))
}

/// A root store with every certificate in a PEM file.
pub fn load_roots(path: &Path) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| invalid(path, "bad CA certificate", e))?;
    }
    Ok(roots)
}

/// A server config for a listener. With `client_ca`, the listener asks for a client
/// certificate and verifies it against that CA only. `require_client` refuses a client without
/// one.
pub fn server_config(
    cert: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    require_client: bool,
) -> io::Result<rustls::ServerConfig> {
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?;
    let builder = match client_ca {
        Some(ca) => {
            let roots = Arc::new(load_roots(ca)?);
            let verifier =
                rustls::server::WebPkiClientVerifier::builder_with_provider(roots, provider());
            let verifier = if require_client {
                verifier
            } else {
                verifier.allow_unauthenticated()
            };
            let verifier = verifier
                .build()
                .map_err(|e| invalid(ca, "bad client CA", e))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    builder
        .with_single_cert(load_certs(cert)?, load_key(key)?)
        .map_err(|e| invalid(key, "the key does not match the certificate", e))
}

/// A client config for outbound HTTPS. A CA file replaces the trust roots.
pub fn client_config(ca: Option<&Path>) -> io::Result<ClientConfig> {
    let roots = match ca {
        Some(path) => load_roots(path)?,
        None => trust_roots(),
    };
    Ok(ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_has_tls13_suites_and_signature_algorithms() {
        let p = provider();
        assert!(!p.cipher_suites.is_empty());
        assert!(!p.signature_verification_algorithms.all.is_empty());
    }

    #[cfg(feature = "roots-webpki")]
    #[test]
    fn webpki_roots_are_compiled_in() {
        assert!(trust_roots().len() > 50);
    }

    #[test]
    fn a_ca_file_replaces_the_trust_roots() {
        let ca = rcgen::CertificateParams::new(Vec::<String>::new())
            .unwrap()
            .self_signed(&rcgen::KeyPair::generate().unwrap())
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, ca.pem()).unwrap();
        assert_eq!(load_roots(&path).unwrap().len(), 1);
        assert!(client_config(Some(&path)).is_ok());
        assert!(client_config(None).is_ok());
    }
}
