//! The cryptography provider, selected by the TLS backend feature (spec section 15.5).

use std::sync::{Arc, OnceLock};

use rustls::crypto::CryptoProvider;

#[cfg(not(any(feature = "tls-aws-lc", feature = "tls-ring", feature = "tls-openssl")))]
compile_error!("enable exactly one TLS backend feature: tls-aws-lc, tls-ring, or tls-openssl");

/// The name of the provider in this build.
#[cfg(feature = "tls-aws-lc")]
pub const PROVIDER_NAME: &str = "aws-lc-rs";
#[cfg(all(feature = "tls-ring", not(feature = "tls-aws-lc")))]
pub const PROVIDER_NAME: &str = "ring";
#[cfg(all(
    feature = "tls-openssl",
    not(feature = "tls-aws-lc"),
    not(feature = "tls-ring")
))]
pub const PROVIDER_NAME: &str = "openssl";

fn build() -> CryptoProvider {
    #[cfg(feature = "tls-aws-lc")]
    return rustls::crypto::aws_lc_rs::default_provider();
    #[cfg(all(feature = "tls-ring", not(feature = "tls-aws-lc")))]
    return rustls::crypto::ring::default_provider();
    #[cfg(all(
        feature = "tls-openssl",
        not(feature = "tls-aws-lc"),
        not(feature = "tls-ring")
    ))]
    return rustls_openssl::default_provider();
}

/// The provider of this build, with every cipher suite it offers. The client listener can use it
/// too. Every rustls config takes a provider explicitly, so the process default never matters.
pub fn crypto_provider() -> Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    PROVIDER.get_or_init(|| Arc::new(build())).clone()
}

/// The provider for QUIC: the same provider with only its TLS 1.3 cipher suites.
pub(crate) fn quic_provider() -> Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| {
            let mut p = build();
            p.cipher_suites.retain(|s| s.tls13().is_some());
            Arc::new(p)
        })
        .clone()
}
