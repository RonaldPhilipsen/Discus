//! Build identity and the Cargo features compiled into this binary (spec §15.5).

#[cfg(not(any(feature = "tls-aws-lc", feature = "tls-ring", feature = "tls-openssl")))]
compile_error!("enable exactly one TLS backend feature: tls-aws-lc, tls-ring, or tls-openssl");

#[cfg(any(
    all(feature = "tls-aws-lc", feature = "tls-ring"),
    all(feature = "tls-aws-lc", feature = "tls-openssl"),
    all(feature = "tls-ring", feature = "tls-openssl"),
))]
compile_error!("enable only one TLS backend feature: tls-aws-lc, tls-ring, or tls-openssl");

#[cfg(not(any(feature = "roots-system", feature = "roots-webpki")))]
compile_error!("enable exactly one trust root feature: roots-system or roots-webpki");

#[cfg(all(feature = "roots-system", feature = "roots-webpki"))]
compile_error!("enable only one trust root feature: roots-system or roots-webpki");

/// The build identifier: `DISCUS_BUILD` at compile time, or `dev`.
pub const BUILD_ID: &str = match option_env!("DISCUS_BUILD") {
    Some(id) => id,
    None => "dev",
};

/// The optional subsystem features that are on, in a fixed order.
pub const FEATURES: &[&str] = &[
    #[cfg(feature = "otel")]
    "otel",
    #[cfg(feature = "oauth2")]
    "oauth2",
    #[cfg(feature = "oauth2-introspection")]
    "oauth2-introspection",
    #[cfg(feature = "k8s")]
    "k8s",
    #[cfg(feature = "pyroscope")]
    "pyroscope",
];

/// The cryptography provider for QUIC and the client listener.
#[cfg(feature = "tls-aws-lc")]
pub const TLS_BACKEND: &str = "aws-lc-rs";
#[cfg(feature = "tls-ring")]
pub const TLS_BACKEND: &str = "ring";
#[cfg(feature = "tls-openssl")]
pub const TLS_BACKEND: &str = "openssl";

/// The trust roots for outbound HTTPS without a configured CA.
#[cfg(feature = "roots-system")]
pub const TRUST_ROOTS: &str = "system";
#[cfg(feature = "roots-webpki")]
pub const TRUST_ROOTS: &str = "webpki";

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(
        feature = "otel",
        feature = "oauth2",
        not(feature = "oauth2-introspection"),
        feature = "k8s",
        not(feature = "pyroscope"),
        feature = "tls-aws-lc",
        feature = "roots-system"
    ))]
    #[test]
    fn default_build_reports_the_default_features() {
        assert_eq!(FEATURES, ["otel", "oauth2", "k8s"]);
        assert_eq!(TLS_BACKEND, "aws-lc-rs");
        assert_eq!(TRUST_ROOTS, "system");
    }

    #[cfg(feature = "oauth2-introspection")]
    #[test]
    fn introspection_turns_on_oauth2() {
        assert!(FEATURES.contains(&"oauth2"));
    }

    #[test]
    fn features_are_unique_and_never_empty_names() {
        for (i, f) in FEATURES.iter().enumerate() {
            assert!(!f.is_empty());
            assert!(!FEATURES[i + 1..].contains(f));
        }
    }
}
