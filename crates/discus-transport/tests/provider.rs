#![allow(clippy::unwrap_used)]

use discus_transport::testing::{TestCa, TEST_SAN};
use discus_transport::{crypto_provider, ALPN_RPC, PROVIDER_NAME};

#[test]
fn the_provider_matches_the_feature() {
    #[cfg(feature = "tls-aws-lc")]
    assert_eq!(PROVIDER_NAME, "aws-lc-rs");
    #[cfg(all(feature = "tls-ring", not(feature = "tls-aws-lc")))]
    assert_eq!(PROVIDER_NAME, "ring");
    #[cfg(all(
        feature = "tls-openssl",
        not(feature = "tls-aws-lc"),
        not(feature = "tls-ring")
    ))]
    assert_eq!(PROVIDER_NAME, "openssl");
    let provider = crypto_provider();
    assert!(provider
        .cipher_suites
        .iter()
        .any(|s| s.suite() == rustls::CipherSuite::TLS13_AES_128_GCM_SHA256));
}

#[test]
fn server_config_is_tls13_only_without_early_data() {
    let ca = TestCa::new();
    let material = ca.material(TEST_SAN);
    let server = material.server_config(&[ALPN_RPC]);
    assert_eq!(server.max_early_data_size, 0);
    assert_eq!(server.alpn_protocols, vec![ALPN_RPC.to_vec()]);
    let client = material.client_config(ALPN_RPC);
    assert!(!client.enable_early_data);
    for suite in &server.crypto_provider().cipher_suites {
        assert!(
            suite.tls13().is_some(),
            "{:?} is not TLS 1.3",
            suite.suite()
        );
    }
}
