//! A local test CA on `rcgen` and raw QUIC client helpers. Tests only: never enable the `testing`
//! feature in a release build.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};

use crate::TlsMaterial;

/// The SAN that test certificates carry.
pub const TEST_SAN: &str = "spiffe://cluster.local/ns/test/sa/discus";

/// A throwaway CA that issues node certificates with a URI SAN.
pub struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl Default for TestCa {
    fn default() -> TestCa {
        TestCa::new()
    }
}

impl TestCa {
    pub fn new() -> TestCa {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "discus test CA");
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        TestCa {
            pem: cert.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    pub fn ca_pem(&self) -> String {
        self.pem.clone()
    }

    /// Issue a node certificate with `san` as its only URI SAN. Returns the cert and key PEM.
    pub fn issue(&self, san: &str) -> (String, String) {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "discus node");
        params.subject_alt_names = vec![SanType::URI(san.try_into().unwrap())];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// Material with a certificate for `san` that also expects `san` from peers.
    pub fn material(&self, san: &str) -> TlsMaterial {
        self.material_for(san, san)
    }

    /// Material with a certificate for `cert_san` that expects `expected_san` from peers. An
    /// empty `expected_san` accepts any certificate from this CA.
    pub fn material_for(&self, cert_san: &str, expected_san: &str) -> TlsMaterial {
        let (cert, key) = self.issue(cert_san);
        TlsMaterial::from_pem(
            self.pem.as_bytes(),
            cert.as_bytes(),
            key.as_bytes(),
            expected_san,
        )
        .unwrap()
    }
}

/// A client config that checks the server like a node but presents no client certificate.
pub fn client_without_cert(material: &TlsMaterial, alpn: &[u8]) -> rustls::ClientConfig {
    let mut config = rustls::ClientConfig::builder_with_provider(crate::provider::quic_provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(material.verifier())
        .with_no_client_auth();
    config.alpn_protocols = vec![alpn.to_vec()];
    config
}

/// A raw QUIC client endpoint on `127.0.0.1:0` with `tls` as its default client config.
pub fn quic_client(tls: rustls::ClientConfig) -> quinn::Endpoint {
    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic)));
    endpoint
}
