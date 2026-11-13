//! Client TLS for test nodes. Every client uses TLS, so each test node gets a certificate for
//! `localhost` and `127.0.0.1` from one test CA, and every test client trusts that CA.

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

struct Fixture {
    ca: String,
    cert: String,
    key: String,
    ca_pem: String,
    ca_der: CertificateDer<'static>,
    // Keeps the certificate files for the life of the test process.
    _dir: tempfile::TempDir,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.distinguished_name
            .push(DnType::CommonName, "discus client test ca");
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_cert = ca.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca, ca_key);
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap()
            .signed_by(&key, &issuer)
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name).display().to_string();
        std::fs::write(path("cert.pem"), cert.pem()).unwrap();
        std::fs::write(path("key.pem"), key.serialize_pem()).unwrap();
        std::fs::write(path("ca.pem"), ca_cert.pem()).unwrap();
        Fixture {
            ca: path("ca.pem"),
            cert: path("cert.pem"),
            key: path("key.pem"),
            ca_pem: ca_cert.pem(),
            ca_der: ca_cert.der().clone(),
            _dir: dir,
        }
    })
}

/// The flags that a test node needs for client TLS, unless `args` brings its own certificate.
pub fn server_args<S: AsRef<str>>(args: &[S]) -> Vec<String> {
    if args.iter().any(|a| a.as_ref() == "--tls-cert") {
        return Vec::new();
    }
    let f = fixture();
    vec![
        "--tls-cert".to_string(),
        f.cert.clone(),
        "--tls-key".to_string(),
        f.key.clone(),
    ]
}

/// The PEM file of the CA that signed the test node certificates, for external clients.
pub fn ca_file() -> &'static str {
    &fixture().ca
}

/// The certificates that a redis-rs client needs to trust the test nodes.
pub fn redis_certs() -> redis::TlsCertificates {
    redis::TlsCertificates {
        client_tls: None,
        root_cert: Some(fixture().ca_pem.clone().into_bytes()),
    }
}

/// A redis-rs client for the test node at `url`, a `rediss://` URL.
pub fn redis_client(url: &str) -> redis::Client {
    redis::Client::build_with_tls(url, redis_certs()).unwrap()
}

/// A rustls client config that trusts the test nodes.
pub fn client_config() -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(fixture().ca_der.clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(discus::security::tls::provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

/// A TLS connection to the test node at `addr`.
pub async fn connect(addr: SocketAddr) -> TlsStream<TcpStream> {
    try_connect(addr).await.unwrap()
}

/// A TLS connection to the test node at `addr`, or the error of the connect or the handshake.
pub async fn try_connect(addr: SocketAddr) -> std::io::Result<TlsStream<TcpStream>> {
    let tcp = TcpStream::connect(addr).await?;
    let _ = tcp.set_nodelay(true);
    let connector = tokio_rustls::TlsConnector::from(client_config());
    connector
        .connect(ServerName::IpAddress(addr.ip().into()), tcp)
        .await
}
