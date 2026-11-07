//! Helpers for the security tests: a test CA, TLS clients, and a node starter.
#![allow(dead_code, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_util::sync::CancellationToken;

/// A certificate and key, on disk and in memory.
pub struct CertFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub chain: Vec<CertificateDer<'static>>,
    pub key_der: Vec<u8>,
}

impl CertFiles {
    pub fn cert_str(&self) -> &str {
        self.cert.to_str().unwrap()
    }
    pub fn key_str(&self) -> &str {
        self.key.to_str().unwrap()
    }
    fn private_key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone()))
    }
}

/// A CA made in the test, with its files in a temporary directory.
pub struct TestCa {
    pub dir: tempfile::TempDir,
    pub ca_path: PathBuf,
    issuer: Issuer<'static, KeyPair>,
    der: CertificateDer<'static>,
    next: std::sync::atomic::AtomicUsize,
}

impl TestCa {
    pub fn new() -> TestCa {
        Self::named("discus test ca")
    }

    pub fn named(name: &str) -> TestCa {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, cert.pem()).unwrap();
        TestCa {
            dir,
            ca_path,
            der: cert.der().clone(),
            issuer: Issuer::new(params, key),
            next: Default::default(),
        }
    }

    pub fn ca_str(&self) -> &str {
        self.ca_path.to_str().unwrap()
    }

    /// The path of a new file in the CA directory.
    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn issue(&self, params: CertificateParams) -> CertFiles {
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        let n = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cert_path = self.dir.path().join(format!("cert{n}.pem"));
        let key_path = self.dir.path().join(format!("key{n}.pem"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        CertFiles {
            cert: cert_path,
            key: key_path,
            chain: vec![cert.der().clone()],
            key_der: key.serialize_der(),
        }
    }

    /// A server certificate for `localhost` and `127.0.0.1`.
    pub fn server_cert(&self) -> CertFiles {
        let params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
        self.issue(params)
    }

    /// A client certificate with one URI SAN.
    pub fn client_cert(&self, uri: &str) -> CertFiles {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .subject_alt_names
            .push(SanType::URI(uri.try_into().unwrap()));
        self.issue(params)
    }

    /// A client certificate with only a subject CN.
    pub fn client_cert_cn(&self, cn: &str) -> CertFiles {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(DnType::CommonName, cn);
        self.issue(params)
    }

    pub fn root_store(&self) -> rustls::RootCertStore {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.der.clone()).unwrap();
        roots
    }

    /// A TLS client config that trusts this CA, with an optional client certificate.
    pub fn client_config(&self, client: Option<&CertFiles>) -> rustls::ClientConfig {
        let b = rustls::ClientConfig::builder_with_provider(discus::security::tls::provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(self.root_store());
        match client {
            Some(c) => b
                .with_client_auth_cert(c.chain.clone(), c.private_key())
                .unwrap(),
            None => b.with_no_client_auth(),
        }
    }
}

/// Run a TLS handshake over `tcp`, trusting `ca`.
pub async fn tls_over(
    tcp: TcpStream,
    ca: &TestCa,
    client: Option<&CertFiles>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let connector = tokio_rustls::TlsConnector::from(Arc::new(ca.client_config(client)));
    let name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(name, tcp).await?;
    // TLS 1.3 sends the server's verdict on the client certificate after the handshake, so
    // flush once to surface a rejection here.
    tls.flush().await?;
    Ok(tls)
}

/// Connect with TLS, trusting `ca`.
pub async fn tls_connect(
    addr: SocketAddr,
    ca: &TestCa,
    client: Option<&CertFiles>,
) -> std::io::Result<TlsStream<TcpStream>> {
    tls_over(TcpStream::connect(addr).await?, ca, client).await
}

/// Encode a command as a RESP array.
pub fn cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// Write `wire` and read until the stream goes quiet for 150 ms, closes, or fails. An error or
/// a close before any byte gives an empty string.
pub async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S, wire: &[u8]) -> String {
    if s.write_all(wire).await.is_err() || s.flush().await.is_err() {
        return String::new();
    }
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 16 * 1024];
    loop {
        let wait = if buf.is_empty() {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(150)
        };
        match tokio::time::timeout(wait, s.read(&mut chunk)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

pub struct Node {
    pub addr: SocketAddr,
    pub admin: SocketAddr,
    pub shutdown: CancellationToken,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn argv(args: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = [
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    v.extend(args.iter().map(|s| s.to_string()));
    v
}

/// Start a node, or return the reason it refused to start.
pub async fn try_start(args: &[&str]) -> Result<Node, String> {
    let config = discus::Config::parse_from(argv(args)).map_err(|e| e.to_string())?;
    let server = discus::Server::bind(config)
        .await
        .map_err(|e| e.to_string())?;
    let (addr, admin) = (server.local_addr(), server.admin_addr());
    let shutdown = CancellationToken::new();
    tokio::spawn(server.run(shutdown.clone()));
    Ok(Node {
        addr,
        admin,
        shutdown,
    })
}

pub async fn start(args: &[&str]) -> Node {
    try_start(args).await.unwrap()
}

#[cfg(feature = "oauth2")]
pub mod idp;

/// Write `content` to a new file in the CA directory and return its path.
pub fn write_file(ca: &TestCa, name: &str, content: &str) -> String {
    let path = ca.path(name);
    std::fs::write(&path, content).unwrap();
    path.to_str().unwrap().to_string()
}

/// The status code of a plain HTTP GET on the admin port.
pub async fn admin_status(node: &Node, path: &str) -> u16 {
    reqwest::get(format!("http://{}{path}", node.admin))
        .await
        .unwrap()
        .status()
        .as_u16()
}

/// Poll `/readyz` until it answers 200, for at most `secs` seconds.
pub async fn wait_ready(node: &Node, secs: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if admin_status(node, "/readyz").await == 200 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}
