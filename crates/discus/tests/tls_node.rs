//! The `node` TLS mode with optional client mTLS (spec §14.1, §14.2).
mod security_support;

use security_support::{cmd, exchange, start, tls_connect, TestCa};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn tls_args<'a>(ca: &'a TestCa, server: &'a security_support::CertFiles) -> Vec<&'a str> {
    vec![
        "--tls-mode",
        "node",
        "--tls-cert",
        server.cert_str(),
        "--tls-key",
        server.key_str(),
        "--tls-client-ca",
        ca.ca_str(),
    ]
}

#[tokio::test]
async fn tls_plus_shared_token_needs_auth_over_tls() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let mut args = tls_args(&ca, &server);
    args.extend([
        "--auth-policy",
        "all:tls,shared_token",
        "--shared-token",
        "s3cret",
    ]);
    let node = start(&args).await;
    let mut s = tls_connect(node.addr, &ca, None).await.unwrap();
    assert!(exchange(&mut s, &cmd(&["PING"]))
        .await
        .starts_with("-NOAUTH"));
    let mut wire = cmd(&["AUTH", "s3cret"]);
    wire.extend(cmd(&["PING"]));
    assert_eq!(exchange(&mut s, &wire).await, "+OK\r\n+PONG\r\n");
}

#[tokio::test]
async fn the_default_policy_needs_tls_and_no_credentials() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let node = start(&tls_args(&ca, &server)).await;
    let mut s = tls_connect(node.addr, &ca, None).await.unwrap();
    let mut wire = cmd(&["SET", "k", "v"]);
    wire.extend(cmd(&["GET", "k"]));
    assert_eq!(exchange(&mut s, &wire).await, "+OK\r\n$1\r\nv\r\n");
}

#[tokio::test]
async fn a_plaintext_client_on_a_tls_listener_is_closed() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let mut args = tls_args(&ca, &server);
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    let mut s = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    s.write_all(&cmd(&["PING"])).await.unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), s.read_to_end(&mut buf))
        .await
        .expect("the node closes the connection");
    assert!(!String::from_utf8_lossy(&buf).contains("PONG"));
}

#[tokio::test]
async fn mtls_alone_authenticates_with_read_write_by_default() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let client = ca.client_cert("spiffe://test/app");
    let mut args = tls_args(&ca, &server);
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    let mut s = tls_connect(node.addr, &ca, Some(&client)).await.unwrap();
    let mut wire = cmd(&["SET", "k", "v"]);
    wire.extend(cmd(&["GET", "k"]));
    assert_eq!(exchange(&mut s, &wire).await, "+OK\r\n$1\r\nv\r\n");
    // Without a certificate the same listener gives NOAUTH.
    let mut anon = tls_connect(node.addr, &ca, None).await.unwrap();
    assert!(exchange(&mut anon, &cmd(&["GET", "k"]))
        .await
        .starts_with("-NOAUTH"));
}

#[tokio::test]
async fn a_read_only_certificate_cannot_write() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let client = ca.client_cert("spiffe://test/ro-reporting");
    let file = ca.path("auth.toml");
    std::fs::write(
        &file,
        r#"
[[client_cert]]
identity = "spiffe://test/ro*"
scopes = ["discus.read"]
"#,
    )
    .unwrap();
    let mut args = tls_args(&ca, &server);
    args.extend([
        "--auth-policy",
        "any:mtls",
        "--auth-config",
        file.to_str().unwrap(),
    ]);
    let node = start(&args).await;
    let mut s = tls_connect(node.addr, &ca, Some(&client)).await.unwrap();
    let set = exchange(&mut s, &cmd(&["SET", "k", "v"])).await;
    assert!(set.starts_with("-NOPERM"), "{set:?}");
    assert_eq!(exchange(&mut s, &cmd(&["GET", "k"])).await, "$-1\r\n");
}

#[tokio::test]
async fn required_client_auth_rejects_a_client_without_a_certificate() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let mut args = tls_args(&ca, &server);
    args.extend(["--auth-policy", "any:mtls", "--tls-client-auth", "required"]);
    let node = start(&args).await;
    let result = match tls_connect(node.addr, &ca, None).await {
        Err(_) => String::new(),
        Ok(mut s) => exchange(&mut s, &cmd(&["PING"])).await,
    };
    assert_eq!(result, "");
}

#[tokio::test]
async fn a_certificate_from_another_ca_is_rejected() {
    let ca = TestCa::new();
    let other = TestCa::named("other ca");
    let server = ca.server_cert();
    let stranger = other.client_cert("spiffe://test/app");
    let mut args = tls_args(&ca, &server);
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    // The client trusts the node CA but presents a certificate from another CA.
    let config = rustls::ClientConfig::builder_with_provider(discus::security::tls::provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(ca.root_store())
        .with_client_auth_cert(
            stranger.chain.clone(),
            rustls_pki_types::PrivateKeyDer::Pkcs8(stranger.key_der.clone().into()),
        )
        .unwrap();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
    let result = match connector.connect(name, tcp).await {
        Err(_) => String::new(),
        Ok(mut s) => exchange(&mut s, &cmd(&["PING"])).await,
    };
    assert_eq!(result, "");
}
