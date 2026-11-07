//! The admin port requires `discus.admin` or an admin certificate, never the shared token
//! (spec §14.1, §14.2).
#![allow(clippy::unwrap_used)]
mod security_support;

use security_support::{
    admin_status, start, tls_connect, try_start, write_file, CertFiles, TestCa,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A plain HTTP GET with an optional bearer token. Returns the status code and the body.
async fn get(node: &security_support::Node, path: &str, bearer: Option<&str>) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(node.admin).await.unwrap();
    raw_get(&mut s, path, bearer).await
}

async fn raw_get<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    s: &mut S,
    path: &str,
    bearer: Option<&str>,
) -> (u16, String) {
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!("GET {path} HTTP/1.1\r\nHost: discus\r\n{auth}Connection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out).await;
    let text = String::from_utf8_lossy(&out).into_owned();
    let code = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_string();
    (code, body)
}

#[tokio::test]
async fn admin_routes_are_closed_by_default() {
    let node = start(&[]).await;
    assert_eq!(get(&node, "/admin/security", None).await.0, 401);
    assert_eq!(admin_status(&node, "/healthz").await, 200);
    assert_eq!(admin_status(&node, "/readyz").await, 200);
}

#[tokio::test]
async fn the_shared_token_never_opens_the_admin_port() {
    let node = start(&["--shared-token", "s3cret"]).await;
    assert_eq!(get(&node, "/admin/security", Some("s3cret")).await.0, 401);
}

fn admin_tls_args<'a>(ca: &'a TestCa, server: &'a CertFiles, file: &'a str) -> Vec<&'a str> {
    vec![
        "--admin-tls",
        "--tls-cert",
        server.cert_str(),
        "--tls-key",
        server.key_str(),
        "--tls-client-ca",
        ca.ca_str(),
        "--auth-config",
        file,
    ]
}

const ADMIN_TOML: &str = "[admin]\ncert_identities = [\"spiffe://test/ops/*\"]\n";

async fn tls_get(
    node: &security_support::Node,
    ca: &TestCa,
    client: Option<&CertFiles>,
    path: &str,
) -> (u16, String) {
    let mut s = tls_connect(node.admin, ca, client).await.unwrap();
    raw_get(&mut s, path, None).await
}

#[tokio::test]
async fn an_admin_certificate_opens_the_admin_port() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let file = write_file(&ca, "auth.toml", ADMIN_TOML);
    let node = start(&admin_tls_args(&ca, &server, &file)).await;
    let ops = ca.client_cert("spiffe://test/ops/alice");
    let (code, body) = tls_get(&node, &ca, Some(&ops), "/admin/security").await;
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("\"tls_mode\":\"off\""), "{body}");
    assert!(body.contains("\"ready\":true"), "{body}");
    // The probes answer over TLS without a certificate.
    assert_eq!(tls_get(&node, &ca, None, "/healthz").await.0, 200);
}

#[tokio::test]
async fn a_non_admin_certificate_is_refused() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let file = write_file(&ca, "auth.toml", ADMIN_TOML);
    let node = start(&admin_tls_args(&ca, &server, &file)).await;
    let app = ca.client_cert("spiffe://test/app");
    assert_eq!(
        tls_get(&node, &ca, Some(&app), "/admin/security").await.0,
        401
    );
    assert_eq!(tls_get(&node, &ca, None, "/admin/security").await.0, 401);
}

#[tokio::test]
async fn admin_identities_need_admin_tls() {
    let ca = TestCa::new();
    let file = write_file(&ca, "auth.toml", ADMIN_TOML);
    let e = try_start(&["--auth-config", &file]).await.err().unwrap();
    assert!(e.contains("--admin-tls"), "{e}");
}

#[cfg(feature = "oauth2")]
mod tokens {
    use super::*;
    use security_support::idp::MockIdp;
    use security_support::wait_ready;

    async fn node(ca: &TestCa, idp: &MockIdp) -> security_support::Node {
        let file = write_file(ca, "auth.toml", &idp.toml(ca, ""));
        let node = start(&[
            "--auth-policy",
            "any:oauth2",
            "--dev-insecure-oauth2",
            "--auth-config",
            &file,
        ])
        .await;
        assert!(wait_ready(&node, 10).await);
        node
    }

    #[tokio::test]
    async fn a_token_with_discus_admin_opens_the_admin_port() {
        let ca = TestCa::new();
        let idp = MockIdp::start(&ca).await;
        let node = node(&ca, &idp).await;
        let (code, body) = get(&node, "/admin/security", Some(&idp.token("discus.admin"))).await;
        assert_eq!(code, 200, "{body}");
        assert!(body.contains(&idp.issuer), "{body}");
        assert!(body.contains("\"loaded\":true"), "{body}");
    }

    #[tokio::test]
    async fn a_token_without_discus_admin_is_refused() {
        let ca = TestCa::new();
        let idp = MockIdp::start(&ca).await;
        let node = node(&ca, &idp).await;
        let t = idp.token("discus.read discus.write");
        assert_eq!(get(&node, "/admin/security", Some(&t)).await.0, 401);
    }
}
