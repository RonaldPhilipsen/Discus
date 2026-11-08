//! RFC 7662 introspection for opaque tokens (spec §14.3).
#![cfg(feature = "oauth2-introspection")]
#![allow(clippy::unwrap_used)]
mod security_support;

use std::time::Duration;

use security_support::idp::{unix_now, MockIdp};
use security_support::{cmd, exchange, start, wait_status, write_file, Node, TestCa};
use serde_json::json;

async fn node(ca: &TestCa, idp: &MockIdp, secret: &str, args: &[&str]) -> Node {
    let secret_file = write_file(ca, "secret", secret);
    let extra = format!("client_id = \"discus\"\nclient_secret_file = \"{secret_file}\"");
    let file = write_file(ca, "auth.toml", &idp.toml(ca, &extra));
    let mut argv = vec![
        "--auth-policy",
        "any:oauth2",
        "--dev-insecure-oauth2",
        "--auth-config",
        &file,
    ];
    argv.extend_from_slice(args);
    let node = start(&argv).await;
    assert!(wait_status(node.admin, "/readyz", 200, Duration::from_secs(10)).await);
    node
}

fn active(idp: &MockIdp, exp: i64) -> serde_json::Value {
    json!({
        "active": true,
        "iss": idp.issuer,
        "aud": "discus",
        "sub": "svc",
        "exp": exp,
        "scope": "discus.read",
    })
}

async fn send(node: &Node, wire: &[u8]) -> String {
    let mut s = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    exchange(&mut s, wire).await
}

#[tokio::test]
async fn an_active_opaque_token_authenticates() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    idp.add_opaque("opaque-1", active(&idp, unix_now() + 300));
    let node = node(&ca, &idp, "secret", &[]).await;
    let mut wire = cmd(&["AUTH", "opaque-1"]);
    wire.extend(cmd(&["GET", "k"]));
    wire.extend(cmd(&["SET", "k", "v"]));
    let r = send(&node, &wire).await;
    assert!(r.starts_with("+OK\r\n$-1\r\n-NOPERM"), "{r:?}");
}

#[tokio::test]
async fn an_inactive_token_gets_the_generic_error() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    idp.add_opaque("opaque-1", json!({"active": false}));
    let node = node(&ca, &idp, "secret", &[]).await;
    let r = send(&node, &cmd(&["AUTH", "opaque-1"])).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
    let unknown = send(&node, &cmd(&["AUTH", "opaque-2"])).await;
    assert_eq!(r, unknown);
}

#[tokio::test]
async fn the_cache_saves_calls_but_never_outlives_the_token() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    idp.add_opaque("opaque-1", active(&idp, unix_now() + 2));
    let node = node(&ca, &idp, "secret", &["--auth-clock-skew-secs", "0"]).await;
    let mut s = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    assert_eq!(
        exchange(&mut s, &cmd(&["AUTH", "opaque-1"])).await,
        "+OK\r\n"
    );
    assert_eq!(send(&node, &cmd(&["AUTH", "opaque-1"])).await, "+OK\r\n");
    assert_eq!(idp.introspect_calls(), 1);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let r = exchange(&mut s, &cmd(&["GET", "k"])).await;
    assert!(r.starts_with("-NOAUTH"), "{r:?}");
    let r = send(&node, &cmd(&["AUTH", "opaque-1"])).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
}

#[tokio::test]
async fn a_wrong_client_secret_fails_closed() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    idp.add_opaque("opaque-1", active(&idp, unix_now() + 300));
    let node = node(&ca, &idp, "wrong", &[]).await;
    let r = send(&node, &cmd(&["AUTH", "opaque-1"])).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
}

#[tokio::test]
async fn jwts_still_verify_locally() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &idp, "secret", &[]).await;
    let r = send(&node, &cmd(&["AUTH", &idp.token("discus.read")])).await;
    assert_eq!(r, "+OK\r\n");
    assert_eq!(idp.introspect_calls(), 0);
}
