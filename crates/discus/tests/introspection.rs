//! RFC 7662 introspection for opaque tokens (spec §14.3).
#![cfg(feature = "oauth2-introspection")]
#![allow(clippy::unwrap_used)]
mod security_support;

use std::time::Duration;

use security_support::idp::{unix_now, MockIdp};
use security_support::{cmd, exchange, send, start_oauth2, write_file, Node, TestCa, ACL_USERS};
use serde_json::json;

async fn node(ca: &TestCa, idp: &MockIdp, secret: &str, args: &[&str]) -> Node {
    let secret_file = write_file(ca, "secret", secret);
    let extra = format!("client_id = \"discus\"\nclient_secret_file = \"{secret_file}\"");
    let file = write_file(ca, "auth.toml", &idp.toml(ca, &extra));
    start_oauth2(&file, args).await
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
    let mut s = security_support::common::client_tls::connect(node.addr).await;
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

/// A node that introspects a token again after 1 s instead of 300 s, and a connection that holds
/// a grant from `opaque-1`.
async fn node_with_grant(
    ca: &TestCa,
    idp: &MockIdp,
) -> (Node, tokio_rustls::client::TlsStream<tokio::net::TcpStream>) {
    idp.add_opaque("opaque-1", active(idp, unix_now() + 300));
    let node = node(ca, idp, "secret", &["--test-introspection-cache-secs", "1"]).await;
    let mut s = security_support::common::client_tls::connect(node.addr).await;
    assert_eq!(
        exchange(&mut s, &cmd(&["AUTH", "opaque-1"])).await,
        "+OK\r\n"
    );
    assert_eq!(idp.introspect_calls(), 1);
    (node, s)
}

/// Longer than the 1 s cache time of `node_with_grant`, so the next command is past the recheck
/// time on any machine.
const PAST_RECHECK: Duration = Duration::from_secs(3);

#[tokio::test]
async fn a_revoked_token_loses_its_grant_at_the_recheck() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let (_node, mut s) = node_with_grant(&ca, &idp).await;
    idp.add_opaque("opaque-1", json!({"active": false}));
    tokio::time::sleep(PAST_RECHECK).await;
    let mut wire = cmd(&["GET", "k"]);
    wire.extend(cmd(&["GET", "k"]));
    let r = exchange(&mut s, &wire).await;
    assert_eq!(r, "-NOAUTH Authentication required.\r\n");
    assert_eq!(
        exchange(&mut s, &cmd(&["PING"])).await,
        "",
        "the node closed the connection"
    );
    assert_eq!(idp.introspect_calls(), 2);
}

#[tokio::test]
async fn an_active_token_keeps_its_grant_across_a_recheck() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let (_node, mut s) = node_with_grant(&ca, &idp).await;
    tokio::time::sleep(PAST_RECHECK).await;
    let mut wire = cmd(&["PING"]);
    wire.extend(cmd(&["GET", "k"]));
    wire.extend(cmd(&["SET", "k", "v"]));
    let r = exchange(&mut s, &wire).await;
    assert!(r.starts_with("+PONG\r\n$-1\r\n-NOPERM"), "{r:?}");
    assert!(idp.introspect_calls() >= 2);
}

#[tokio::test]
async fn an_unreachable_provider_drops_the_grant_at_the_recheck() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let (_node, mut s) = node_with_grant(&ca, &idp).await;
    idp.set_down(true);
    tokio::time::sleep(PAST_RECHECK).await;
    let r = exchange(&mut s, &cmd(&["GET", "k"])).await;
    assert!(r.starts_with("-NOAUTH"), "{r:?}");
    assert_eq!(
        exchange(&mut s, &cmd(&["PING"])).await,
        "",
        "the node closed the connection"
    );
}

#[tokio::test]
async fn a_refused_token_reaches_the_provider_once() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &idp, "secret", &[]).await;
    for _ in 0..3 {
        let r = send(&node, &cmd(&["AUTH", "opaque-bad"])).await;
        assert!(r.starts_with("-WRONGPASS"), "{r:?}");
    }
    assert_eq!(idp.introspect_calls(), 1);
}

#[tokio::test]
async fn an_oversize_token_never_reaches_the_provider() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let long = "x".repeat(16 * 1024 + 1);
    idp.add_opaque(&long, active(&idp, unix_now() + 300));
    let node = node(&ca, &idp, "secret", &[]).await;
    let r = send(&node, &cmd(&["AUTH", &long])).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
    assert_eq!(idp.introspect_calls(), 0);
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

/// A node with the policy `any:password,oauth2`, an issuer with introspection, and the users of
/// `acl`.
async fn mixed_node(ca: &TestCa, idp: &MockIdp, acl: &str) -> Node {
    let secret_file = write_file(ca, "secret", "secret");
    let extra = format!("client_id = \"discus\"\nclient_secret_file = \"{secret_file}\"");
    let file = write_file(ca, "auth.toml", &idp.toml(ca, &extra));
    let policy = "any:password,oauth2";
    security_support::start_users_oauth2(ca, &file, policy, acl, &["--dev-insecure-oauth2"]).await
}

#[tokio::test]
async fn a_password_never_reaches_the_provider() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    idp.add_opaque("opaque-1", active(&idp, unix_now() + 300));
    let acl = format!("{ACL_USERS}user default on >default-pass ~* +@all\n").replacen(
        "user default off\n",
        "",
        1,
    );
    let node = mixed_node(&ca, &idp, &acl).await;

    for (parts, reply) in [
        (&["AUTH", "reader", "read-pass"][..], "+OK"),
        (&["AUTH", "reader", "wrong"], "-WRONGPASS"),
        (&["AUTH", "gone", "gone-pass"], "-WRONGPASS"),
        (&["AUTH", "default-pass"], "+OK"),
        (&["AUTH", "default", "default-pass"], "+OK"),
        (&["HELLO", "3", "AUTH", "reader", "read-pass"], "%"),
        (&["HELLO", "3", "AUTH", "default", "default-pass"], "%"),
    ] {
        let r = send(&node, &cmd(parts)).await;
        assert!(r.starts_with(reply), "{parts:?}: {r:?}");
    }
    assert_eq!(idp.introspect_calls(), 0);

    let r = send(&node, &cmd(&["AUTH", "someone", "opaque-1"])).await;
    assert_eq!(r, "+OK\r\n");
    assert_eq!(idp.introspect_calls(), 1);
}
