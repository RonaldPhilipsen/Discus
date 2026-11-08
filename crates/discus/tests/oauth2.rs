//! OAuth 2.0 bearer tokens on the RESP listener (spec §14.3, §16 Auth row).
#![cfg(feature = "oauth2")]
#![allow(clippy::unwrap_used)]
mod security_support;

use std::time::Duration;

use security_support::idp::{unix_now, MockIdp};
use security_support::{
    cmd, exchange, send, start, start_oauth2, status, tls_connect, try_start, wait_status,
    write_file, Node, TestCa,
};
use serde_json::json;

/// A plaintext node (with the development flag) that trusts `idps`.
async fn node(ca: &TestCa, idps: &[&MockIdp], extra_toml: &str, args: &[&str]) -> Node {
    let toml: String = idps.iter().map(|i| i.toml(ca, extra_toml)).collect();
    start_oauth2(&write_file(ca, "auth.toml", &toml), args).await
}

async fn auth(node: &Node, token: &str) -> String {
    send(node, &cmd(&["AUTH", token])).await
}

#[tokio::test]
async fn auth_with_a_bare_token_works() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &[&idp], "", &[]).await;
    let mut wire = cmd(&["AUTH", &idp.token("discus.write")]);
    wire.extend(cmd(&["SET", "k", "v"]));
    wire.extend(cmd(&["GET", "k"]));
    assert_eq!(send(&node, &wire).await, "+OK\r\n+OK\r\n$1\r\nv\r\n");
}

#[tokio::test]
async fn auth_oauth2_and_hello_auth_work() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &[&idp], "", &[]).await;
    let t = idp.token("discus.read");
    assert_eq!(send(&node, &cmd(&["AUTH", "OAUTH2", &t])).await, "+OK\r\n");
    let hello = send(&node, &cmd(&["HELLO", "3", "AUTH", "someone", &t])).await;
    assert!(hello.starts_with('%'), "{hello:?}");
}

#[tokio::test]
async fn an_invalid_token_gets_the_generic_error() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &[&idp], "", &[]).await;
    let mut c = idp.claims("discus.read");
    c["aud"] = json!("other");
    let wrong_aud = auth(&node, &idp.sign("k1", &c)).await;
    let garbage = auth(&node, "not-a-token").await;
    assert!(wrong_aud.starts_with("-WRONGPASS"), "{wrong_aud:?}");
    assert_eq!(wrong_aud, garbage);
    assert!(send(&node, &cmd(&["PING"])).await.starts_with("-NOAUTH"));
}

#[tokio::test]
async fn a_read_scope_cannot_write() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &[&idp], "", &[]).await;
    let mut wire = cmd(&["AUTH", &idp.token("discus.read")]);
    wire.extend(cmd(&["SET", "k", "v"]));
    wire.extend(cmd(&["GET", "k"]));
    let reply = send(&node, &wire).await;
    assert!(
        reply.starts_with("+OK\r\n-NOPERM this user has no permissions to run the 'set' command"),
        "{reply:?}"
    );
    assert!(reply.ends_with("$-1\r\n"), "{reply:?}");
}

#[tokio::test]
async fn expiry_mid_connection_gives_noauth_then_reauth_works() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &[&idp], "", &["--auth-clock-skew-secs", "0"]).await;
    let mut c = idp.claims("discus.read");
    c["exp"] = json!(unix_now() + 2);
    let short = idp.sign("k1", &c);
    let mut s = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    assert_eq!(exchange(&mut s, &cmd(&["AUTH", &short])).await, "+OK\r\n");
    assert_eq!(exchange(&mut s, &cmd(&["GET", "k"])).await, "$-1\r\n");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let r = exchange(&mut s, &cmd(&["GET", "k"])).await;
    assert!(r.starts_with("-NOAUTH"), "{r:?}");
    let fresh = idp.token("discus.read");
    assert_eq!(exchange(&mut s, &cmd(&["AUTH", &fresh])).await, "+OK\r\n");
    assert_eq!(exchange(&mut s, &cmd(&["GET", "k"])).await, "$-1\r\n");
}

#[tokio::test]
async fn an_unknown_kid_refreshes_the_jwks_once() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &[&idp], "", &[]).await;
    assert_eq!(idp.jwks_fetches(), 1);
    idp.add_key("k2", jsonwebtoken::Algorithm::RS256);
    let t2 = idp.sign("k2", &idp.claims("discus.read"));
    assert_eq!(auth(&node, &t2).await, "+OK\r\n");
    assert_eq!(idp.jwks_fetches(), 2);
    // A second unknown kid within jwks_min_refetch_secs does not fetch again.
    idp.add_key("k3", jsonwebtoken::Algorithm::ES256);
    let t3 = idp.sign("k3", &idp.claims("discus.read"));
    assert!(auth(&node, &t3).await.starts_with("-WRONGPASS"));
    assert_eq!(idp.jwks_fetches(), 2);
}

#[tokio::test]
async fn a_removed_key_revokes_open_connections_after_a_refresh() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    idp.add_key("k2", jsonwebtoken::Algorithm::ES256);
    let node = node(&ca, &[&idp], "jwks_refresh_secs = 1", &[]).await;
    let mut s = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    assert_eq!(
        exchange(&mut s, &cmd(&["AUTH", &idp.token("discus.read")])).await,
        "+OK\r\n"
    );
    idp.remove_key("k1");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let r = exchange(&mut s, &cmd(&["GET", "k"])).await;
    assert!(r.starts_with("-NOAUTH"), "{r:?}");
}

#[tokio::test]
async fn the_node_is_unready_until_the_jwks_loads() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    idp.set_down(true);
    let file = write_file(&ca, "auth.toml", &idp.toml(&ca, ""));
    let node = start(&[
        "--auth-policy",
        "any:oauth2",
        "--dev-insecure-oauth2",
        "--auth-config",
        &file,
    ])
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(status(node.admin, "/readyz").await, 503);
    assert_eq!(status(node.admin, "/startupz").await, 503);
    assert!(auth(&node, &idp.token("discus.read"))
        .await
        .starts_with("-WRONGPASS"));
    idp.set_down(false);
    assert!(wait_status(node.admin, "/readyz", 200, Duration::from_secs(10)).await);
    assert_eq!(auth(&node, &idp.token("discus.read")).await, "+OK\r\n");
}

#[tokio::test]
async fn cached_keys_survive_an_outage_until_they_are_stale() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let extra = "jwks_refresh_secs = 1\njwks_max_stale_secs = 2";
    let node = node(&ca, &[&idp], extra, &[]).await;
    idp.set_down(true);
    assert_eq!(auth(&node, &idp.token("discus.read")).await, "+OK\r\n");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(auth(&node, &idp.token("discus.read"))
        .await
        .starts_with("-WRONGPASS"));
}

#[tokio::test]
async fn two_issuers_each_check_their_own_tokens() {
    let ca = TestCa::new();
    let a = MockIdp::start(&ca).await;
    let b = MockIdp::start(&ca).await;
    let node = node(&ca, &[&a, &b], "", &[]).await;
    assert_eq!(auth(&node, &a.token("discus.read")).await, "+OK\r\n");
    assert_eq!(auth(&node, &b.token("discus.read")).await, "+OK\r\n");
    // Signed by A, claiming to be from B.
    let forged = a.sign("k1", &b.claims("discus.read"));
    assert!(auth(&node, &forged).await.starts_with("-WRONGPASS"));
}

#[tokio::test]
async fn all_mtls_oauth2_needs_both_factors() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let server = ca.server_cert();
    let client = ca.client_cert("spiffe://test/app");
    let file = write_file(&ca, "auth.toml", &idp.toml(&ca, ""));
    let node = start(&[
        "--tls-mode",
        "node",
        "--tls-cert",
        server.cert_str(),
        "--tls-key",
        server.key_str(),
        "--tls-client-ca",
        ca.ca_str(),
        "--auth-policy",
        "all:mtls,oauth2",
        "--auth-config",
        &file,
    ])
    .await;
    assert!(wait_status(node.admin, "/readyz", 200, Duration::from_secs(10)).await);
    let mut s = tls_connect(node.addr, &ca, Some(&client)).await.unwrap();
    assert!(exchange(&mut s, &cmd(&["PING"]))
        .await
        .starts_with("-NOAUTH"));
    let mut wire = cmd(&["AUTH", &idp.token("discus.read")]);
    wire.extend(cmd(&["PING"]));
    assert_eq!(exchange(&mut s, &wire).await, "+OK\r\n+PONG\r\n");
    // A token without a certificate is not enough.
    let mut anon = tls_connect(node.addr, &ca, None).await.unwrap();
    let mut wire = cmd(&["AUTH", &idp.token("discus.read")]);
    wire.extend(cmd(&["PING"]));
    let r = exchange(&mut anon, &wire).await;
    assert!(r.starts_with("+OK\r\n-NOAUTH"), "{r:?}");
}

#[tokio::test]
async fn oauth2_over_plaintext_is_refused_without_the_dev_flag() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let file = write_file(&ca, "auth.toml", &idp.toml(&ca, ""));
    let e = try_start(&["--auth-policy", "any:oauth2", "--auth-config", &file])
        .await
        .err()
        .unwrap();
    assert!(e.contains("--dev-insecure-oauth2"), "{e}");
}

#[tokio::test]
async fn issuers_without_oauth2_in_the_policy_are_refused() {
    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let file = write_file(&ca, "auth.toml", &idp.toml(&ca, ""));
    let e = try_start(&[
        "--auth-policy",
        "any:shared_token",
        "--shared-token",
        "x",
        "--auth-config",
        &file,
    ])
    .await
    .err()
    .unwrap();
    assert!(e.contains("oauth2"), "{e}");
}
