//! Kubernetes service account tokens (spec §14.4, §16 Auth row).
#![cfg(all(feature = "k8s", feature = "oauth2"))]
#![allow(clippy::unwrap_used)]
mod security_support;

use security_support::idp::{unix_now, MockIdp};
use security_support::{cmd, exchange, start, try_start, wait_status, write_file, Node, TestCa};
use serde_json::{json, Value};
use std::time::Duration;

/// The `iss` that the mock API server reports, as a real cluster does.
const CLUSTER_ISS: &str = "https://kubernetes.default.svc.cluster.local";

const ACCOUNTS: &str = r#"
[[kubernetes.account]]
match = "system:serviceaccount:web:frontend"
scopes = ["discus.read", "discus.write"]

[[kubernetes.account]]
match = "system:serviceaccount:jobs:*"
scopes = ["discus.read"]
"#;

fn k8s_toml(ca: &TestCa, api: &MockIdp, token_file: &str) -> String {
    format!(
        "[kubernetes]\napi_server = \"{}\"\nca_file = \"{}\"\ntoken_file = \"{token_file}\"\n{ACCOUNTS}",
        api.issuer,
        ca.ca_str()
    )
}

async fn node(ca: &TestCa, api: &MockIdp, extra_toml: &str) -> Node {
    api.set_issuer_name(CLUSTER_ISS);
    let token_file = write_file(ca, "node-token", "node-sa-token\n");
    let toml = format!("{extra_toml}\n{}", k8s_toml(ca, api, &token_file));
    let file = write_file(ca, "auth.toml", &toml);
    let node = start(&[
        "--auth-policy",
        "any:oauth2",
        "--dev-insecure-oauth2",
        "--auth-config",
        &file,
    ])
    .await;
    assert!(
        wait_status(node.admin, "/readyz", 200, Duration::from_secs(10)).await,
        "the node never became ready"
    );
    node
}

/// Claims of a projected service account token.
fn sa(sub: &str) -> Value {
    let (ns, name) = {
        let parts: Vec<&str> = sub.split(':').collect();
        (parts[2].to_string(), parts[3].to_string())
    };
    json!({
        "iss": CLUSTER_ISS,
        "aud": ["discus"],
        "sub": sub,
        "iat": unix_now(),
        "nbf": unix_now(),
        "exp": unix_now() + 3600,
        "kubernetes.io": {"namespace": ns, "serviceaccount": {"name": name}},
    })
}

async fn send(node: &Node, wire: &[u8]) -> String {
    let mut s = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    exchange(&mut s, wire).await
}

#[tokio::test]
async fn an_exact_account_rule_grants_its_scopes() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let node = node(&ca, &api, "").await;
    let t = api.sign("k1", &sa("system:serviceaccount:web:frontend"));
    let mut wire = cmd(&["AUTH", &t]);
    wire.extend(cmd(&["SET", "k", "v"]));
    assert_eq!(send(&node, &wire).await, "+OK\r\n+OK\r\n");
}

#[tokio::test]
async fn a_namespace_wildcard_matches_every_account_in_it() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let node = node(&ca, &api, "").await;
    for name in ["nightly", "backfill"] {
        let t = api.sign("k1", &sa(&format!("system:serviceaccount:jobs:{name}")));
        let mut wire = cmd(&["AUTH", &t]);
        wire.extend(cmd(&["GET", "k"]));
        wire.extend(cmd(&["SET", "k", "v"]));
        let r = send(&node, &wire).await;
        assert!(r.starts_with("+OK\r\n$-1\r\n-NOPERM"), "{r:?}");
    }
}

#[tokio::test]
async fn an_unmatched_account_is_rejected() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let node = node(&ca, &api, "").await;
    for sub in [
        "system:serviceaccount:web:backend",
        "system:serviceaccount:jobsx:nightly",
    ] {
        let t = api.sign("k1", &sa(sub));
        let r = send(&node, &cmd(&["AUTH", &t])).await;
        assert!(r.starts_with("-WRONGPASS"), "{sub}: {r:?}");
    }
}

#[tokio::test]
async fn the_wrong_audience_is_rejected() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let node = node(&ca, &api, "").await;
    let mut c = sa("system:serviceaccount:web:frontend");
    c["aud"] = json!(["https://kubernetes.default.svc"]);
    let r = send(&node, &cmd(&["AUTH", &api.sign("k1", &c)])).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
}

#[tokio::test]
async fn a_legacy_secret_token_is_rejected() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let node = node(&ca, &api, "").await;
    let legacy = json!({
        "iss": "kubernetes/serviceaccount",
        "sub": "system:serviceaccount:web:frontend",
        "kubernetes.io/serviceaccount/namespace": "web",
    });
    let r = send(&node, &cmd(&["AUTH", &api.sign("k1", &legacy)])).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
    // The same shape with the cluster issuer still lacks exp and aud.
    let mut no_exp = legacy.clone();
    no_exp["iss"] = json!(CLUSTER_ISS);
    let r = send(&node, &cmd(&["AUTH", &api.sign("k1", &no_exp)])).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
}

#[tokio::test]
async fn a_rotated_token_works_on_a_new_connection() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let node = node(&ca, &api, "").await;
    let first = api.sign("k1", &sa("system:serviceaccount:web:frontend"));
    assert_eq!(send(&node, &cmd(&["AUTH", &first])).await, "+OK\r\n");
    // The cluster rotates its signing key, and the kubelet writes a new token.
    api.add_key("k2", jsonwebtoken::Algorithm::RS256);
    let mut c = sa("system:serviceaccount:web:frontend");
    c["exp"] = json!(unix_now() + 7200);
    let second = api.sign("k2", &c);
    assert_eq!(send(&node, &cmd(&["AUTH", &second])).await, "+OK\r\n");
}

#[tokio::test]
async fn the_cluster_issuer_and_a_second_issuer_work_together() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let idp = MockIdp::start(&ca).await;
    let node = node(&ca, &api, &idp.toml(&ca, "")).await;
    let t = api.sign("k1", &sa("system:serviceaccount:web:frontend"));
    assert_eq!(send(&node, &cmd(&["AUTH", &t])).await, "+OK\r\n");
    assert_eq!(
        send(&node, &cmd(&["AUTH", &idp.token("discus.read")])).await,
        "+OK\r\n"
    );
    // A cluster token signed by the other provider never passes.
    let forged = idp.sign("k1", &sa("system:serviceaccount:web:frontend"));
    assert!(send(&node, &cmd(&["AUTH", &forged]))
        .await
        .starts_with("-WRONGPASS"));
}

#[tokio::test]
async fn discovery_sends_the_node_service_account_token() {
    let ca = TestCa::new();
    let api = MockIdp::start(&ca).await;
    let _node = node(&ca, &api, "").await;
    let bearers = api.bearers();
    assert!(!bearers.is_empty());
    assert!(
        bearers.iter().all(|b| b == "Bearer node-sa-token"),
        "{bearers:?}"
    );
}

#[tokio::test]
async fn a_malformed_account_rule_is_refused() {
    let ca = TestCa::new();
    let toml = "[kubernetes]\nca_file = \"/x\"\n[[kubernetes.account]]\nmatch = \"system:serviceaccount:*\"\nscopes = [\"discus.read\"]\n";
    let file = write_file(&ca, "auth.toml", toml);
    let e = try_start(&[
        "--auth-policy",
        "any:oauth2",
        "--dev-insecure-oauth2",
        "--auth-config",
        &file,
    ])
    .await
    .err()
    .unwrap();
    assert!(e.contains("system:serviceaccount:<namespace>:"), "{e}");
}
