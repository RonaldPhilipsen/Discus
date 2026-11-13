//! Configuration checks that make the node refuse to start (spec §14.2, §15.5).

mod common;

/// The start error of a node with the client TLS of a test node and `args`.
async fn bind_error(args: &[&str]) -> String {
    let tls = common::client_tls::server_args(args);
    let mut all: Vec<&str> = args.to_vec();
    all.extend(tls.iter().map(String::as_str));
    bind_error_without_tls(&all).await
}

async fn bind_error_without_tls(args: &[&str]) -> String {
    let mut argv = vec![
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
    ];
    argv.extend_from_slice(args);
    let config = discus::Config::parse_from(argv).expect("valid test arguments");
    match discus::Server::bind(config).await {
        Ok(_) => panic!("the node started with {args:?}"),
        Err(e) => {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
            e.to_string()
        }
    }
}

#[tokio::test]
async fn tls_alone_is_not_a_policy() {
    let e = bind_error(&[
        "--auth-policy",
        "any:tls,shared_token",
        "--shared-token",
        "x",
    ])
    .await;
    assert!(e.contains("tls is not an identity"), "{e}");
    let e = bind_error(&["--auth-policy", "all:tls"]).await;
    assert!(e.contains("tls is not an identity"), "{e}");
}

#[tokio::test]
async fn an_unknown_factor_is_named() {
    let e = bind_error(&["--auth-policy", "any:password"]).await;
    assert!(e.contains("password"), "{e}");
}

#[tokio::test]
async fn an_unused_shared_token_is_an_error() {
    let e = bind_error(&["--auth-policy", "none", "--shared-token", "x"]).await;
    assert!(e.contains("--shared-token"), "{e}");
}

#[tokio::test]
async fn a_shared_token_policy_needs_the_token() {
    let e = bind_error(&["--auth-policy", "any:shared_token"]).await;
    assert!(e.contains("--shared-token"), "{e}");
}

#[tokio::test]
async fn a_node_without_a_client_certificate_refuses_to_start() {
    let e = bind_error_without_tls(&["--auth-policy", "none"]).await;
    assert!(e.contains("--tls-cert"), "{e}");
    let e = bind_error_without_tls(&["--tls-mode", "lb", "--auth-policy", "none"]).await;
    assert!(e.contains("--tls-cert"), "{e}");
}

#[test]
fn there_is_no_plaintext_tls_mode() {
    assert!(discus::Config::parse_from(["discus", "--tls-mode", "off"]).is_err());
}

#[cfg(all(feature = "oauth2", not(feature = "oauth2-introspection")))]
#[tokio::test]
async fn introspection_names_the_missing_feature() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("auth.toml");
    std::fs::write(
        &file,
        "[[issuer]]\nissuer = \"https://idp.test\"\naudience = \"discus\"\nclient_id = \"d\"\n",
    )
    .unwrap();
    let e = bind_error(&[
        "--auth-policy",
        "any:oauth2",
        "--dev-insecure-oauth2",
        "--auth-config",
        file.to_str().unwrap(),
    ])
    .await;
    assert!(e.contains("\"oauth2-introspection\" feature"), "{e}");
}

#[cfg(not(all(feature = "oauth2", feature = "k8s")))]
#[tokio::test]
async fn the_kubernetes_section_names_the_missing_feature() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("auth.toml");
    std::fs::write(
        &file,
        "[kubernetes]\n[[kubernetes.account]]\nmatch = \"system:serviceaccount:a:b\"\nscopes = [\"discus.read\"]\n",
    )
    .unwrap();
    let e = bind_error(&[
        "--auth-policy",
        "any:oauth2",
        "--dev-insecure-oauth2",
        "--auth-config",
        file.to_str().unwrap(),
    ])
    .await;
    let named = if cfg!(feature = "oauth2") {
        "\"k8s\" feature"
    } else {
        "\"oauth2\" feature"
    };
    assert!(e.contains(named), "{e}");
}

#[cfg(not(feature = "oauth2"))]
#[tokio::test]
async fn issuers_name_the_missing_feature() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("auth.toml");
    std::fs::write(
        &file,
        "[[issuer]]\nissuer = \"https://idp.test\"\naudience = \"discus\"\n",
    )
    .unwrap();
    let e = bind_error(&["--auth-config", file.to_str().unwrap()]).await;
    assert!(e.contains("\"oauth2\" feature"), "{e}");
}

#[cfg(not(feature = "oauth2"))]
#[tokio::test]
async fn an_oauth2_policy_names_the_missing_feature() {
    let e = bind_error(&["--auth-policy", "any:oauth2", "--dev-insecure-oauth2"]).await;
    assert!(e.contains("\"oauth2\" feature"), "{e}");
}

#[cfg(not(feature = "k8s"))]
#[tokio::test]
async fn the_k8s_lease_names_the_missing_feature() {
    let mut config = discus::Config::parse_from([
        "discus",
        "--cluster-ca",
        "ca.pem",
        "--cluster-cert",
        "cert.pem",
        "--cluster-key",
        "key.pem",
        "--lease-provider",
        "k8s",
    ])
    .unwrap();
    let err = config.resolve_identity().unwrap_err();
    assert!(err.contains("k8s build feature"), "{err}");
}
