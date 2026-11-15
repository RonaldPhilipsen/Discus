//! A node reloads its client certificates after their files change.
#![allow(clippy::unwrap_used)]
mod security_support;

use std::path::Path;

use security_support::{cmd, eventually, exchange, start, tls_connect, CertFiles, TestCa};

fn copy(from: &Path, to: &Path) {
    std::fs::write(to, std::fs::read(from).unwrap()).unwrap();
}

/// A certificate and key at fixed paths, which a test overwrites.
fn fixed_paths(dir: &tempfile::TempDir, from: &CertFiles) -> (String, String) {
    let (cert, key) = (dir.path().join("tls.crt"), dir.path().join("tls.key"));
    copy(&from.cert, &cert);
    copy(&from.key, &key);
    let s = |p: &Path| p.to_str().unwrap().to_string();
    (s(&cert), s(&key))
}

#[tokio::test]
async fn the_client_listener_reloads_its_certificate() {
    let (old, new) = (TestCa::new(), TestCa::new());
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = fixed_paths(&dir, &old.server_cert());
    let node = start(&[
        "--tls-cert",
        &cert,
        "--tls-key",
        &key,
        "--tls-reload-secs",
        "1",
    ])
    .await;
    assert!(tls_connect(node.addr, &old, None).await.is_ok());
    assert!(tls_connect(node.addr, &new, None).await.is_err());

    let next = new.server_cert();
    copy(&next.key, Path::new(&key));
    copy(&next.cert, Path::new(&cert));
    eventually("the node did not load the new certificate", || async {
        tls_connect(node.addr, &new, None).await.is_ok()
    })
    .await;
    let mut s = tls_connect(node.addr, &new, None).await.unwrap();
    assert_eq!(exchange(&mut s, &cmd(&["PING"])).await, "+PONG\r\n");
}

#[tokio::test]
async fn the_client_listener_reloads_its_client_ca() {
    let (server_ca, old, new) = (TestCa::new(), TestCa::new(), TestCa::new());
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = fixed_paths(&dir, &server_ca.server_cert());
    let client_ca = dir.path().join("ca.crt");
    copy(&old.ca_path, &client_ca);
    let node = start(&[
        "--tls-cert",
        &cert,
        "--tls-key",
        &key,
        "--tls-client-ca",
        client_ca.to_str().unwrap(),
        "--tls-client-auth",
        "required",
        "--tls-reload-secs",
        "1",
    ])
    .await;
    let client = new.client_cert("spiffe://example.org/client");
    let ping = || async {
        match tls_connect(node.addr, &server_ca, Some(&client)).await {
            Ok(mut s) => exchange(&mut s, &cmd(&["PING"])).await == "+PONG\r\n",
            Err(_) => false,
        }
    };
    assert!(
        !ping().await,
        "a client certificate from the new CA worked too early"
    );

    copy(&new.ca_path, &client_ca);
    eventually("the node did not load the new certificate", ping).await;
}
