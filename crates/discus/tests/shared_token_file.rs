//! A node reads the shared token from a file, and reads it again after the file changes.
#![allow(clippy::unwrap_used)]
mod security_support;

use std::path::Path;
use std::time::{Duration, Instant};

use security_support::{cmd, exchange, send, start, Node};

/// Wait until `ok` is true. The node checks the file every second, so 60 s is generous.
async fn eventually<F: std::future::Future<Output = bool>>(mut ok: impl FnMut() -> F) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ok().await {
        assert!(Instant::now() < deadline, "the node did not reload");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Replace the file with one rename, as Kubernetes does, so the node never reads half a token.
fn replace(path: &Path, content: &str) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, content).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

/// A node with the token file and a 1 s reload period. The source limit is high, because a test
/// sends wrong tokens while it waits for a reload.
async fn start_with(file: &Path) -> Node {
    start(&[
        "--shared-token-file",
        file.to_str().unwrap(),
        "--tls-reload-secs",
        "1",
        "--auth-max-failures-per-source",
        "1000000",
    ])
    .await
}

async fn accepts(node: &Node, token: &str) -> bool {
    send(node, &cmd(&["AUTH", token])).await == "+OK\r\n"
}

#[tokio::test]
async fn the_token_of_the_file_authenticates() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("token");
    std::fs::write(&file, "s3cret\n").unwrap();
    let node = start_with(&file).await;
    let denied = send(&node, &cmd(&["PING"])).await;
    assert!(denied.starts_with("-NOAUTH"), "{denied:?}");
    assert!(accepts(&node, "s3cret").await);
    let wrong = send(&node, &cmd(&["AUTH", "s3cret\n"])).await;
    assert!(wrong.starts_with("-WRONGPASS"), "{wrong:?}");
}

#[tokio::test]
async fn a_rewritten_file_changes_the_token() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("token");
    std::fs::write(&file, "old\n").unwrap();
    let node = start_with(&file).await;
    let mut open = security_support::common::client_tls::connect(node.addr).await;
    assert_eq!(exchange(&mut open, &cmd(&["AUTH", "old"])).await, "+OK\r\n");

    replace(&file, "new\n");
    eventually(|| accepts(&node, "new")).await;
    assert!(!accepts(&node, "old").await);
    assert_eq!(exchange(&mut open, &cmd(&["PING"])).await, "+PONG\r\n");
}

#[tokio::test]
async fn a_broken_rewrite_keeps_the_token() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("token");
    std::fs::write(&file, "old\n").unwrap();
    let node = start_with(&file).await;

    for empty in [true, false] {
        if empty {
            replace(&file, "\n");
        } else {
            std::fs::remove_file(&file).unwrap();
        }
        // Three reload periods. A slower node only checks the file later, and then the
        // assertions below still hold.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(accepts(&node, "old").await);
        assert!(!accepts(&node, "").await);
    }

    replace(&file, "new\n");
    eventually(|| accepts(&node, "new")).await;
}
