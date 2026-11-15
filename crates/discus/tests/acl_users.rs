//! Users of the ACL file log in with their passwords, and their rules limit commands and keys.
#![allow(clippy::unwrap_used)]
mod security_support;

use std::path::Path;
use std::time::{Duration, Instant};

use security_support::common::client_tls::connect;
use security_support::{cmd, exchange, send, start, Node, ACL_USERS, NOPERM_KEY as KEY};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// A node with the ACL file in `dir`. The source limit is high, because the tests send wrong
/// passwords.
async fn start_with(dir: &tempfile::TempDir) -> Node {
    let file = dir.path().join("users.acl");
    std::fs::write(&file, ACL_USERS).unwrap();
    start(&[
        "--acl-file",
        file.to_str().unwrap(),
        "--tls-reload-secs",
        "1",
        "--auth-max-failures-per-source",
        "1000000",
    ])
    .await
}

async fn login(node: &Node, user: &str, password: &str) -> TlsStream<TcpStream> {
    let mut s = connect(node.addr).await;
    assert_eq!(
        exchange(&mut s, &cmd(&["AUTH", user, password])).await,
        "+OK\r\n",
        "AUTH {user}"
    );
    s
}

async fn run(s: &mut TlsStream<TcpStream>, parts: &[&str]) -> String {
    exchange(s, &cmd(parts)).await
}

fn no_perm(command: &str) -> String {
    format!("-NOPERM this user has no permissions to run the '{command}' command\r\n")
}

#[tokio::test]
async fn a_user_logs_in_with_its_password() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;
    let denied = send(&node, &cmd(&["PING"])).await;
    assert!(denied.starts_with("-NOAUTH"), "{denied:?}");
    login(&node, "reader", "read-pass").await;
    let hello = send(&node, &cmd(&["HELLO", "3", "AUTH", "reader", "read-pass"])).await;
    assert!(!hello.starts_with('-'), "{hello:?}");
    login(&node, "writer", "w1").await;
    login(&node, "writer", "w2").await;

    for wrong in [
        &["AUTH", "reader", "wrong"][..],
        &["AUTH", "nobody", "read-pass"],
        &["AUTH", "read-pass"],
        &["AUTH", "gone", "gone-pass"],
    ] {
        let reply = send(&node, &cmd(wrong)).await;
        assert!(reply.starts_with("-WRONGPASS"), "{wrong:?}: {reply:?}");
    }
}

#[tokio::test]
async fn the_rules_limit_commands_and_keys() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;

    let mut reader = login(&node, "reader", "read-pass").await;
    assert_eq!(run(&mut reader, &["PING"]).await, "+PONG\r\n");
    assert_eq!(run(&mut reader, &["GET", "cache:a"]).await, "$-1\r\n");
    assert_eq!(run(&mut reader, &["GET", "other"]).await, KEY);
    assert_eq!(run(&mut reader, &["MGET", "cache:a", "x"]).await, KEY);
    assert_eq!(
        run(&mut reader, &["SET", "cache:a", "1"]).await,
        no_perm("set")
    );

    let mut writer = login(&node, "writer", "w1").await;
    assert_eq!(run(&mut writer, &["SET", "jobs:1", "v"]).await, "+OK\r\n");
    assert_eq!(run(&mut writer, &["GET", "jobs:1"]).await, KEY);
    assert_eq!(run(&mut writer, &["SET", "jobs:1", "w", "GET"]).await, KEY);
    assert_eq!(run(&mut writer, &["INCR", "jobs:2"]).await, KEY);
    assert_eq!(run(&mut writer, &["GET", "cache:a"]).await, "$-1\r\n");
    assert_eq!(run(&mut writer, &["FLUSHALL"]).await, no_perm("flushall"));

    let mut admin = login(&node, "admin", "admin-pass").await;
    // The denied `SET ... GET` changed nothing.
    assert_eq!(run(&mut admin, &["GET", "jobs:1"]).await, "$1\r\nv\r\n");
}

#[tokio::test]
async fn a_denied_key_stops_the_whole_command() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;
    let mut admin = login(&node, "admin", "admin-pass").await;
    assert_eq!(run(&mut admin, &["SET", "cache:x", "1"]).await, "+OK\r\n");
    assert_eq!(run(&mut admin, &["SET", "jobs:1", "1"]).await, "+OK\r\n");

    let mut writer = login(&node, "writer", "w1").await;
    assert_eq!(run(&mut writer, &["DEL", "jobs:1", "cache:x"]).await, KEY);
    let mset = ["MSET", "jobs:1", "2", "cache:x", "2"];
    assert_eq!(run(&mut writer, &mset).await, KEY);

    assert_eq!(
        run(&mut admin, &["MGET", "jobs:1", "cache:x"]).await,
        "*2\r\n$1\r\n1\r\n$1\r\n1\r\n"
    );
}

#[tokio::test]
async fn a_second_login_replaces_the_user() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;
    let mut s = login(&node, "reader", "read-pass").await;
    assert_eq!(run(&mut s, &["AUTH", "writer", "w1"]).await, "+OK\r\n");
    assert_eq!(run(&mut s, &["SET", "jobs:1", "v"]).await, "+OK\r\n");

    let failed = run(&mut s, &["AUTH", "reader", "wrong"]).await;
    assert!(failed.starts_with("-WRONGPASS"), "{failed:?}");
    assert_eq!(run(&mut s, &["SET", "jobs:1", "v"]).await, "+OK\r\n");

    assert_eq!(
        run(&mut s, &["AUTH", "reader", "read-pass"]).await,
        "+OK\r\n"
    );
    assert_eq!(run(&mut s, &["SET", "jobs:1", "v"]).await, no_perm("set"));
}

/// Wait until `ok` is true. The node checks the file every second, so 60 s is generous.
async fn eventually<F: std::future::Future<Output = bool>>(mut ok: impl FnMut() -> F) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ok().await {
        assert!(Instant::now() < deadline, "the node did not reload");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Replace the file with one rename, as Kubernetes does, so the node never reads half a file.
fn replace(path: &Path, content: &str) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, content).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

#[tokio::test]
async fn a_reload_changes_users_on_open_connections() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;
    let file = dir.path().join("users.acl");
    let mut r = login(&node, "reader", "read-pass").await;
    let mut w = login(&node, "writer", "w1").await;
    let mut a = login(&node, "admin", "admin-pass").await;

    replace(
        &file,
        "\
user default off
user admin on >admin-pass ~* +@all
user reader off >read-pass ~cache:* +@read +@connection
user writer on >w3 ~jobs:* +@read +@connection
",
    );
    eventually(|| async { send(&node, &cmd(&["AUTH", "writer", "w3"])).await == "+OK\r\n" }).await;

    let denied = run(&mut r, &["PING"]).await;
    assert!(denied.starts_with("-NOAUTH"), "{denied:?}");
    assert_eq!(run(&mut w, &["PING"]).await, "+PONG\r\n");
    assert_eq!(run(&mut w, &["SET", "jobs:1", "v"]).await, no_perm("set"));
    assert_eq!(run(&mut w, &["GET", "jobs:1"]).await, "$-1\r\n");
    let old = send(&node, &cmd(&["AUTH", "writer", "w1"])).await;
    assert!(old.starts_with("-WRONGPASS"), "{old:?}");
    assert_eq!(run(&mut a, &["PING"]).await, "+PONG\r\n");

    replace(
        &file,
        "user default off\nuser admin on >admin-pass ~* +@all\n",
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while !run(&mut w, &["PING"]).await.starts_with("-NOAUTH") {
        assert!(Instant::now() < deadline, "the node did not reload");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn a_broken_reload_keeps_the_users() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;
    replace(&dir.path().join("users.acl"), "usr broken\n");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        send(&node, &cmd(&["AUTH", "reader", "read-pass"])).await,
        "+OK\r\n"
    );
}
