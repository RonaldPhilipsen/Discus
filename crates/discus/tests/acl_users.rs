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
    // Not `eventually`: its closure cannot borrow the connection mutably.
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

#[tokio::test]
async fn a_user_without_the_connection_group_cannot_ping() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("users.acl");
    std::fs::write(
        &file,
        format!("{ACL_USERS}user nocon on >nocon-pass ~* +@read\n"),
    )
    .unwrap();
    let node = start(&[
        "--acl-file",
        file.to_str().unwrap(),
        "--auth-max-failures-per-source",
        "1000000",
    ])
    .await;
    let mut s = login(&node, "nocon", "nocon-pass").await;
    assert_eq!(run(&mut s, &["PING"]).await, no_perm("ping"));
    assert_eq!(
        run(&mut s, &["AUTH", "reader", "read-pass"]).await,
        "+OK\r\n"
    );
    assert_eq!(run(&mut s, &["PING"]).await, "+PONG\r\n");
}

const READ_ONLY: &str = "-ERR Discus reads its users from the file of --acl-file. \
                         Change the file, and the node loads it again.\r\n";

#[tokio::test]
async fn acl_shows_the_user_and_the_categories() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;
    let mut s = login(&node, "reader", "read-pass").await;

    assert_eq!(run(&mut s, &["ACL", "WHOAMI"]).await, "$6\r\nreader\r\n");
    assert_eq!(run(&mut s, &["acl", "whoami"]).await, "$6\r\nreader\r\n");

    let all = run(&mut s, &["ACL", "CAT"]).await;
    assert!(all.starts_with("*21\r\n$8\r\nkeyspace\r\n"), "{all:?}");

    let dangerous = run(&mut s, &["ACL", "CAT", "dangerous"]).await;
    assert!(dangerous.contains("$8\r\nflushall\r\n"), "{dangerous:?}");
    assert!(dangerous.contains("$6\r\nconfig\r\n"), "{dangerous:?}");
    assert!(!dangerous.contains("get\r\n"), "{dangerous:?}");

    assert_eq!(
        run(&mut s, &["ACL", "CAT", "nothing"]).await,
        "-ERR Unknown category 'nothing'\r\n"
    );
    assert_eq!(
        run(&mut s, &["ACL", "CAT", "all"]).await,
        "-ERR Unknown category 'all'\r\n"
    );

    let admin_only = run(&mut s, &["ACL", "CAT", "admin"]).await;
    assert!(admin_only.contains("$8\r\nacl|list\r\n"), "{admin_only:?}");
    assert!(!admin_only.contains("whoami"), "{admin_only:?}");
    let connection = run(&mut s, &["ACL", "CAT", "connection"]).await;
    assert!(
        connection.contains("$10\r\nacl|whoami\r\n"),
        "{connection:?}"
    );
    assert!(connection.contains("$3\r\nacl\r\n"), "{connection:?}");

    // The user `reader` has `+@connection`. That category does not hold the subcommands that
    // show or change the users.
    for sub in [
        &["SETUSER", "x", "on"][..],
        &["DELUSER", "x"],
        &["LIST"],
        &["USERS"],
        &["GETUSER", "reader"],
        &["getuser", "reader"],
        &["LOAD"],
        &["SAVE"],
        &["LOG"],
        &["GENPASS"],
        &["DRYRUN", "reader", "get", "a"],
    ] {
        let mut parts = vec!["ACL"];
        parts.extend_from_slice(sub);
        let name = format!("acl|{}", sub[0].to_lowercase());
        assert_eq!(run(&mut s, &parts).await, no_perm(&name), "{sub:?}");
    }

    let mut admin = login(&node, "admin", "admin-pass").await;
    for sub in [
        &["SETUSER", "x", "on"][..],
        &["DELUSER", "x"],
        &["LOAD"],
        &["SAVE"],
        &["LOG"],
        &["GENPASS"],
        &["DRYRUN", "reader", "get", "a"],
    ] {
        let mut parts = vec!["ACL"];
        parts.extend_from_slice(sub);
        assert_eq!(run(&mut admin, &parts).await, READ_ONLY, "{sub:?}");
    }

    assert_eq!(
        run(&mut s, &["ACL", "BOGUS"]).await,
        "-ERR unknown subcommand or wrong number of arguments for 'ACL BOGUS'\r\n"
    );
    assert_eq!(
        run(&mut s, &["ACL", "WHOAMI", "x"]).await,
        "-ERR unknown subcommand or wrong number of arguments for 'ACL WHOAMI'\r\n"
    );
    assert_eq!(
        run(&mut s, &["ACL", "CAT", "a", "b"]).await,
        "-ERR unknown subcommand or wrong number of arguments for 'ACL CAT'\r\n"
    );
}

#[tokio::test]
async fn whoami_is_default_without_a_user() {
    let node = start(&["--auth-policy", "none"]).await;
    let mut s = connect(node.addr).await;
    assert_eq!(run(&mut s, &["ACL", "WHOAMI"]).await, "$7\r\ndefault\r\n");
    for sub in [&["LIST"][..], &["USERS"], &["GETUSER", "default"]] {
        let mut parts = vec!["ACL"];
        parts.extend_from_slice(sub);
        assert_eq!(
            run(&mut s, &parts).await,
            "-ERR This node has no users, because --acl-file is not set.\r\n"
        );
    }
}

fn bulk(text: &str) -> String {
    format!("${}\r\n{text}\r\n", text.len())
}

/// SHA-256 of the text `pw`.
const PW_HASH: &str = "30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4";

const LISTED_USERS: &str = "\
user default off
user root on >pw ~* +@all
user limited on >pw %R~cache:* %W~jobs:* +@all -@dangerous +acl|users
user open on nopass allkeys +@read +@connection
";

#[tokio::test]
async fn an_allowed_user_lists_the_users() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_file(&dir, LISTED_USERS).await;
    let mut root = login(&node, "root", "pw").await;

    assert_eq!(
        run(&mut root, &["ACL", "USERS"]).await,
        format!(
            "*4\r\n{}{}{}{}",
            bulk("default"),
            bulk("limited"),
            bulk("open"),
            bulk("root")
        )
    );
    let lines = [
        "user default off resetchannels -@all".to_owned(),
        format!(
            "user limited on #{PW_HASH} %R~cache:* %W~jobs:* resetchannels +@all -@dangerous \
             +acl|users"
        ),
        "user open on nopass ~* resetchannels -@all +@read +@connection".to_owned(),
        format!("user root on #{PW_HASH} ~* resetchannels +@all"),
    ];
    let expected: String = lines.iter().map(|line| bulk(line)).collect();
    assert_eq!(
        run(&mut root, &["acl", "list"]).await,
        format!("*4\r\n{expected}")
    );

    assert_eq!(
        run(&mut root, &["ACL", "GETUSER", "limited"]).await,
        format!(
            "*12\r\n{}*1\r\n{}{}*1\r\n{}{}{}{}{}{}$0\r\n\r\n{}*0\r\n",
            bulk("flags"),
            bulk("on"),
            bulk("passwords"),
            bulk(PW_HASH),
            bulk("commands"),
            bulk("+@all -@dangerous +acl|users"),
            bulk("keys"),
            bulk("%R~cache:* %W~jobs:*"),
            bulk("channels"),
            bulk("selectors"),
        )
    );
    let open = run(&mut root, &["ACL", "GETUSER", "open"]).await;
    assert!(
        open.starts_with(&format!(
            "*12\r\n{}*2\r\n{}{}{}*0\r\n",
            bulk("flags"),
            bulk("on"),
            bulk("nopass"),
            bulk("passwords")
        )),
        "{open:?}"
    );
    assert_eq!(
        run(&mut root, &["ACL", "GETUSER", "nobody"]).await,
        "$-1\r\n"
    );
    assert_eq!(run(&mut root, &["ACL", "GETUSER", "ROOT"]).await, "$-1\r\n");
    assert_eq!(
        run(&mut root, &["ACL", "LIST", "x"]).await,
        "-ERR unknown subcommand or wrong number of arguments for 'ACL LIST'\r\n"
    );
}

#[tokio::test]
async fn a_rule_for_one_subcommand_limits_the_user() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_file(&dir, LISTED_USERS).await;

    // `-@dangerous` takes away the three subcommands, and `+acl|users` gives one back.
    let mut limited = login(&node, "limited", "pw").await;
    let users = run(&mut limited, &["ACL", "USERS"]).await;
    assert!(users.starts_with("*4\r\n"), "{users:?}");
    assert_eq!(
        run(&mut limited, &["ACL", "LIST"]).await,
        no_perm("acl|list")
    );
    assert_eq!(
        run(&mut limited, &["ACL", "GETUSER", "root"]).await,
        no_perm("acl|getuser")
    );
    assert_eq!(
        run(&mut limited, &["ACL", "WHOAMI"]).await,
        "$7\r\nlimited\r\n"
    );

    let mut open = login(&node, "open", "x").await;
    assert_eq!(run(&mut open, &["ACL", "WHOAMI"]).await, "$4\r\nopen\r\n");
    for sub in ["LIST", "USERS"] {
        let reply = run(&mut open, &["ACL", sub]).await;
        assert_eq!(reply, no_perm(&format!("acl|{}", sub.to_lowercase())));
        assert!(!reply.contains(PW_HASH));
    }
}

#[tokio::test]
async fn acl_list_shows_the_users_of_a_reload() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_with(&dir).await;
    let mut admin = login(&node, "admin", "admin-pass").await;
    let before = run(&mut admin, &["ACL", "USERS"]).await;
    assert!(before.starts_with("*5\r\n"), "{before:?}");

    replace(
        &dir.path().join("users.acl"),
        "user admin on >admin-pass ~* +@all\nuser extra off\n",
    );
    let after = format!("*2\r\n{}{}", bulk("admin"), bulk("extra"));
    eventually(|| async {
        let mut s = login(&node, "admin", "admin-pass").await;
        run(&mut s, &["ACL", "USERS"]).await == after
    })
    .await;
    assert_eq!(run(&mut admin, &["ACL", "USERS"]).await, after);
}

/// A node with the users of `acl`.
async fn start_file(dir: &tempfile::TempDir, acl: &str) -> Node {
    let file = dir.path().join("users.acl");
    std::fs::write(&file, acl).unwrap();
    start(&[
        "--acl-file",
        file.to_str().unwrap(),
        "--auth-max-failures-per-source",
        "1000000",
    ])
    .await
}

#[tokio::test]
async fn auth_with_only_a_password_logs_in_the_user_default() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_file(
        &dir,
        "user default on >default-pass ~cache:* +@read +@connection\n",
    )
    .await;
    let mut s = connect(node.addr).await;
    assert_eq!(run(&mut s, &["AUTH", "default-pass"]).await, "+OK\r\n");
    assert_eq!(run(&mut s, &["ACL", "WHOAMI"]).await, "$7\r\ndefault\r\n");
    assert_eq!(run(&mut s, &["GET", "cache:a"]).await, "$-1\r\n");
    assert_eq!(run(&mut s, &["GET", "other"]).await, KEY);
    assert_eq!(run(&mut s, &["SET", "cache:a", "1"]).await, no_perm("set"));
    let wrong = send(&node, &cmd(&["AUTH", "wrong"])).await;
    assert!(wrong.starts_with("-WRONGPASS"), "{wrong:?}");
}

#[tokio::test]
async fn a_nopass_user_logs_in_with_any_password() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_file(&dir, "user default off\nuser open on nopass ~* +@all\n").await;
    for password in ["", "x", "anything at all"] {
        let mut s = login(&node, "open", password).await;
        assert_eq!(run(&mut s, &["ACL", "WHOAMI"]).await, "$4\r\nopen\r\n");
    }
}
