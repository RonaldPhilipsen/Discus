#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::gossip::{wait_agreed, wait_until, GossipNode, Net};
use discus_store::Value as Stored;
use redis::AsyncCommands;

const LONG: Duration = Duration::from_secs(30);

#[tokio::test]
async fn pushes_pops_ranges_and_trims() {
    let mut c = common::connect(&[]).await;
    let n: i64 = c.rpush("l", &["a", "b", "c", "d", "e"]).await.unwrap();
    assert_eq!(n, 5);
    let n: i64 = c.lpush("l", &["y", "z"]).await.unwrap();
    assert_eq!(n, 7);
    let all: Vec<String> = c.lrange("l", 0, -1).await.unwrap();
    assert_eq!(all, ["z", "y", "a", "b", "c", "d", "e"]);
    let mid: Vec<String> = c.lrange("l", -100, 1).await.unwrap();
    assert_eq!(mid, ["z", "y"]);
    let none: Vec<String> = c.lrange("l", 5, 1).await.unwrap();
    assert!(none.is_empty());
    let last: Option<String> = c.lindex("l", -1).await.unwrap();
    assert_eq!(last.as_deref(), Some("e"));
    let past: Option<String> = c.lindex("l", 99).await.unwrap();
    assert_eq!(past, None);
    let head: Option<String> = c.lpop("l", None).await.unwrap();
    assert_eq!(head.as_deref(), Some("z"));
    let tail: Vec<String> = c.rpop("l", std::num::NonZeroUsize::new(2)).await.unwrap();
    assert_eq!(tail, ["e", "d"]);
    let len: i64 = c.llen("l").await.unwrap();
    assert_eq!(len, 4);
    let _: () = c.ltrim("l", 1, -2).await.unwrap();
    let all: Vec<String> = c.lrange("l", 0, -1).await.unwrap();
    assert_eq!(all, ["a", "b"]);
    let t: String = redis::cmd("TYPE")
        .arg("l")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(t, "list");

    let _: () = c.ltrim("l", 5, 1).await.unwrap();
    let exists: i64 = c.exists("l").await.unwrap();
    assert_eq!(exists, 0, "an empty list stays");
    let _: () = c.rpush("p", "only").await.unwrap();
    let _: Option<String> = c.rpop("p", None).await.unwrap();
    let exists: i64 = c.exists("p").await.unwrap();
    assert_eq!(exists, 0, "a popped-empty list stays");
}

#[tokio::test]
async fn wrong_type_and_the_member_limit() {
    let mut c = common::connect(&["--max-collection-members", "3"]).await;
    let _: () = c.set("s", "v").await.unwrap();
    for cmd in [
        &["LPUSH", "s", "a"][..],
        &["LLEN", "s"],
        &["LTRIM", "s", "0", "1"],
    ] {
        let err = redis::cmd(cmd[0])
            .arg(&cmd[1..])
            .query_async::<redis::Value>(&mut c)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("WRONGTYPE"), "{cmd:?}: {err}");
    }
    let n: i64 = c.rpush("l", &["a", "a", "a"]).await.unwrap();
    assert_eq!(n, 3, "a list keeps duplicates");
    let err = c.rpush::<_, _, i64>("l", "a").await.unwrap_err();
    assert!(err.to_string().contains("collection too large"), "{err}");
    let len: i64 = c.llen("l").await.unwrap();
    assert_eq!(len, 3, "a refused push changed the list");
}

/// The replies of redis-server 8.10.2, byte for byte. A missing key with a count is a null
/// array in RESP2 and one null in RESP3. A bad count is an error before the type of the key.
#[tokio::test]
async fn replies_match_valkey_on_the_wire() {
    let addr = common::start_node(&[]).await;
    let cases: &[(&[&str], &str)] = &[
        (&["RPUSH", "l", "a", "b"], ":2\r\n"),
        (&["SET", "s", "v"], "+OK\r\n"),
        (&["LPOP", "l", "0"], "*0\r\n"),
        (&["LPOP", "missing", "0"], "*-1\r\n"),
        (&["LPOP", "missing"], "$-1\r\n"),
        (&["RPOP", "missing", "1"], "*-1\r\n"),
        (
            &["LPOP", "s", "x"],
            "-ERR value is out of range, must be positive\r\n",
        ),
        (
            &["LPOP", "l", "-1"],
            "-ERR value is out of range, must be positive\r\n",
        ),
        (
            &["LPOP", "l", "1", "2"],
            "-ERR wrong number of arguments for 'lpop' command\r\n",
        ),
        (
            &["LPOP", "s", "0"],
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        ),
        (&["LINDEX", "missing", "0"], "$-1\r\n"),
        (
            &["LRANGE", "l", "x", "1"],
            "-ERR value is not an integer or out of range\r\n",
        ),
        (&["LRANGE", "missing", "0", "-1"], "*0\r\n"),
        (&["LTRIM", "missing", "0", "1"], "+OK\r\n"),
        (&["RPOP", "l", "1"], "*1\r\n$1\r\nb\r\n"),
    ];
    for (parts, want) in cases {
        let got = common::roundtrip(addr, &discus_wire::encode_command(&common::cmd(parts))).await;
        assert_eq!(got, *want, "{parts:?}");
    }
    let mut wire = Vec::new();
    for parts in [
        &["HELLO", "3"][..],
        &["LPOP", "missing", "1"],
        &["LPOP", "missing"],
    ] {
        wire.extend_from_slice(&discus_wire::encode_command(&common::cmd(parts)));
    }
    let got = read_until(addr, &wire, b"_\r\n_\r\n").await;
    assert!(
        got.ends_with(b"_\r\n_\r\n"),
        "{:?}",
        String::from_utf8_lossy(&got)
    );
}

async fn read_until(addr: std::net::SocketAddr, wire: &[u8], end: &[u8]) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = common::client_tls::connect(addr).await;
    s.write_all(wire).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let mut got = Vec::new();
    while !got.ends_with(end) {
        let n = s.read(&mut buf).await.unwrap();
        assert!(n > 0, "closed: {:?}", String::from_utf8_lossy(&got));
        got.extend_from_slice(&buf[..n]);
    }
    got
}

fn list_of(n: &GossipNode, key: &str) -> Option<Vec<String>> {
    n.node.store.read(0, key.as_bytes(), |e| match &e?.value {
        Stored::List(l) => Some(
            l.iter()
                .map(|m| String::from_utf8_lossy(m).into_owned())
                .collect(),
        ),
        _ => None,
    })
}

/// Replication copies a list in order, and the handoff to a joiner keeps that order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_replicate_and_hand_off_in_order() {
    let net = Net::new();
    let args = ["--replication-factor", "3"];
    let nodes = net.start_agreed(2, &args).await;
    let mut c = nodes[0].redis().await;
    let keys: Vec<String> = (0..20).map(|i| format!("l{i}")).collect();
    for k in &keys {
        let _: () = c.rpush(k, &["a", "b"]).await.unwrap();
        let _: () = c.lpush(k, "z").await.unwrap();
    }
    let want = Some(vec!["z".to_string(), "a".into(), "b".into()]);
    assert!(
        wait_until(LONG, || nodes
            .iter()
            .all(|n| keys.iter().all(|k| list_of(n, k) == want)))
        .await,
        "a replica missed a list"
    );
    let joiner = net.start(&[nodes[0].cluster_addr], &args).await;
    wait_agreed(&[&nodes[0], &nodes[1], &joiner], LONG).await;
    assert!(
        wait_until(LONG, || keys.iter().all(|k| list_of(&joiner, k) == want)).await,
        "the joiner missed a list"
    );
}
