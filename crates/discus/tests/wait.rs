#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::cluster::{start_cluster, Cluster, ClusterNode};
use common::gossip::{wait_agreed, wait_until, GossipNode, Net};
use discus_placement::Version;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ARGS: &[&str] = &["--replication-factor", "3", "--primary-read-prefix", "sess"];

async fn client(n: &ClusterNode) -> (MultiplexedConnection, u64) {
    let mut conn = n.redis().await;
    let id: u64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(&mut conn)
        .await
        .unwrap();
    (conn, id)
}

/// The record of connection `id` on `n`, sorted.
fn record(n: &ClusterNode, id: u64) -> Vec<(u32, Bytes, Version)> {
    let writes = n.node.conn_writes(id).unwrap();
    let w = writes.lock();
    let mut all: Vec<_> = w
        .record
        .entries()
        .map(|(db, k, v)| (db, k.clone(), v))
        .collect();
    all.sort();
    all
}

/// The entry of `key` in database `db` at its version on its primary.
fn entry(nodes: &[ClusterNode], db: u32, key: &str) -> (u32, Bytes, Version) {
    let primary = nodes[0].primary(key.as_bytes());
    let primary = nodes.iter().find(|n| n.id == primary).unwrap();
    let version = primary.node.store.version(db, key.as_bytes()).unwrap();
    (db, Bytes::from(key.to_string()), version)
}

#[tokio::test]
async fn a_write_records_its_key_at_the_version_of_the_primary() {
    let c = start_cluster(3, ARGS).await;
    let (mut conn, id) = client(&c.nodes[0]).await;
    let k = c.key_on(1, "k");
    let _: () = conn.set(&k, "v").await.unwrap();
    assert_eq!(record(&c.nodes[0], id), vec![entry(&c.nodes, 0, &k)]);

    let _: i64 = conn.del("missing").await.unwrap();
    let _: i64 = conn.incr("cnt:x", 1).await.unwrap();
    let _: () = conn.set("cnt:y", "1").await.unwrap();
    assert_eq!(record(&c.nodes[0], id), vec![entry(&c.nodes, 0, &k)]);

    let _: () = conn.set(&k, "w").await.unwrap();
    assert_eq!(record(&c.nodes[0], id), vec![entry(&c.nodes, 0, &k)]);

    let (_, other) = client(&c.nodes[0]).await;
    assert!(record(&c.nodes[0], other).is_empty());
}

#[tokio::test]
async fn a_primary_read_key_and_a_multi_key_write_enter_the_record() {
    let c = start_cluster(3, ARGS).await;
    let (mut conn, id) = client(&c.nodes[0]).await;
    let _: () = conn.set("sess:a", "v").await.unwrap();
    let keys: Vec<String> = (0..3).map(|i| c.key_on(i, "m")).collect();
    let pairs: Vec<(&str, &str)> = keys.iter().map(|k| (k.as_str(), "v")).collect();
    let _: () = conn.mset(&pairs).await.unwrap();
    let mut want = vec![entry(&c.nodes, 0, "sess:a")];
    want.extend(keys.iter().map(|k| entry(&c.nodes, 0, k)));
    want.sort();
    assert_eq!(record(&c.nodes[0], id), want);

    let _: i64 = conn.del(&[&keys[0], "missing"]).await.unwrap();
    want.retain(|e| e.1 != keys[0].as_bytes());
    want.push(entry(&c.nodes, 0, &keys[0]));
    want.sort();
    assert_eq!(record(&c.nodes[0], id), want);
}

#[tokio::test]
async fn rename_and_copy_record_the_keys_they_change() {
    let c = start_cluster(3, ARGS).await;
    let (mut conn, id) = client(&c.nodes[0]).await;
    let (src, dst, copy) = (c.key_on(1, "src"), c.key_on(2, "dst"), c.key_on(0, "cp"));
    let _: () = conn.set(&src, "v").await.unwrap();
    let _: () = conn.rename(&src, &dst).await.unwrap();
    let mut want = vec![entry(&c.nodes, 0, &src), entry(&c.nodes, 0, &dst)];
    want.sort();
    assert_eq!(record(&c.nodes[0], id), want);

    let _: () = conn.set(&copy, "v").await.unwrap();
    let writes = c.nodes[0].node.conn_writes(id).unwrap();
    writes.lock().record.clear();
    let copied: i64 = redis::cmd("COPY")
        .arg(&dst)
        .arg(&copy)
        .arg("REPLACE")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(copied, 1);
    assert_eq!(record(&c.nodes[0], id), vec![entry(&c.nodes, 0, &copy)]);
}

#[tokio::test]
async fn a_flush_empties_the_record() {
    let mut args = ARGS.to_vec();
    args.extend(["--databases", "2"]);
    let nodes = Net::new().start_agreed(3, &args).await;
    let (mut conn, id) = client(&nodes[0]).await;
    let _: () = conn.set("a", "v").await.unwrap();
    let moved: bool = redis::cmd("MOVE")
        .arg("a")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(moved);
    let _: () = conn.set("b", "v").await.unwrap();
    let mut want = vec![
        entry(&nodes, 0, "a"),
        entry(&nodes, 1, "a"),
        entry(&nodes, 0, "b"),
    ];
    want.sort();
    assert_eq!(record(&nodes[0], id), want);

    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();
    assert_eq!(record(&nodes[0], id), vec![entry(&nodes, 1, "a")]);
    let _: () = conn.set("b", "v").await.unwrap();
    let _: () = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    assert!(record(&nodes[0], id).is_empty());
}

/// A cap that a wait which ends at once stays far below, and that a wait for its full timeout of
/// 60 s exceeds.
const CAP: Duration = Duration::from_secs(30);

/// A timeout that a wait which can succeed never reaches on a working cluster.
const LONG: u64 = 30_000;

const RF3: &[&str] = &["--replication-factor", "3"];

/// The reply of `args`, or the text of its error as the client sees it.
async fn query(conn: &mut MultiplexedConnection, args: &[&str]) -> Result<redis::Value, String> {
    let mut cmd = redis::cmd(args[0]);
    for a in &args[1..] {
        cmd.arg(*a);
    }
    cmd.query_async(conn)
        .await
        .map_err(|e| format!("{} {}", e.code().unwrap_or(""), e.detail().unwrap_or("")))
}

/// `WAIT numreplicas timeout`, and how long it took.
async fn wait(conn: &mut MultiplexedConnection, numreplicas: i64, timeout: u64) -> (i64, Duration) {
    let started = Instant::now();
    let n: i64 = redis::cmd("WAIT")
        .arg(numreplicas)
        .arg(timeout)
        .query_async(conn)
        .await
        .unwrap();
    (n, started.elapsed())
}

/// The primary of `key` stops sending replication batches.
fn stall(c: &Cluster, key: &str, on: bool) {
    c.by_id(c.nodes[0].primary(key.as_bytes()))
        .node
        .faults
        .hold_replication
        .store(on, Ordering::Relaxed);
}

#[tokio::test]
async fn wait_rows() {
    let c = start_cluster(3, RF3).await;
    let mut conn = c.nodes[0].redis().await;
    let err = |s: &str| Err(s.to_string());
    assert_eq!(
        query(&mut conn, &["WAIT", "1"]).await,
        err("ERR wrong number of arguments for 'wait' command")
    );
    assert_eq!(
        query(&mut conn, &["WAIT", "x", "300"]).await,
        err("ERR value is not an integer or out of range")
    );
    assert_eq!(
        query(&mut conn, &["WAIT", "1", "x"]).await,
        err("ERR timeout is not an integer or out of range")
    );
    assert_eq!(
        query(&mut conn, &["WAIT", "1", "-5"]).await,
        err("ERR timeout is negative")
    );
    assert_eq!(
        query(&mut conn, &["WAIT", "1", "9223372036854775807"]).await,
        err("ERR timeout is out of range")
    );
    // Without writes, the count is the number of secondaries that any key has.
    let (n, took) = wait(&mut conn, -1, 60_000).await;
    assert_eq!((n, took < CAP), (2, true), "{took:?}");
    let (n, took) = wait(&mut conn, 0, 0).await;
    assert_eq!((n, took < CAP), (2, true), "{took:?}");
    let (n, took) = wait(&mut conn, 2, 60_000).await;
    assert_eq!((n, took < CAP), (2, true), "{took:?}");
    let (n, took) = wait(&mut conn, 3, 300).await;
    assert_eq!(n, 2);
    assert!(took >= Duration::from_millis(300), "{took:?}");
}

#[tokio::test]
async fn wait_on_a_node_without_secondaries_counts_zero() {
    let mut conn = common::connect(&[]).await;
    let _: () = conn.set("k", "v").await.unwrap();
    let (n, took) = wait(&mut conn, 0, 0).await;
    assert_eq!((n, took < CAP), (0, true), "{took:?}");
    let (n, took) = wait(&mut conn, 1, 300).await;
    assert_eq!(n, 0);
    assert!(took >= Duration::from_millis(300), "{took:?}");
}

#[tokio::test]
async fn waitaof_rows() {
    let c = start_cluster(3, RF3).await;
    let mut conn = c.nodes[0].redis().await;
    let err = |s: &str| Err(s.to_string());
    let zeros = Ok(redis::Value::Array(vec![
        redis::Value::Int(0),
        redis::Value::Int(0),
    ]));
    assert_eq!(
        query(&mut conn, &["WAITAOF", "1", "0", "300"]).await,
        err("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
    );
    assert_eq!(
        query(&mut conn, &["WAITAOF", "0", "-1", "300"]).await,
        err("ERR value is out of range, must be positive")
    );
    assert_eq!(
        query(&mut conn, &["WAITAOF", "0", "1", "-1"]).await,
        err("ERR timeout is negative")
    );
    assert_eq!(
        query(&mut conn, &["WAITAOF", "x", "0", "0"]).await,
        err("ERR value is not an integer or out of range")
    );
    assert_eq!(
        query(&mut conn, &["WAITAOF", "2", "0", "0"]).await,
        err("ERR value is out of range, value must between 0 and 1")
    );
    assert_eq!(
        query(&mut conn, &["WAITAOF", "1", "x", "300"]).await,
        err("ERR value is out of range, must be positive")
    );
    assert_eq!(
        query(&mut conn, &["WAITAOF", "0", "0"]).await,
        err("ERR wrong number of arguments for 'waitaof' command")
    );
    let started = Instant::now();
    assert_eq!(query(&mut conn, &["WAITAOF", "0", "0", "0"]).await, zeros);
    assert!(started.elapsed() < CAP);
    let started = Instant::now();
    assert_eq!(query(&mut conn, &["WAITAOF", "0", "1", "300"]).await, zeros);
    assert!(started.elapsed() >= Duration::from_millis(300));
}

#[tokio::test]
async fn wait_counts_both_secondaries() {
    let c = start_cluster(3, RF3).await;
    let mut conn = c.nodes[0].redis().await;
    let _: () = conn.set(c.key_on(1, "k"), "v").await.unwrap();
    assert_eq!(wait(&mut conn, 2, LONG).await.0, 2);
}

#[tokio::test]
async fn a_stalled_primary_gives_zero_after_the_timeout() {
    let c = start_cluster(3, RF3).await;
    let k = c.key_on(1, "k");
    let mut conn = c.nodes[0].redis().await;
    stall(&c, &k, true);
    let _: () = conn.set(&k, "v").await.unwrap();
    let (n, took) = wait(&mut conn, 1, 300).await;
    assert_eq!(n, 0);
    assert!(took >= Duration::from_millis(300), "{took:?}");
    stall(&c, &k, false);
    let (n, took) = wait(&mut conn, 1, 0).await;
    assert!(n >= 1, "{n}");
    assert!(took < CAP, "{took:?}");
}

#[tokio::test]
async fn the_lower_count_of_two_primaries_wins() {
    let c = start_cluster(3, RF3).await;
    let (k1, k2) = (c.key_on(1, "a"), c.key_on(2, "b"));
    let mut conn = c.nodes[0].redis().await;
    stall(&c, &k1, true);
    let _: () = conn.set(&k1, "v").await.unwrap();
    let _: () = conn.set(&k2, "v").await.unwrap();
    let (n, took) = wait(&mut conn, 1, 300).await;
    assert_eq!(n, 0);
    assert!(took >= Duration::from_millis(300), "{took:?}");
}

#[tokio::test]
async fn a_hot_key_wait_ends_early() {
    let c = start_cluster(3, RF3).await;
    let k = c.key_on(0, "hot");
    let p = c.by_id(c.nodes[0].primary(k.as_bytes()));
    let done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    // The writers run the command on the primary without the client path, so the queues always
    // hold the key. Only a stored version newer than the recorded one can end the wait.
    let spec = discus::command::lookup(b"SET").unwrap();
    let writers: Vec<_> = (0..4)
        .map(|i| {
            let (node, done, cmd) = (p.node.clone(), done.clone(), common::cmd(&["SET", &k, "w"]));
            tokio::spawn(async move {
                let mut n = 0u64;
                while !done.load(Ordering::Relaxed) && started.elapsed() < CAP {
                    discus::cluster::local(&node, spec, 0, &cmd);
                    n += 1;
                    tokio::task::yield_now().await;
                }
                (i, n)
            })
        })
        .collect();
    let mut conn = c.nodes[1].redis().await;
    let _: () = conn.set(&k, "o").await.unwrap();
    let (n, took) = wait(&mut conn, 1, 60_000).await;
    done.store(true, Ordering::Relaxed);
    for w in writers {
        let (i, n) = w.await.unwrap();
        assert!(n > 0, "writer {i} never wrote");
    }
    assert!(n >= 1, "{n}");
    assert!(took < CAP, "{took:?}");
}

#[tokio::test]
async fn a_pipelined_wait_sees_the_write() {
    let c = start_cluster(3, RF3).await;
    let k = c.key_on(1, "k");
    let mut conn = c.nodes[0].redis().await;
    let (ok, n): (String, i64) = redis::pipe()
        .cmd("SET")
        .arg(&k)
        .arg("v")
        .cmd("WAIT")
        .arg(2)
        .arg(LONG)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!((ok.as_str(), n), ("OK", 2));
    let v = c
        .by_id(c.nodes[0].primary(k.as_bytes()))
        .node
        .store
        .version(0, k.as_bytes())
        .unwrap();
    let holders = c
        .nodes
        .iter()
        .filter(|n| n.node.store.version(0, k.as_bytes()) >= Some(v))
        .count();
    assert_eq!(holders, 3);
}

#[tokio::test]
async fn wait_counts_the_current_owners_after_a_join() {
    let net = Net::new();
    let nodes = net.start_agreed(3, RF3).await;
    let (mut conn, id) = client(&nodes[0]).await;
    let keys: Vec<String> = (0..30).map(|i| format!("j{i}")).collect();
    for k in &keys {
        let _: () = conn.set(k, "v").await.unwrap();
    }
    assert_eq!(wait(&mut conn, 2, LONG).await.0, 2);
    // A count of 2 lets every key leave the record, so the keys go in again after the join.
    for k in &keys {
        let _: () = conn.set(k, "w").await.unwrap();
    }
    let written: Vec<(&String, Version)> = keys
        .iter()
        .map(|k| {
            (
                k,
                nodes[0]
                    .node
                    .conn_writes(id)
                    .unwrap()
                    .lock()
                    .record
                    .entries()
                    .find(|(_, key, _)| key.as_ref() == k.as_bytes())
                    .unwrap()
                    .2,
            )
        })
        .collect();
    assert!(
        wait_until(Duration::from_secs(20), || {
            nodes.iter().all(|n| {
                let rep = n.node.replicator().unwrap();
                !rep.scanning() && keys.iter().all(|k| rep.settled(0, k.as_bytes()))
            })
        })
        .await
    );
    for n in &nodes {
        n.node.faults.hold_scan.store(true, Ordering::Relaxed);
    }
    let joiner = net.start(&[nodes[0].cluster_addr], RF3).await;
    let all: Vec<&GossipNode> = nodes.iter().chain([&joiner]).collect();
    wait_agreed(&all, Duration::from_secs(30)).await;
    let epoch = nodes[0].node.epochs().current().clone();
    assert!(epoch.is_normal(joiner.id));
    let moved = keys
        .iter()
        .filter(|k| epoch.owners(k.as_bytes(), 3).contains(&joiner.id))
        .count();
    assert!(moved > 0);
    // The joiner holds none of its keys while the scans hold.
    let (n, _) = wait(&mut conn, 2, 300).await;
    assert!(n < 2, "{n}");

    for n in &nodes {
        n.node.faults.hold_scan.store(false, Ordering::Relaxed);
    }
    let (n, took) = wait(&mut conn, 2, 25_000).await;
    assert_eq!(n, 2, "{took:?}");
    let epoch = nodes[0].node.epochs().current().clone();
    for (k, v) in written {
        for owner in epoch.owners(k.as_bytes(), 3) {
            let n = all.iter().find(|n| n.id == owner).unwrap();
            assert!(
                n.node.store.version(0, k.as_bytes()) >= Some(v),
                "{owner} lacks {k}"
            );
        }
    }
}

#[tokio::test]
async fn a_flushed_key_does_not_block_wait() {
    let c = start_cluster(3, RF3).await;
    let k = c.key_on(1, "k");
    let mut conn = c.nodes[0].redis().await;
    stall(&c, &k, true);
    let _: () = conn.set(&k, "v").await.unwrap();
    let mut other = c.nodes[2].redis().await;
    let _: () = redis::cmd("FLUSHALL")
        .query_async(&mut other)
        .await
        .unwrap();
    // A key that no owner holds counts 0, as a write lost with its primary does.
    let (n, took) = wait(&mut conn, 1, 300).await;
    assert_eq!(n, 0);
    assert!(took >= Duration::from_millis(300), "{took:?}");
    // A flush on the connection itself empties the record.
    let _: () = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    let (n, took) = wait(&mut conn, 1, 0).await;
    assert!(n >= 1, "{n}");
    assert!(took < CAP, "{took:?}");
}

/// A gossip cluster of three nodes, a connection on the first, and a key whose primary is
/// another node.
async fn lost_primary(
    net: &Net,
    extra: &[&str],
) -> (Vec<GossipNode>, MultiplexedConnection, u64, String) {
    let mut args = RF3.to_vec();
    args.extend_from_slice(extra);
    let nodes = net.start_agreed(3, &args).await;
    let (conn, id) = client(&nodes[0]).await;
    let k = (0..)
        .map(|i| format!("lost:{i}"))
        .find(|k| nodes[0].primary(k.as_bytes()) == nodes[1].id)
        .unwrap();
    (nodes, conn, id, k)
}

#[tokio::test]
async fn a_write_lost_with_its_primary_counts_zero() {
    let net = Net::new();
    let (nodes, mut conn, _, k) = lost_primary(&net, &[]).await;
    nodes[1]
        .node
        .faults
        .hold_replication
        .store(true, Ordering::Relaxed);
    let _: () = conn.set(&k, "v").await.unwrap();
    nodes[1].kill();
    let rest = [&nodes[0], &nodes[2]];
    wait_agreed(&rest, Duration::from_secs(30)).await;
    assert!(!nodes[0].node.epochs().current().is_normal(nodes[1].id));
    let (n, took) = wait(&mut conn, 1, 2_000).await;
    assert_eq!(n, 0);
    assert!(took >= Duration::from_millis(2_000), "{took:?}");
}

#[tokio::test]
async fn an_overflow_counts_zero_after_a_member_loss() {
    let net = Net::new();
    let (nodes, mut conn, id, _) = lost_primary(&net, &["--write-set-keys", "4"]).await;
    for i in 0..5 {
        let _: () = conn.set(format!("o{i}"), "v").await.unwrap();
    }
    let writes = nodes[0].node.conn_writes(id).unwrap();
    assert!(writes.lock().record.overflowed());
    nodes[2].kill();
    let rest = [&nodes[0], &nodes[1]];
    wait_agreed(&rest, Duration::from_secs(30)).await;
    let (n, took) = wait(&mut conn, 1, 2_000).await;
    assert_eq!(n, 0);
    assert!(took >= Duration::from_millis(2_000), "{took:?}");
}

#[tokio::test]
async fn mset_keys_are_checked_at_their_own_versions() {
    let c = start_cluster(3, RF3).await;
    let (k1, k2) = (c.key_on(1, "a"), c.key_on(2, "b"));
    let mut conn = c.nodes[0].redis().await;
    let _: () = conn.mset(&[(&k1, "1"), (&k2, "2")]).await.unwrap();
    assert_eq!(wait(&mut conn, 2, LONG).await.0, 2);
}

#[tokio::test]
async fn an_overflowed_record_asks_whether_every_node_drained() {
    let mut args = RF3.to_vec();
    args.extend(["--write-set-keys", "4"]);
    let c = start_cluster(3, &args).await;
    let (mut conn, id) = client(&c.nodes[0]).await;
    for i in 0..5 {
        let _: () = conn.set(format!("o{i}"), "v").await.unwrap();
    }
    let overflowed = || {
        c.nodes[0]
            .node
            .conn_writes(id)
            .unwrap()
            .lock()
            .record
            .overflowed()
    };
    assert!(overflowed());
    assert_eq!(wait(&mut conn, 2, LONG).await.0, 2);
    assert!(!overflowed());

    let held = c.key_on(1, "h");
    stall(&c, &held, true);
    for i in 0..5 {
        let _: () = conn.set(c.key_on(1, &format!("h{i}")), "v").await.unwrap();
    }
    assert!(overflowed());
    let (n, took) = wait(&mut conn, 2, 300).await;
    assert_eq!(n, 0);
    assert!(took >= Duration::from_millis(300), "{took:?}");
}

#[tokio::test]
async fn a_closed_connection_ends_its_wait() {
    let c = start_cluster(3, RF3).await;
    let k = c.key_on(1, "k");
    stall(&c, &k, true);
    let mut s = common::client_tls::connect(c.nodes[0].addr).await;
    let mut buf = vec![0u8; 256];
    s.write_all(b"CLIENT ID\r\n").await.unwrap();
    s.flush().await.unwrap();
    let n = s.read(&mut buf).await.unwrap();
    let id: u64 = std::str::from_utf8(&buf[1..n])
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let wire = format!("SET {k} v\r\nWAIT 1 0\r\n");
    s.write_all(wire.as_bytes()).await.unwrap();
    s.flush().await.unwrap();
    // The reply of the `SET` waits in the output buffer behind the `WAIT`.
    let recorded = || {
        c.nodes[0]
            .node
            .conn_writes(id)
            .is_some_and(|w| !w.lock().record.is_empty())
    };
    assert!(wait_until(Duration::from_secs(20), recorded).await);
    // The client stops writing first, as a client that ends with `shutdown` does.
    s.shutdown().await.unwrap();
    drop(s);
    assert!(
        wait_until(Duration::from_secs(20), || c.nodes[0]
            .node
            .conn_writes(id)
            .is_none())
        .await,
        "the wait outlived its connection"
    );
    let mut conn = c.nodes[0].redis().await;
    let _: () = conn.set(c.key_on(2, "after"), "v").await.unwrap();
    assert_eq!(wait(&mut conn, 0, 0).await.0, 2);
}
