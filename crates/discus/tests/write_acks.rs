#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::cluster::{start_cluster, Cluster, ClusterNode};
use common::gossip::wait_until;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError, RedisResult};

const NOREPLICAS: &str = "NOREPLICAS Not enough good replicas to write.";

/// A cap that a write which waits for nothing stays far below, and that a write which waits for
/// the 60 s timeout exceeds.
const CAP: Duration = Duration::from_secs(30);

fn args<'a>(extra: &[&'a str]) -> Vec<&'a str> {
    let mut v = vec!["--replication-factor", "3"];
    v.extend_from_slice(extra);
    v
}

fn is_noreplicas(e: &RedisError) -> bool {
    format!("{} {}", e.code().unwrap_or(""), e.detail().unwrap_or("")) == NOREPLICAS
}

fn primary<'a>(c: &'a Cluster, key: &str) -> &'a ClusterNode {
    c.by_id(c.nodes[0].primary(key.as_bytes()))
}

fn secondaries<'a>(c: &'a Cluster, key: &str) -> Vec<&'a ClusterNode> {
    c.nodes[0].node.owners(key.as_bytes())[1..]
        .iter()
        .map(|id| c.by_id(*id))
        .collect()
}

/// The node that does not own `key`.
fn outsider<'a>(c: &'a Cluster, key: &str) -> &'a ClusterNode {
    let owners = c.nodes[0].node.owners(key.as_bytes());
    c.nodes.iter().find(|n| !owners.contains(&n.id)).unwrap()
}

/// The primary of `key` stops sending replication batches.
fn stall(c: &Cluster, key: &str) {
    primary(c, key)
        .node
        .faults
        .hold_replication
        .store(true, Ordering::Relaxed);
}

async fn set(conn: &mut MultiplexedConnection, key: &str, value: &str) -> RedisResult<String> {
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query_async(conn)
        .await
}

/// Write `key = v0` and wait until every owner holds it.
async fn everywhere(c: &Cluster, key: &str) {
    let mut conn = c.nodes[0].redis().await;
    assert_eq!(set(&mut conn, key, "v0").await.unwrap(), "OK");
    let owners = c.nodes[0].node.owners(key.as_bytes());
    assert!(
        wait_until(Duration::from_secs(10), || owners
            .iter()
            .all(|id| c.by_id(*id).holds(key.as_bytes())))
        .await
    );
}

#[tokio::test]
async fn a_write_returns_after_a_secondary_stores_it() {
    // A long timeout and closed mode, so a slow machine shows a timeout as `-NOREPLICAS`.
    let c = start_cluster(
        3,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "60000",
        ]),
    )
    .await;
    let k = c.key_on(0, "ack");
    everywhere(&c, &k).await;
    // A reply that does not wait comes before the slow secondaries store the write.
    for s in secondaries(&c, &k) {
        s.node.faults.stall_bulk_ms.store(500, Ordering::Relaxed);
    }
    let mut conn = c.nodes[0].redis().await;
    assert_eq!(set(&mut conn, &k, "v1").await.unwrap(), "OK");
    let v = c.nodes[0].node.store.version(0, k.as_bytes()).unwrap();
    let holders = c
        .nodes
        .iter()
        .filter(|n| n.node.store.version(0, k.as_bytes()) == Some(v))
        .count();
    assert!(holders >= 2, "{holders}");
}

#[tokio::test]
async fn open_mode_replies_ok_when_replication_stalls() {
    // The only test here that installs the global metrics. Other tests add to the counter too, so
    // the test asserts a lower bound.
    #[cfg(feature = "otel")]
    let h = {
        let h = discus_telemetry::testing::MetricHarness::new();
        assert!(discus_telemetry::install_global(h.metrics.clone()));
        h
    };
    let c = start_cluster(
        3,
        &args(&["--write-acks", "1", "--replication-timeout-ms", "300"]),
    )
    .await;
    let k = c.key_on(0, "open");
    stall(&c, &k);
    let mut conn = c.nodes[0].redis().await;
    let started = Instant::now();
    assert_eq!(set(&mut conn, &k, "v1").await.unwrap(), "OK");
    assert!(started.elapsed() >= Duration::from_millis(300));
    #[cfg(feature = "otel")]
    {
        let points = h.points();
        let open = points.iter().find(|p| {
            p.name == "discus.write.unacked"
                && p.attrs.iter().any(|(k, v)| k == "mode" && v == "open")
        });
        assert!(open.is_some_and(|p| p.value >= 1.0), "{points:?}");
    }
}

#[tokio::test]
async fn closed_mode_replies_noreplicas_when_replication_stalls() {
    let c = start_cluster(
        3,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "300",
        ]),
    )
    .await;
    let k = c.key_on(0, "closed");
    stall(&c, &k);
    let mut conn = c.nodes[0].redis().await;
    let err = set(&mut conn, &k, "v1").await.unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    let v: String = conn.get(&k).await.unwrap();
    assert_eq!(v, "v1");
}

#[tokio::test]
async fn closed_mode_fails_at_once_when_the_secondaries_are_down() {
    let c = start_cluster(
        3,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "60000",
            "--cluster-handshake-timeout-ms",
            "500",
        ]),
    )
    .await;
    let k = c.key_on(0, "down");
    let p = primary(&c, &k);
    let gone = secondaries(&c, &k);
    for s in &gone {
        s.kill();
    }
    // This write makes a batch fail. Its own reply does not matter.
    let mut first = p.redis().await;
    let k1 = k.clone();
    tokio::spawn(async move { set(&mut first, &k1, "v0").await });
    let replicator = p.node.replicator().unwrap();
    assert!(
        wait_until(CAP, || gone.iter().all(|s| replicator.peer_down(s.id))).await,
        "the secondaries never went down"
    );
    let mut conn = p.redis().await;
    let started = Instant::now();
    let err = set(&mut conn, &k, "v1").await.unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    assert!(started.elapsed() < CAP, "{:?}", started.elapsed());
}

#[tokio::test]
async fn too_few_owners_end_the_wait_at_once() {
    let c = start_cluster(
        2,
        &args(&[
            "--write-acks",
            "2",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "60000",
        ]),
    )
    .await;
    let k = c.key_on(0, "few");
    let mut conn = c.nodes[0].redis().await;
    let started = Instant::now();
    let err = set(&mut conn, &k, "v1").await.unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    assert!(started.elapsed() < CAP, "{:?}", started.elapsed());
}

#[tokio::test]
async fn a_hot_key_does_not_wait_for_the_timeout() {
    let c = start_cluster(
        3,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "60000",
        ]),
    )
    .await;
    let k = c.key_on(0, "hot");
    let p = primary(&c, &k);
    let done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    // The writers run the command on the primary without the wait, so the queues always hold
    // the key: queued or in flight. Only a stored version can end the wait of the observer.
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
    let mut conn = p.redis().await;
    for i in 0..20 {
        assert_eq!(set(&mut conn, &k, &format!("o{i}")).await.unwrap(), "OK");
    }
    let observed = started.elapsed();
    done.store(true, Ordering::Relaxed);
    for w in writers {
        let (i, n) = w.await.unwrap();
        assert!(n > 0, "writer {i} never wrote");
    }
    assert!(observed < CAP, "{observed:?}");
}

#[tokio::test]
async fn a_write_that_changes_nothing_does_not_wait() {
    let c = start_cluster(
        3,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "60000",
        ]),
    )
    .await;
    let k = c.key_on(0, "same");
    let mut conn = c.nodes[0].redis().await;
    assert_eq!(set(&mut conn, &k, "v0").await.unwrap(), "OK");
    stall(&c, &k);
    let started = Instant::now();
    let nx: Option<String> = redis::cmd("SET")
        .arg(&k)
        .arg("v1")
        .arg("NX")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(nx, None);
    let missing = c.key_on(0, "missing");
    let del: i64 = conn.del(&missing).await.unwrap();
    assert_eq!(del, 0);
    assert!(started.elapsed() < CAP, "{:?}", started.elapsed());
}

#[tokio::test]
async fn an_incr_that_gets_noreplicas_applies_once() {
    let c = start_cluster(
        4,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "300",
        ]),
    )
    .await;
    let k = c.key_on(0, "incr");
    stall(&c, &k);
    let mut conn = outsider(&c, &k).redis().await;
    let err = conn.incr::<_, _, i64>(&k, 1).await.unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    let mut on_primary = primary(&c, &k).redis().await;
    let v: String = on_primary.get(&k).await.unwrap();
    assert_eq!(v, "1");
}

#[tokio::test]
async fn a_noreplicas_write_stays_in_the_write_set() {
    let c = start_cluster(
        3,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "300",
        ]),
    )
    .await;
    let k = c.key_on(0, "ws");
    everywhere(&c, &k).await;
    let mut conn = secondaries(&c, &k)[0].redis().await;
    stall(&c, &k);
    let err = set(&mut conn, &k, "v1").await.unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    let v: String = conn.get(&k).await.unwrap();
    assert_eq!(v, "v1");
}

#[tokio::test]
async fn a_multi_key_write_waits_at_each_primary() {
    let c = start_cluster(
        4,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "300",
        ]),
    )
    .await;
    let (a, b) = (c.key_on(0, "ma"), c.key_on(1, "mb"));
    let mut conn = c.nodes[3].redis().await;
    stall(&c, &b);
    let err = conn
        .mset::<_, _, ()>(&[(&a, "a1"), (&b, "b1")])
        .await
        .unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    for (key, want) in [(&a, "a1"), (&b, "b1")] {
        let mut on_primary = primary(&c, key).redis().await;
        let v: String = on_primary.get(key).await.unwrap();
        assert_eq!(v, want);
    }
    primary(&c, &b)
        .node
        .faults
        .hold_replication
        .store(false, Ordering::Relaxed);
    let _: () = conn.mset(&[(&a, "a2"), (&b, "b2")]).await.unwrap();
}

/// Set `--write-ack-timeout` on every node, as a reload of the settings file does.
fn set_mode(c: &Cluster, mode: discus::config::AckTimeout) {
    for n in &c.nodes {
        let mut next = (**n.node.live_settings.get()).clone();
        next.write_ack_timeout = mode;
        n.node.live_settings.apply(next);
    }
}

#[tokio::test]
async fn a_multi_key_delete_waits_at_each_primary_in_both_modes() {
    let c = start_cluster(
        4,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "300",
        ]),
    )
    .await;
    let (a, b) = (c.key_on(0, "da"), c.key_on(1, "db"));
    let mut conn = c.nodes[3].redis().await;
    let _: () = conn.mset(&[(&a, "a"), (&b, "b")]).await.unwrap();
    stall(&c, &b);
    let err = conn.del::<_, i64>(&[&a, &b]).await.unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    for key in [&a, &b] {
        let mut on_primary = primary(&c, key).redis().await;
        let exists: i64 = on_primary.exists(key).await.unwrap();
        assert_eq!(exists, 0, "{key}");
    }
    set_mode(&c, discus::config::AckTimeout::Open);
    let _: () = conn.mset(&[(&a, "a"), (&b, "b")]).await.unwrap();
    let deleted: i64 = conn.del(&[&a, &b]).await.unwrap();
    assert_eq!(deleted, 2);
}

#[tokio::test]
async fn a_rename_returns_noreplicas_from_the_put() {
    let c = start_cluster(
        4,
        &args(&[
            "--write-acks",
            "1",
            "--write-ack-timeout",
            "closed",
            "--replication-timeout-ms",
            "300",
        ]),
    )
    .await;
    let (src, dst) = (c.key_on(0, "src"), c.key_on(1, "dst"));
    let mut conn = c.nodes[3].redis().await;
    assert_eq!(set(&mut conn, &src, "v").await.unwrap(), "OK");
    stall(&c, &dst);
    let err = redis::cmd("RENAME")
        .arg(&src)
        .arg(&dst)
        .query_async::<String>(&mut conn)
        .await
        .unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    let mut on_primary = primary(&c, &dst).redis().await;
    let v: String = on_primary.get(&dst).await.unwrap();
    assert_eq!(v, "v");
}

#[tokio::test]
async fn zero_write_acks_keeps_a_stalled_write_fast() {
    let c = start_cluster(3, &args(&["--replication-timeout-ms", "60000"])).await;
    let k = c.key_on(0, "zero");
    stall(&c, &k);
    let mut conn = c.nodes[0].redis().await;
    let started = Instant::now();
    assert_eq!(set(&mut conn, &k, "v1").await.unwrap(), "OK");
    assert!(started.elapsed() < CAP, "{:?}", started.elapsed());
}

#[tokio::test]
async fn a_node_without_peers_replies_noreplicas_at_once() {
    let mut conn = common::connect(&args(&[
        "--write-acks",
        "1",
        "--write-ack-timeout",
        "closed",
    ]))
    .await;
    let err = set(&mut conn, "alone", "v1").await.unwrap_err();
    assert!(is_noreplicas(&err), "{err}");
    let v: String = conn.get("alone").await.unwrap();
    assert_eq!(v, "v1");
}
