#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::cluster::{start_cluster, Cluster, ClusterNode};
use common::counters::is_try_again;
use common::gossip::wait_until;
use redis::AsyncCommands;

const ARGS: &[&str] = &[
    "--replication-factor",
    "3",
    "--replication-timeout-ms",
    "300",
    "--primary-read-prefix",
    "sess",
];

/// The node that does not own `key`.
fn outsider<'a>(c: &'a Cluster, key: &str) -> &'a ClusterNode {
    let owners = c.nodes[0].node.owners(key.as_bytes());
    c.nodes.iter().find(|n| !owners.contains(&n.id)).unwrap()
}

/// Write `key = v0` everywhere, then make every replica of `key` stop taking replication.
async fn stale_replicas(c: &Cluster, key: &str) {
    let mut client = c.nodes[0].redis().await;
    let _: () = client.set(key, "v0").await.unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || c
            .nodes
            .iter()
            .filter(|n| n.holds(key.as_bytes()))
            .count()
            == 3)
        .await
    );
    let primary = c.nodes[0].primary(key.as_bytes());
    for n in c.nodes.iter().filter(|n| n.id != primary) {
        n.node.faults.stall_bulk_ms.store(2_000, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn a_new_connection_reads_the_latest_write_of_a_listed_key() {
    let c = start_cluster(4, ARGS).await;
    let k = c.key_on(0, "sess");
    stale_replicas(&c, &k).await;
    let entry = outsider(&c, &k);
    let mut writer = entry.redis().await;
    let _: () = writer.set(&k, "v1").await.unwrap();
    for _ in 0..20 {
        let mut other = entry.redis().await;
        let v: String = other.get(&k).await.unwrap();
        assert_eq!(v, "v1");
    }
}

#[tokio::test]
async fn exists_on_a_new_connection_sees_a_listed_key() {
    let c = start_cluster(4, ARGS).await;
    let k = c.key_on(0, "sess");
    stale_replicas(&c, &k).await;
    let entry = outsider(&c, &k);
    let mut writer = entry.redis().await;
    let _: () = writer.del(&k).await.unwrap();
    for _ in 0..20 {
        let mut other = entry.redis().await;
        let exists: i64 = other.exists(&k).await.unwrap();
        assert_eq!(exists, 0);
    }
}

#[tokio::test]
async fn a_listed_key_does_not_enter_the_write_set() {
    let c = start_cluster(4, ARGS).await;
    let k = c.key_on(0, "sess");
    let entry = outsider(&c, &k);
    let mut conn = entry.redis().await;
    let _: () = conn.set(&k, "v1").await.unwrap();
    let _: () = conn.mset(&[(&k, "v2")]).await.unwrap();
    assert_eq!(entry.node.write_set_bytes(), 0);
}

#[tokio::test]
async fn an_unreachable_primary_gives_try_again_for_a_listed_key() {
    let c = start_cluster(4, ARGS).await;
    let k = c.key_on(0, "sess");
    let entry = outsider(&c, &k);
    let mut conn = entry.redis().await;
    let _: () = conn.set(&k, "v1").await.unwrap();
    c.nodes[0]
        .node
        .faults
        .stall_forwards
        .store(true, Ordering::Relaxed);
    let err = conn.get::<_, String>(&k).await.unwrap_err();
    assert!(is_try_again(&err), "{err}");
}

#[tokio::test]
async fn an_empty_prefix_stops_the_node() {
    assert!(common::try_start_full(&["--primary-read-prefix", ""])
        .await
        .is_err());
}

/// A long wait for a secondary, so a reply that comes before the acknowledgment cannot pass.
const WAITING: &[&str] = &[
    "--replication-factor",
    "3",
    "--replication-timeout-ms",
    "10000",
    "--forward-write-timeout-ms",
    "2000",
    "--primary-read-prefix",
    "sess",
];

/// The secondaries take this long to apply a batch. A reply that does not wait for them comes
/// first.
const SLOW_MS: u64 = 500;

/// Write `key` and wait until every owner acknowledged it, so that only the next write is in
/// flight.
async fn settled(c: &Cluster, key: &str) {
    let mut client = c.nodes[0].redis().await;
    let _: () = client.set(key, "v0").await.unwrap();
    let primary = c.by_id(c.nodes[0].primary(key.as_bytes()));
    assert!(
        wait_until(Duration::from_secs(10), || primary
            .node
            .replicator()
            .unwrap()
            .settled(key.as_bytes()))
        .await
    );
}

fn secondaries<'a>(c: &'a Cluster, key: &str) -> Vec<&'a ClusterNode> {
    c.nodes[0].node.owners(key.as_bytes())[1..]
        .iter()
        .map(|id| c.by_id(*id))
        .collect()
}

fn stall(nodes: &[&ClusterNode], ms: u64) {
    for n in nodes {
        n.node.faults.stall_bulk_ms.store(ms, Ordering::Relaxed);
    }
}

fn any_tombstone(nodes: &[&ClusterNode], key: &str) -> bool {
    nodes.iter().any(|n| {
        n.node
            .store
            .entry(key.as_bytes())
            .is_some_and(|e| e.tombstone)
    })
}

#[tokio::test]
async fn a_delete_of_a_listed_key_reaches_a_secondary_before_the_reply() {
    let c = start_cluster(4, WAITING).await;
    let k = c.key_on(0, "sess");
    settled(&c, &k).await;
    let replicas = secondaries(&c, &k);
    stall(&replicas, SLOW_MS);
    let mut conn = outsider(&c, &k).redis().await;
    let del: i64 = conn.del(&k).await.unwrap();
    assert_eq!(del, 1);
    assert!(any_tombstone(&replicas, &k));
}

#[tokio::test]
async fn a_delete_fails_open_when_no_secondary_answers() {
    // The forward timeout is shorter than the wait, so the delete only succeeds if the forward
    // allows for the wait. The margin between them absorbs a slow machine.
    let args = &[
        "--replication-factor",
        "3",
        "--replication-timeout-ms",
        "3000",
        "--forward-write-timeout-ms",
        "1500",
        "--primary-read-prefix",
        "sess",
    ];
    let c = start_cluster(4, args).await;
    let k = c.key_on(0, "sess");
    settled(&c, &k).await;
    let replicas = secondaries(&c, &k);
    stall(&replicas, 60_000);
    let mut conn = outsider(&c, &k).redis().await;
    let del: i64 = conn.del(&k).await.unwrap();
    assert_eq!(del, 1);
    let replicator = c.nodes[0].node.replicator().unwrap();
    for r in &replicas {
        assert!(wait_until(Duration::from_secs(20), || replicator.hints(r.id) == 1).await);
    }
}

#[tokio::test]
async fn a_delete_on_a_key_with_no_secondary_does_not_wait() {
    let args = &[
        "--replication-factor",
        "1",
        "--replication-timeout-ms",
        "20000",
        "--primary-read-prefix",
        "sess",
    ];
    let c = start_cluster(2, args).await;
    let k = c.key_on(0, "sess");
    let mut conn = c.nodes[1].redis().await;
    let _: () = conn.set(&k, "v0").await.unwrap();
    stall(&[&c.nodes[0], &c.nodes[1]], 60_000);
    let started = Instant::now();
    let del: i64 = conn.del(&k).await.unwrap();
    assert_eq!(del, 1);
    assert!(started.elapsed() < Duration::from_secs(15));
    let exists: i64 = conn.exists(&k).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn getdel_and_rename_also_wait_for_a_secondary() {
    let c = start_cluster(4, WAITING).await;

    let k = c.key_on(0, "sess");
    settled(&c, &k).await;
    let replicas = secondaries(&c, &k);
    stall(&replicas, SLOW_MS);
    let mut conn = outsider(&c, &k).redis().await;
    let got: String = redis::cmd("GETDEL")
        .arg(&k)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(got, "v0");
    let secondary_has_tombstone_for_getdel = any_tombstone(&replicas, &k);
    stall(&replicas, 0);

    let src = c.key_on(1, "sessrename");
    let dst = format!("{src}:dst");
    settled(&c, &src).await;
    let replicas = secondaries(&c, &src);
    stall(&replicas, SLOW_MS);
    let mut conn = outsider(&c, &src).redis().await;
    let _: () = conn.rename(&src, &dst).await.unwrap();
    let secondary_has_tombstone_for_rename_source = any_tombstone(&replicas, &src);

    assert!(secondary_has_tombstone_for_getdel && secondary_has_tombstone_for_rename_source);
}
