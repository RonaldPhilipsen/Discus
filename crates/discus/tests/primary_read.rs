#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

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
