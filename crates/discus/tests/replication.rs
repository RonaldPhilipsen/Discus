#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::cluster::{start_cluster, Cluster, ClusterNode};
use common::gossip::wait_until;
use discus_placement::{Version, NTP64};
use discus_store::{DbKey, Entry, Value};
use discus_wire::{Batch, Message};
use redis::AsyncCommands;

const RF3: &[&str] = &["--replication-factor", "3"];

/// The nodes of `c` that are not the primary of `key`.
fn replicas<'a>(c: &'a Cluster, key: &str) -> Vec<&'a ClusterNode> {
    let primary = c.nodes[0].primary(key.as_bytes());
    c.nodes.iter().filter(|n| n.id != primary).collect()
}

fn version(n: &ClusterNode, key: &str) -> Option<Version> {
    n.node.store.version(0, key.as_bytes())
}

#[tokio::test]
async fn a_write_reaches_every_owner() {
    let c = start_cluster(3, RF3).await;
    let key = c.key_on(1, "rep");
    let mut client = c.nodes[0].redis().await;
    let _: () = client.set(&key, "v").await.unwrap();
    let primary = c.by_id(c.nodes[0].primary(key.as_bytes()));
    let want = version(primary, &key).unwrap();
    assert!(
        wait_until(Duration::from_secs(2), || c.nodes.iter().all(|n| version(
            n, &key
        ) == Some(
            want
        )))
        .await
    );
}

#[tokio::test]
async fn ttl_replicates_as_an_absolute_deadline() {
    let c = start_cluster(3, RF3).await;
    let key = c.key_on(0, "ttl");
    let mut client = c.nodes[0].redis().await;
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("v")
        .arg("PX")
        .arg(60_000)
        .query_async(&mut client)
        .await
        .unwrap();
    let deadline = c.nodes[0]
        .node
        .store
        .entry(0, key.as_bytes())
        .unwrap()
        .expires_at_ms;
    assert!(deadline.is_some());
    assert!(
        wait_until(Duration::from_secs(2), || c.nodes.iter().all(|n| n
            .node
            .store
            .entry(0, key.as_bytes())
            .is_some_and(|e| e.expires_at_ms == deadline)))
        .await
    );
}

#[tokio::test]
async fn a_delete_replicates_as_a_tombstone() {
    let c = start_cluster(3, RF3).await;
    let key = c.key_on(2, "del");
    let mut client = c.nodes[0].redis().await;
    let _: () = client.set(&key, "v").await.unwrap();
    let n: i64 = client.del(&key).await.unwrap();
    assert_eq!(n, 1);
    let primary = c.by_id(c.nodes[0].primary(key.as_bytes()));
    let want = version(primary, &key).unwrap();
    assert!(
        wait_until(Duration::from_secs(2), || c.nodes.iter().all(|n| {
            version(n, &key) == Some(want) && !n.holds(key.as_bytes())
        }))
        .await
    );
}

#[tokio::test]
async fn replicas_serve_reads_by_default() {
    let c = start_cluster(3, RF3).await;
    let key = c.key_on(0, "read");
    let mut client = c.nodes[1].redis().await;
    let _: () = client.set(&key, "v").await.unwrap();
    assert!(
        wait_until(Duration::from_secs(2), || c
            .nodes
            .iter()
            .all(|n| n.holds(key.as_bytes())))
        .await
    );
    c.nodes[0].kill();
    for n in &c.nodes[1..] {
        let mut client = n.redis().await;
        let v: Option<String> = client.get(&key).await.unwrap();
        assert_eq!(v.as_deref(), Some("v"));
    }
}

#[tokio::test]
async fn a_replica_miss_repairs_from_the_primary() {
    let c = start_cluster(3, RF3).await;
    let key = c.key_on(0, "repair");
    let mut client = c.nodes[0].redis().await;
    let _: () = client.set(&key, "v").await.unwrap();
    let replica = &c.nodes[1];
    assert!(wait_until(Duration::from_secs(2), || replica.holds(key.as_bytes())).await);
    replica.node.store.remove(0, key.as_bytes());
    let mut through = replica.redis().await;
    let v: Option<String> = through.get(&key).await.unwrap();
    assert_eq!(v.as_deref(), Some("v"));
    assert!(replica.holds(key.as_bytes()));
}

#[tokio::test]
async fn an_entry_too_far_ahead_is_dropped() {
    let c = start_cluster(3, RF3).await;
    let n = &c.nodes[1];
    let ahead = NTP64(discus_placement::hlc::system_clock().0 + (2u64 << 32));
    let mut e = Entry::new(Value::Str("future".into()), None);
    e.version = Version::new(ahead, c.nodes[0].id);
    let mut batch = Batch::new(n.node.epoch_id());
    batch.entries.push((DbKey::new(0, "far"), e));
    let reply = n
        .node
        .replicator()
        .unwrap()
        .handle(c.nodes[0].id, Message::Replicate(batch))
        .await;
    assert!(matches!(reply, Message::BatchAck(a) if a.applied == 0));
    assert!(n.node.store.entry(0, b"far").is_none());
    assert!(n.node.hlc.current() < ahead);
}

#[tokio::test]
async fn a_slow_replica_does_not_slow_writes() {
    let c = start_cluster(3, RF3).await;
    let key = c.key_on(0, "slow");
    for r in replicas(&c, &key) {
        r.node.faults.stall_bulk_ms.store(1_500, Ordering::Relaxed);
    }
    let mut client = c.nodes[0].redis().await;
    let started = Instant::now();
    for i in 0..100 {
        let _: () = client.set(&key, i).await.unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    for r in replicas(&c, &key) {
        r.node.faults.stall_bulk_ms.store(0, Ordering::Relaxed);
    }
    let primary = &c.nodes[0];
    let want = version(primary, &key).unwrap();
    assert!(
        wait_until(Duration::from_secs(10), || replicas(&c, &key)
            .iter()
            .all(|r| version(r, &key) == Some(want)))
        .await
    );
}
