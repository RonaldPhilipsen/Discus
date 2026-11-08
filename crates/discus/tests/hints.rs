#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::cluster::{start_cluster, ClusterNode};
use common::gossip::{wait_agreed, wait_until, GossipNode, Net};
use discus_store::Value;
use redis::AsyncCommands;

const OUTAGE: &[&str] = &[
    "--replication-factor",
    "3",
    "--replication-timeout-ms",
    "200",
    "--replication-retry-ms",
    "100",
];

fn value_of(n: &ClusterNode, key: &str) -> Option<String> {
    n.node
        .store
        .read(key.as_bytes(), |e| match e.map(|e| &e.value) {
            Some(Value::Str(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
}

fn hints(n: &ClusterNode, target: &ClusterNode) -> usize {
    n.node.replicator().unwrap().hints(target.id)
}

#[tokio::test]
async fn hints_replay_after_an_outage() {
    let c = start_cluster(3, OUTAGE).await;
    let (p, r) = (&c.nodes[0], &c.nodes[1]);
    let keys: Vec<String> = (0..50).map(|i| c.key_on(0, &format!("out{i}"))).collect();
    r.node.faults.stall_bulk_ms.store(3_000, Ordering::Relaxed);
    let mut client = p.redis().await;
    for k in &keys {
        let _: () = client.set(k, "v").await.unwrap();
    }
    for k in &keys[..10] {
        let _: i64 = client.del(k).await.unwrap();
    }
    assert!(wait_until(Duration::from_secs(2), || hints(p, r) > 0).await);
    r.node.faults.stall_bulk_ms.store(0, Ordering::Relaxed);
    assert!(
        wait_until(Duration::from_secs(8), || {
            hints(p, r) == 0
                && keys[10..]
                    .iter()
                    .all(|k| value_of(r, k).as_deref() == Some("v"))
                && keys[..10].iter().all(|k| {
                    r.node
                        .store
                        .entry(k.as_bytes())
                        .is_some_and(|e| e.tombstone)
                })
        })
        .await
    );
}

#[tokio::test]
async fn hints_replay_after_a_lagging_queue_drains() {
    let mut args = OUTAGE.to_vec();
    args.extend([
        "--replication-queue-keys",
        "8",
        "--replication-batch-keys",
        "4",
    ]);
    let c = start_cluster(3, &args).await;
    let (p, r) = (&c.nodes[0], &c.nodes[1]);
    r.node.faults.stall_bulk_ms.store(50, Ordering::Relaxed);
    let keys: Vec<String> = (0..200).map(|i| c.key_on(0, &format!("lag{i}"))).collect();
    let mut client = p.redis().await;
    for k in &keys {
        let _: () = client.set(k, "v").await.unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(20), || keys.iter().all(|k| r
            .node
            .store
            .version(k.as_bytes())
            == p.node.store.version(k.as_bytes())))
        .await
    );
}

#[tokio::test]
async fn a_lost_hint_makes_the_target_discard_the_key() {
    let mut args = OUTAGE.to_vec();
    args.extend(["--hint-max-entries", "4"]);
    let c = start_cluster(3, &args).await;
    let (p, r) = (&c.nodes[0], &c.nodes[1]);
    let keys: Vec<String> = (0..20).map(|i| c.key_on(0, &format!("lost{i}"))).collect();
    let mut client = p.redis().await;
    for k in &keys {
        let _: () = client.set(k, "old").await.unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(3), || keys.iter().all(|k| value_of(
            r, k
        )
        .as_deref()
            == Some("old")))
        .await
    );
    r.node.faults.stall_bulk_ms.store(3_000, Ordering::Relaxed);
    for k in &keys {
        let _: () = client.set(k, "new").await.unwrap();
    }
    assert!(wait_until(Duration::from_secs(2), || hints(p, r) == 4).await);
    r.node.faults.stall_bulk_ms.store(0, Ordering::Relaxed);
    assert!(wait_until(Duration::from_secs(8), || hints(p, r) == 0).await);
    for k in &keys {
        assert_ne!(
            value_of(r, k).as_deref(),
            Some("old"),
            "{k} kept a stale value"
        );
    }
}

async fn tombstone_overflow() -> (common::cluster::Cluster, String) {
    let c = start_cluster(
        1,
        &[
            "--max-memory",
            "100000",
            "--tombstone-reserve-percent",
            "1",
            "--tombstone-ttl-ms",
            "1000",
        ],
    )
    .await;
    let mut client = c.nodes[0].redis().await;
    for i in 0..50 {
        let _: () = client.set(format!("t{i}"), "v").await.unwrap();
        let _: i64 = client.del(format!("t{i}")).await.unwrap();
    }
    assert!(!c.nodes[0].node.store.needs_refill().is_empty());
    let kept = (0..)
        .map(|i| format!("keep{i}"))
        .find(|k| {
            let s = &c.nodes[0].node.store;
            !s.needs_refill().contains(&s.shard_index(k.as_bytes()))
        })
        .unwrap();
    let _: () = client.set(&kept, "v").await.unwrap();
    (c, kept)
}

#[tokio::test]
async fn a_short_absence_keeps_the_store() {
    let (c, kept) = tombstone_overflow().await;
    let n = &c.nodes[0];
    n.node
        .replicator()
        .unwrap()
        .rejoining(Duration::from_millis(10));
    assert!(n.node.store.needs_refill().is_empty());
    assert!(n.holds(kept.as_bytes()));
}

#[tokio::test]
async fn a_long_absence_discards_the_store() {
    let (c, kept) = tombstone_overflow().await;
    let n = &c.nodes[0];
    n.node
        .replicator()
        .unwrap()
        .rejoining(Duration::from_millis(1_500));
    assert!(!n.holds(kept.as_bytes()));
    assert_eq!(n.node.store.len(), 0);
}

async fn gossip_three(net: &Net, extra: &[&str]) -> Vec<GossipNode> {
    let mut args = vec!["--replication-factor", "3"];
    args.extend_from_slice(extra);
    let nodes = net.start_agreed(3, &args).await;
    nodes
}

#[tokio::test]
async fn a_promoted_primary_pulls_newer_entries() {
    let net = Net::new();
    let nodes = gossip_three(&net, &[]).await;
    let epoch = nodes[0].node.epochs().current().clone();
    let (key, owners) = (0..)
        .map(|i| format!("promo{i}"))
        .map(|k| {
            let o = epoch.owners(k.as_bytes(), 3);
            (k, o)
        })
        .next()
        .unwrap();
    let by = |id| nodes.iter().find(|n| n.id == id).unwrap();
    let (p, x) = (by(owners[0]), by(owners[1]));
    let mut client = p.redis().await;
    let _: () = client.set(&key, "v2").await.unwrap();
    let v2 = p.node.store.version(key.as_bytes()).unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || nodes.iter().all(|n| n
            .node
            .store
            .version(key.as_bytes())
            == Some(v2)))
        .await
    );
    let mut old = x.node.store.entry(key.as_bytes()).unwrap();
    old.value = Value::Str("v1".into());
    old.version =
        discus_placement::Version::new(discus_placement::NTP64(v2.time().0 - 1), v2.origin());
    x.node.store.remove(key.as_bytes());
    assert!(x.node.store.apply(key.as_bytes(), old));
    p.kill();
    let rest: Vec<&GossipNode> = nodes.iter().filter(|n| n.id != p.id).collect();
    wait_agreed(&rest, Duration::from_secs(10)).await;
    assert_eq!(
        x.node.epochs().current().primary(key.as_bytes()),
        Some(x.id)
    );
    assert!(
        wait_until(Duration::from_secs(5), || x
            .node
            .store
            .version(key.as_bytes())
            == Some(v2))
        .await
    );
}

#[tokio::test]
async fn hints_for_a_dead_target_are_dropped() {
    let net = Net::new();
    let nodes = gossip_three(&net, &["--replication-timeout-ms", "200"]).await;
    let (p, z) = (&nodes[0], &nodes[2]);
    let epoch = p.node.epochs().current().clone();
    let keys: Vec<String> = (0..)
        .map(|i| format!("dead{i}"))
        .filter(|k| epoch.primary(k.as_bytes()) == Some(p.id))
        .take(20)
        .collect();
    z.kill();
    let mut client = p.redis().await;
    for k in &keys {
        let _: () = client.set(k, "v").await.unwrap();
    }
    let rep = p.node.replicator().unwrap();
    assert!(wait_until(Duration::from_secs(3), || rep.hints(z.id) > 0).await);
    let rest = [&nodes[0], &nodes[1]];
    wait_agreed(&rest, Duration::from_secs(10)).await;
    assert!(wait_until(Duration::from_secs(2), || rep.hints(z.id) == 0).await);
}
