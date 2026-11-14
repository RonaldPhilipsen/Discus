#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::cluster::{start_cluster, Cluster, ClusterNode};
use common::gossip::wait_until;
use redis::AsyncCommands;

const ARGS: &[&str] = &[
    "--replication-factor",
    "3",
    "--replication-timeout-ms",
    "300",
];

/// The node that does not own `key`.
fn outsider<'a>(c: &'a Cluster, key: &str) -> &'a ClusterNode {
    let owners = c.nodes[0].node.owners(key.as_bytes());
    c.nodes.iter().find(|n| !owners.contains(&n.id)).unwrap()
}

/// Write `key = v0` everywhere, then make every replica of `key` stop taking replication.
async fn stale_replicas(c: &Cluster, keys: &[&str]) {
    let mut client = c.nodes[0].redis().await;
    for k in keys {
        let _: () = client.set(*k, "v0").await.unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(3), || keys.iter().all(|k| {
            c.nodes.iter().filter(|n| n.holds(k.as_bytes())).count() == 3
        }))
        .await
    );
    for k in keys {
        let primary = c.nodes[0].primary(k.as_bytes());
        for n in c.nodes.iter().filter(|n| n.id != primary) {
            n.node.faults.stall_bulk_ms.store(2_000, Ordering::Relaxed);
        }
    }
}

fn key(c: &Cluster) -> String {
    c.key_on(0, "ws")
}

#[tokio::test]
async fn set_then_get_on_one_connection_reads_the_write() {
    let c = start_cluster(4, ARGS).await;
    let k = key(&c);
    stale_replicas(&c, &[&k]).await;
    let mut conn = outsider(&c, &k).redis().await;
    for i in 1..=20 {
        let _: () = conn.set(&k, format!("v{i}")).await.unwrap();
        let v: String = conn.get(&k).await.unwrap();
        assert_eq!(v, format!("v{i}"));
    }
}

#[tokio::test]
async fn another_connection_can_read_a_stale_value() {
    let c = start_cluster(4, ARGS).await;
    let k = key(&c);
    stale_replicas(&c, &[&k]).await;
    let entry = outsider(&c, &k);
    let mut writer = entry.redis().await;
    let _: () = writer.set(&k, "v1").await.unwrap();
    let mut other = entry.redis().await;
    let mut seen = Vec::new();
    for _ in 0..6 {
        let v: String = other.get(&k).await.unwrap();
        seen.push(v);
    }
    assert!(seen.iter().any(|v| v == "v0"), "{seen:?}");
}

#[tokio::test]
async fn a_reconnect_loses_the_write_set() {
    let c = start_cluster(4, ARGS).await;
    let k = key(&c);
    stale_replicas(&c, &[&k]).await;
    let entry = outsider(&c, &k);
    {
        let mut first = entry.redis().await;
        let _: () = first.set(&k, "v1").await.unwrap();
    }
    let mut again = entry.redis().await;
    let mut seen = Vec::new();
    for _ in 0..6 {
        let v: String = again.get(&k).await.unwrap();
        seen.push(v);
    }
    assert!(seen.iter().any(|v| v == "v0"), "{seen:?}");
}

#[tokio::test]
async fn multi_key_commands_use_the_write_set() {
    let c = start_cluster(4, ARGS).await;
    let a = c.key_on(0, "wsa");
    let b = c.key_on(1, "wsb");
    stale_replicas(&c, &[&a, &b]).await;
    let entry = c
        .nodes
        .iter()
        .find(|n| {
            !c.nodes[0].node.owners(a.as_bytes()).contains(&n.id)
                || !c.nodes[0].node.owners(b.as_bytes()).contains(&n.id)
        })
        .unwrap();
    let mut conn = entry.redis().await;
    for round in 1..=5 {
        let v = format!("v{round}");
        let _: () = conn.mset(&[(&a, &v), (&b, &v)]).await.unwrap();
        let got: Vec<String> = conn.mget(&[&a, &b]).await.unwrap();
        assert_eq!(got, vec![v.clone(), v]);
    }
}

#[tokio::test]
async fn the_write_set_lets_go_once_replicas_catch_up() {
    let c = start_cluster(4, ARGS).await;
    let k = key(&c);
    let entry = outsider(&c, &k);
    let mut conn = entry.redis().await;
    let _: () = conn.set(&k, "v1").await.unwrap();
    assert!(entry.node.write_set_bytes() > 0);
    let primary = c.by_id(c.nodes[0].primary(k.as_bytes()));
    let want = primary.node.store.version(0, k.as_bytes());
    assert!(
        wait_until(Duration::from_secs(3), || c
            .nodes
            .iter()
            .filter(|n| n.node.store.version(0, k.as_bytes()) == want)
            .count()
            == 3)
        .await
    );
    assert!(
        wait_until(Duration::from_secs(3), || primary
            .node
            .replicator()
            .unwrap()
            .settled(k.as_bytes()))
        .await
    );
    let v: String = conn.get(&k).await.unwrap();
    assert_eq!(v, "v1");
    assert_eq!(entry.node.write_set_bytes(), 0);
}

#[tokio::test]
async fn a_single_owner_cluster_records_no_write_set() {
    let c = start_cluster(3, &["--replication-factor", "1"]).await;
    let mut conn = c.nodes[0].redis().await;
    for i in 0..20 {
        let k = format!("solo:{i}");
        let _: () = conn.set(&k, "v").await.unwrap();
        let v: String = conn.get(&k).await.unwrap();
        assert_eq!(v, "v");
    }
    assert_eq!(c.nodes[0].node.write_set_bytes(), 0);
}
