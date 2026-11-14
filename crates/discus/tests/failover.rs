#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::cluster::start_cluster;
use common::counters::is_try_again;
use common::gossip::{wait_until, GossipNode, Net};
use redis::AsyncCommands;

async fn three(net: &Net) -> Vec<GossipNode> {
    let nodes = net
        .start_agreed(
            3,
            &[
                "--replication-factor",
                "2",
                "--forward-write-timeout-ms",
                "200",
            ],
        )
        .await;
    nodes
}

/// A key whose primary is `owner` and whose secondary is not `entry`, so the entry node has to
/// forward it.
fn key_of(nodes: &[GossipNode], owner: usize, entry: usize, prefix: &str) -> String {
    let epoch = nodes[0].node.epochs().current().clone();
    (0..)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| {
            let o = epoch.owners(k.as_bytes(), 2);
            o[0] == nodes[owner].id && !o.contains(&nodes[entry].id)
        })
        .unwrap()
}

#[test]
fn the_idempotent_list_is_exact() {
    use bytes::Bytes;
    use discus::cluster::hold::idempotent;
    let cmd = |s: &str| -> Vec<Bytes> {
        s.split(' ')
            .map(|p| Bytes::copy_from_slice(p.as_bytes()))
            .collect()
    };
    for yes in [
        "SET k v",
        "SET k v EX 10",
        "SET k v KEEPTTL",
        "DEL a b",
        "EXPIRE k 1",
        "PEXPIRE k 1",
        "EXPIREAT k 1",
        "PEXPIREAT k 1",
        "PERSIST k",
        "MSET a 1 b 2",
        "HSET h f v",
        "HDEL h f",
        "SADD s m",
        "SREM s m",
        "ZADD z 1 m",
        "ZADD z NX CH 1 incr",
        "ZREM z m",
        "DISCUS.PUT k x",
    ] {
        assert!(idempotent(&cmd(yes)), "{yes}");
    }
    for no in [
        "SET k v NX",
        "SET k v xx",
        "ZADD z INCR 1 m",
        "ZADD z NX incr 1 m",
        "INCR k",
        "APPEND k v",
        "GETDEL k",
        "SPOP s",
        "HINCRBY h f 1",
        "DISCUS.TAKE k",
    ] {
        assert!(!idempotent(&cmd(no)), "{no}");
    }
}

#[tokio::test]
async fn an_acknowledged_delete_survives_a_failover() {
    let net = Net::new();
    let nodes = net
        .start_agreed(
            3,
            &["--replication-factor", "2", "--primary-read-prefix", "sess"],
        )
        .await;
    let k = key_of(&nodes, 0, 1, "sess");
    let mut c = nodes[1].redis().await;
    let _: () = c.set(&k, "v").await.unwrap();
    let primary = nodes[0].node.clone();
    assert!(
        wait_until(Duration::from_secs(10), || primary
            .replicator()
            .unwrap()
            .settled(0, k.as_bytes()))
        .await
    );
    let del: i64 = c.del(&k).await.unwrap();
    assert_eq!(del, 1);
    nodes[0].kill();
    assert!(
        wait_until(Duration::from_secs(60), || nodes[1]
            .node
            .epochs()
            .current()
            .members()
            .len()
            == 2)
        .await
    );
    let get_after_failover: Option<String> = c.get(&k).await.unwrap();
    assert_eq!(get_after_failover, None);
}

#[tokio::test]
async fn a_held_set_succeeds_on_the_new_primary() {
    let net = Net::new();
    let nodes = three(&net).await;
    let k = key_of(&nodes, 0, 1, "held");
    let mut c = nodes[1].redis().await;
    nodes[0].kill();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _: () = c.set(&k, "v").await.unwrap();
    let v: Option<String> = c.get(&k).await.unwrap();
    assert_eq!(v.as_deref(), Some("v"));
}

#[tokio::test]
async fn an_untransmitted_incr_is_retried() {
    let net = Net::new();
    let nodes = three(&net).await;
    let k = key_of(&nodes, 0, 1, "untx");
    let mut c = nodes[1].redis().await;
    let _: () = c.set(&k, 5).await.unwrap();
    let primary = nodes[0].node.clone();
    assert!(
        wait_until(Duration::from_secs(3), || primary
            .replicator()
            .unwrap()
            .settled(0, k.as_bytes()))
        .await
    );
    nodes[0].kill();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let n: i64 = c.incr(&k, 1).await.unwrap();
    assert_eq!(n, 6);
}

#[tokio::test]
async fn a_held_write_keeps_same_key_order() {
    let net = Net::new();
    let nodes = three(&net).await;
    let k = key_of(&nodes, 0, 1, "order");
    let mut c = nodes[1].redis().await;
    nodes[0].kill();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let ((), v): ((), String) = redis::pipe()
        .set(&k, "v2")
        .get(&k)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(v, "v2");
}

#[tokio::test]
async fn a_failed_write_reports_evidence() {
    let net = Net::new();
    let nodes = three(&net).await;
    let k = key_of(&nodes, 0, 1, "evid");
    let mut c = nodes[1].redis().await;
    let _: () = c.set(&k, "v").await.unwrap();
    let mut events = nodes[1].node.link().unwrap().transport.events();
    nodes[0].kill();
    let _: () = c.set(&k, "w").await.unwrap();
    let dead = nodes[0].id;
    let seen = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(e) = events.recv().await {
                if e.peer == Some(dead) {
                    return true;
                }
            }
        }
    })
    .await;
    assert_eq!(seen, Ok(true));
}

#[tokio::test]
async fn a_transmitted_incr_returns_tryagain() {
    let c = start_cluster(
        2,
        &[
            "--forward-write-timeout-ms",
            "200",
            "--hold-budget-ms",
            "1000",
        ],
    )
    .await;
    let k = c.key_on(0, "tx");
    c.nodes[0]
        .node
        .faults
        .stall_forwards
        .store(true, Ordering::Relaxed);
    let mut client = c.nodes[1].redis().await;
    let started = Instant::now();
    let err = client.incr::<_, _, i64>(&k, 1).await.unwrap_err();
    assert!(is_try_again(&err), "{err}");
    assert!(started.elapsed() < Duration::from_secs(1));
    let started = Instant::now();
    let err = client.set::<_, _, ()>(&k, "v").await.unwrap_err();
    assert!(is_try_again(&err), "{err}");
    assert!(started.elapsed() >= Duration::from_millis(900));
}

#[tokio::test]
async fn the_hold_budget_expires() {
    let c = start_cluster(
        2,
        &[
            "--hold-budget-ms",
            "1000",
            "--cluster-handshake-timeout-ms",
            "200",
        ],
    )
    .await;
    let k = c.key_on(0, "budget");
    let mut client = c.nodes[1].redis().await;
    c.nodes[0].kill();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    let err = client.set::<_, _, ()>(&k, "v").await.unwrap_err();
    assert!(is_try_again(&err), "{err}");
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(900) && took < Duration::from_secs(2),
        "{took:?}"
    );
}
