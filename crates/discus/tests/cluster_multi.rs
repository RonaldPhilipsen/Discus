#![allow(clippy::unwrap_used)]

mod common;

use std::time::{Duration, Instant};

use common::cluster::{start_cluster, start_cluster_with};
use discus_store::{Entry, Value as Stored};
use redis::AsyncCommands;

fn shuffled(mut v: Vec<String>, seed: u64) -> Vec<String> {
    let mut rng = fastrand::Rng::with_seed(seed);
    rng.shuffle(&mut v);
    v
}

#[tokio::test]
async fn mget_merges_in_request_order() {
    let c = start_cluster(3, &[]).await;
    let mut conns = Vec::new();
    for n in &c.nodes {
        conns.push(n.redis().await);
    }
    let keys: Vec<String> = (0..50).map(|i| format!("mget:{i}")).collect();
    for (i, k) in keys.iter().enumerate() {
        let _: () = conns[i % 3].set(k, format!("v:{k}")).await.unwrap();
    }
    let owners: std::collections::HashSet<_> = keys
        .iter()
        .map(|k| c.nodes[0].primary(k.as_bytes()))
        .collect();
    assert_eq!(owners.len(), 3, "the keys span every node");
    for (n, conn) in conns.iter_mut().enumerate() {
        let mut ask = shuffled(keys.clone(), n as u64);
        ask.insert(7, "mget:missing:a".into());
        ask.push("mget:missing:b".into());
        let got: Vec<Option<String>> = conn.mget(&ask).await.unwrap();
        let want: Vec<Option<String>> = ask
            .iter()
            .map(|k| (!k.contains("missing")).then(|| format!("v:{k}")))
            .collect();
        assert_eq!(got, want);
    }
}

#[tokio::test]
async fn mset_and_del_fan_out() {
    let c = start_cluster(3, &[]).await;
    let mut a = c.nodes[0].redis().await;
    let pairs: Vec<(String, String)> = (0..30)
        .map(|i| (format!("ms:{i}"), format!("{i}")))
        .collect();
    let _: () = a.mset(&pairs).await.unwrap();
    for (k, _) in &pairs {
        let primary = c.nodes[0].primary(k.as_bytes());
        assert!(c.by_id(primary).holds(k.as_bytes()), "{k}");
    }
    let five: Vec<&String> = pairs.iter().take(5).map(|(k, _)| k).collect();
    let mut exists = redis::cmd("EXISTS");
    for k in &five {
        exists.arg(k.as_str());
    }
    exists.arg(five[0].as_str());
    let n: i64 = exists.query_async(&mut a).await.unwrap();
    assert_eq!(n, 6);
    let mut keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
    keys.push("ms:missing".into());
    let removed: i64 = a.del(&keys).await.unwrap();
    assert_eq!(removed, 30);
    assert!(c
        .nodes
        .iter()
        .all(|n| pairs.iter().all(|(k, _)| !n.holds(k.as_bytes()))));
}

#[tokio::test]
async fn mset_partial_failure_returns_tryagain() {
    let c = start_cluster(
        3,
        &[
            "--cluster-handshake-timeout-ms",
            "300",
            "--forward-write-timeout-ms",
            "500",
            // The failed part holds for a new primary first, which never comes here.
            "--hold-budget-ms",
            "500",
        ],
    )
    .await;
    let mut a = c.nodes[0].redis().await;
    let on_a = c.key_on(0, "pa");
    let on_c = c.key_on(2, "pc");
    c.nodes[2].kill();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let start = Instant::now();
    let err = a
        .mset::<_, _, ()>(&[(on_a.as_str(), "1"), (on_c.as_str(), "2")])
        .await
        .unwrap_err();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );
    assert_eq!(err.code(), Some("TRYAGAIN"));
    assert!(c.nodes[0].holds(on_a.as_bytes()));
}

#[tokio::test]
async fn a_forward_to_a_stopped_node_returns_tryagain() {
    let c = start_cluster(
        3,
        &[
            "--cluster-handshake-timeout-ms",
            "300",
            "--forward-read-timeout-ms",
            "500",
        ],
    )
    .await;
    let mut a = c.nodes[0].redis().await;
    let on_c = c.key_on(2, "gone");
    let mut events = c.nodes[0].node.link().unwrap().transport.events();
    c.nodes[2].kill();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let start = Instant::now();
    let err = a.get::<_, Option<String>>(&on_c).await.unwrap_err();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );
    assert_eq!(err.code(), Some("TRYAGAIN"));
    let mut saw = false;
    while let Ok(event) = events.try_recv() {
        saw |= event.peer == Some(c.nodes[2].id);
    }
    assert!(saw, "no evidence about the stopped node");
}

#[tokio::test]
async fn dbsize_sums_the_members_and_flush_broadcasts() {
    let c = start_cluster(3, &[]).await;
    let mut a = c.nodes[0].redis().await;
    let mut b = c.nodes[1].redis().await;
    let pairs: Vec<(String, i32)> = (0..30).map(|i| (format!("db:{i}"), i)).collect();
    let _: () = a.mset(&pairs).await.unwrap();
    for n in &c.nodes {
        let mut conn = n.redis().await;
        let size: i64 = redis::cmd("DBSIZE").query_async(&mut conn).await.unwrap();
        assert_eq!(size, 30);
    }
    let _: () = redis::cmd("FLUSHALL").query_async(&mut a).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut b).await.unwrap();
    assert_eq!(size, 0);
}

#[tokio::test]
async fn scan_and_randomkey_stay_local() {
    let c = start_cluster(3, &[]).await;
    let mut a = c.nodes[0].redis().await;
    let pairs: Vec<(String, i32)> = (0..60).map(|i| (format!("scan:{i}"), i)).collect();
    let _: () = a.mset(&pairs).await.unwrap();
    let (_, keys): (String, Vec<String>) = redis::cmd("SCAN")
        .arg(0)
        .arg("COUNT")
        .arg(1000)
        .query_async(&mut a)
        .await
        .unwrap();
    assert!(!keys.is_empty());
    assert!(keys.len() < 60);
    assert!(keys.iter().all(|k| c.nodes[0].holds(k.as_bytes())));
    let random: String = redis::cmd("RANDOMKEY").query_async(&mut a).await.unwrap();
    assert!(c.nodes[0].holds(random.as_bytes()));
}

#[tokio::test]
async fn any_node_serves_any_key_with_redis_rs() {
    let c = start_cluster(3, &[]).await;
    let mut conns = Vec::new();
    for n in &c.nodes {
        conns.push(n.redis().await);
    }
    let keys: Vec<String> = (0..300).map(|i| format!("any:{i}")).collect();
    for (i, k) in keys.iter().enumerate() {
        let _: () = conns[i % 3].set(k, i).await.unwrap();
        let got: usize = conns[(i + 1) % 3].get(k).await.unwrap();
        assert_eq!(got, i);
    }
    let all: Vec<usize> = conns[2].mget(&keys).await.unwrap();
    assert_eq!(all, (0..300).collect::<Vec<_>>());
}

#[tokio::test]
async fn read_from_replicas_falls_over_to_the_next_owner() {
    let c = start_cluster_with(3, |_| {
        vec![
            "--replication-factor".into(),
            "2".into(),
            "--read-from-replicas".into(),
            "--forward-read-timeout-ms".into(),
            "200".into(),
            "--cluster-handshake-timeout-ms".into(),
            "200".into(),
        ]
    })
    .await;
    // A key that the third node does not own, read through the third node.
    let (key, entry) = (0..)
        .map(|i| format!("rr:{i}"))
        .find_map(|k| {
            let owners = c.nodes[0].node.owners(k.as_bytes());
            let outside = c.nodes.iter().position(|n| !owners.contains(&n.id))?;
            Some((k, outside))
        })
        .unwrap();
    let owners = c.nodes[0].node.owners(key.as_bytes());
    let (down, live) = (c.by_id(owners[0]), c.by_id(owners[1]));
    let stamp = live.node.stamp().unwrap();
    live.node.store.write(0, key.as_bytes(), stamp, |slot| {
        slot.set(Some(Entry::new(Stored::Str("from-replica".into()), None)))
    });
    down.kill();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut conn = c.nodes[entry].redis().await;
    for _ in 0..4 {
        let got: String = conn.get(&key).await.unwrap();
        assert_eq!(got, "from-replica");
    }
}

#[tokio::test]
async fn dbsize_divides_by_the_replication_factor() {
    let c = start_cluster(3, &["--replication-factor", "3"]).await;
    let mut a = c.nodes[0].redis().await;
    for i in 0..30 {
        let _: () = a.set(format!("rf{i}"), i).await.unwrap();
    }
    assert!(
        common::gossip::wait_until(Duration::from_secs(3), || c.nodes.iter().all(|n| n
            .node
            .store
            .len()
            == 30))
        .await
    );
    for n in &c.nodes {
        let mut conn = n.redis().await;
        let size: i64 = redis::cmd("DBSIZE").query_async(&mut conn).await.unwrap();
        assert_eq!(size, 30);
    }
}

#[tokio::test]
async fn a_flush_clears_every_owner() {
    let c = start_cluster(
        3,
        &[
            "--replication-factor",
            "3",
            "--replication-timeout-ms",
            "200",
        ],
    )
    .await;
    let mut a = c.nodes[0].redis().await;
    // One replica stops taking replication, so the primary holds hints for it.
    c.nodes[2]
        .node
        .faults
        .stall_bulk_ms
        .store(60_000, std::sync::atomic::Ordering::Relaxed);
    let keys: Vec<String> = (0..20).map(|i| c.key_on(0, &format!("fl{i}"))).collect();
    for k in &keys {
        let _: () = a.set(k, "v").await.unwrap();
    }
    let rep = c.nodes[0].node.replicator().unwrap();
    assert!(
        common::gossip::wait_until(Duration::from_secs(3), || rep.hints(c.nodes[2].id) > 0).await
    );
    let _: () = redis::cmd("FLUSHALL").query_async(&mut a).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    for n in &c.nodes {
        assert_eq!(n.node.store.len(), 0);
    }
    assert_eq!(rep.hints(c.nodes[2].id), 0);
}
