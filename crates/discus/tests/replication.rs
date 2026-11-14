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

/// A node holds only its own databases. Entries and offers of another database change nothing,
/// so a peer cannot create maps here for a database that this node does not have. Such an offer
/// is wanted, because this node does not hold the key.
#[tokio::test]
async fn a_node_ignores_databases_that_it_does_not_hold() {
    let c = start_cluster(1, &[]).await;
    let (n, id) = (&c.nodes[0].node, c.nodes[0].id);
    let r = n.replicator().unwrap().clone();
    let stamp = n.stamp().unwrap();
    let entry = Entry {
        version: stamp,
        ..Entry::new(Value::Str("v".into()), None)
    };
    let (held, other) = (DbKey::new(0, &b"k"[..]), DbKey::new(1, &b"k"[..]));
    let entries = vec![(held.clone(), entry.clone()), (other.clone(), entry)];
    assert_eq!(r.apply_entries(n, entries), 1);
    assert_eq!(n.store.databases(), vec![0]);

    let mut offer = Batch::new(n.epoch_id());
    offer.offers = vec![
        (DbKey::new(0, &b"x"[..]), stamp),
        (DbKey::new(1, &b"x"[..]), stamp),
    ];
    let Message::BatchAck(ack) = r.handle(id, Message::Handoff(offer)).await else {
        panic!("a handoff gets an acknowledgement");
    };
    // The offer of database 1 stays wanted, so a source that drops keys after a handoff keeps
    // its copy. Wanting it creates no map.
    assert_eq!(
        ack.want,
        vec![DbKey::new(0, &b"x"[..]), DbKey::new(1, &b"x"[..])]
    );
    assert_eq!(n.store.databases(), vec![0]);

    let fetch = Message::Fetch {
        keys: vec![held.clone(), other],
    };
    let Message::Entries(found) = r.handle(id, fetch).await else {
        panic!("a fetch gets entries");
    };
    let keys: Vec<DbKey> = found.entries.into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![held]);

    let epoch = n.epoch_id();
    let digest = Message::Digest {
        from: epoch,
        to: epoch,
        db: 1,
        sums: vec![1; 256],
    };
    let Message::Entries(answer) = r.handle(id, digest).await else {
        panic!("a digest of another database gets entries");
    };
    assert!(answer.entries.is_empty());
}

/// `flush_db` drops the queued keys of one database and keeps the others.
#[tokio::test]
async fn flush_db_drops_the_queue_of_one_database() {
    let c = start_cluster(2, &["--replication-factor", "2"]).await;
    let a = &c.nodes[0].node;
    a.faults.hold_replication.store(true, Ordering::Relaxed);
    let key = bytes::Bytes::from_static(b"k");
    let r = a.replicator().unwrap().clone();
    r.written(a, 0, &key);
    r.written(a, 1, &key);
    assert!(!r.settled(0, &key) && !r.settled(1, &key));
    r.flush_db(1);
    assert!(r.settled(1, &key));
    assert!(!r.settled(0, &key));
    a.faults.hold_replication.store(false, Ordering::Relaxed);
}

/// A drop flushes the queued keys of every dropped database, also one whose map is empty.
#[tokio::test]
async fn a_drop_flushes_the_queue_of_a_database_without_entries() {
    let c = start_cluster(2, &["--replication-factor", "2"]).await;
    let a = &c.nodes[0].node;
    a.faults.hold_replication.store(true, Ordering::Relaxed);
    let key = bytes::Bytes::from_static(b"k");
    let r = a.replicator().unwrap().clone();
    for db in [0, 1, 2] {
        r.written(a, db, &key);
    }
    assert!(a.store.databases().iter().all(|db| *db == 0));
    a.drop_databases_from(1);
    assert!(
        wait_until(Duration::from_secs(10), || r.settled(1, &key)
            && r.settled(2, &key))
        .await,
        "the queued keys of databases 1 and 2 stayed"
    );
    assert!(!r.settled(0, &key));
    a.faults.hold_replication.store(false, Ordering::Relaxed);
}
