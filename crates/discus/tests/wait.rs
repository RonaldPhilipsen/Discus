#![allow(clippy::unwrap_used)]

mod common;

use bytes::Bytes;
use common::cluster::{start_cluster, ClusterNode};
use common::gossip::Net;
use discus_placement::Version;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;

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
