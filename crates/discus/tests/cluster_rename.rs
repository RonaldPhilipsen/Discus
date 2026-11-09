#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::cluster::{start_cluster, Cluster, ClusterNode};
use common::counters::is_try_again;
use common::gossip::wait_until;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisResult};

const ARGS: &[&str] = &["--replication-factor", "2"];

/// A source key on node 0 and a destination key on node 1.
fn keys(c: &Cluster) -> (String, String) {
    (c.key_on(0, "src"), c.key_on(1, "dst"))
}

/// The node that owns no copy of `key`, so a RENAME that runs on that node alone cannot find it.
fn outsider<'a>(c: &'a Cluster, key: &str) -> &'a ClusterNode {
    let owners = c.nodes[0].node.owners(key.as_bytes());
    c.nodes.iter().find(|n| !owners.contains(&n.id)).unwrap()
}

async fn rename(conn: &mut MultiplexedConnection, from: &str, to: &str) -> RedisResult<String> {
    redis::cmd("RENAME")
        .arg(from)
        .arg(to)
        .query_async(conn)
        .await
}

#[tokio::test]
async fn rename_moves_a_string_between_primaries() {
    let c = start_cluster(3, ARGS).await;
    let (src, dst) = keys(&c);
    let mut conn = outsider(&c, &src).redis().await;
    let _: () = redis::cmd("SET")
        .arg(&src)
        .arg("v")
        .arg("PX")
        .arg(100_000)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(rename(&mut conn, &src, &dst).await.unwrap(), "OK");

    let get_dst: String = conn.get(&dst).await.unwrap();
    let exists_src: i64 = conn.exists(&src).await.unwrap();
    let pttl_dst: i64 = conn.pttl(&dst).await.unwrap();
    assert_eq!(get_dst, "v");
    assert_eq!(exists_src, 0);
    assert!(pttl_dst > 0 && pttl_dst <= 100_000, "{pttl_dst}");
}

#[tokio::test]
async fn rename_moves_a_hash_between_primaries() {
    let c = start_cluster(3, ARGS).await;
    let (src, dst) = keys(&c);
    let mut conn = outsider(&c, &src).redis().await;
    let expected_fields: HashMap<String, String> = [("a", "1"), ("b", "2")]
        .map(|(f, v)| (f.to_string(), v.to_string()))
        .into_iter()
        .collect();
    let _: () = conn.hset(&src, "a", "1").await.unwrap();
    let _: () = conn.hset(&src, "b", "2").await.unwrap();
    let _: () = conn.pexpire(&src, 100_000).await.unwrap();

    assert_eq!(rename(&mut conn, &src, &dst).await.unwrap(), "OK");

    let hgetall_dst: HashMap<String, String> = conn.hgetall(&dst).await.unwrap();
    let pttl_dst: i64 = conn.pttl(&dst).await.unwrap();
    assert_eq!(hgetall_dst, expected_fields);
    assert!(pttl_dst > 0, "{pttl_dst}");
}

#[tokio::test]
async fn only_one_of_two_racing_renames_gets_the_value() {
    let c = start_cluster(3, ARGS).await;
    let src = c.key_on(0, "src");
    let (dst1, dst2) = (c.key_on(1, "dst1"), c.key_on(2, "dst2"));
    let mut a = outsider(&c, &src).redis().await;
    let mut b = outsider(&c, &src).redis().await;
    let _: () = a.set(&src, "v").await.unwrap();

    let (ra, rb) = tokio::join!(rename(&mut a, &src, &dst1), rename(&mut b, &src, &dst2));

    let results = [ra, rb];
    let oks = results.iter().filter(|r| r.is_ok()).count();
    let no_such_key = results
        .iter()
        .filter(|r| {
            r.as_ref()
                .is_err_and(|e| e.to_string().contains("no such key"))
        })
        .count();
    assert_eq!(oks, 1, "{results:?}");
    assert_eq!(no_such_key, 1, "{results:?}");
}

#[tokio::test]
async fn rename_to_the_same_key_on_another_node_changes_nothing() {
    let c = start_cluster(3, ARGS).await;
    let k = c.key_on(0, "same");
    let mut conn = outsider(&c, &k).redis().await;
    let _: () = conn.set(&k, "v").await.unwrap();

    assert_eq!(rename(&mut conn, &k, &k).await.unwrap(), "OK");

    let get: String = conn.get(&k).await.unwrap();
    assert_eq!(get, "v");
    let missing = c.key_on(0, "missing");
    let err = rename(&mut conn, &missing, &missing).await.unwrap_err();
    assert!(err.to_string().contains("no such key"), "{err}");
}

#[tokio::test]
async fn a_lost_destination_fails_and_never_keeps_the_source() {
    let c = start_cluster(
        3,
        &[
            "--replication-factor",
            "2",
            "--forward-write-timeout-ms",
            "200",
            "--hold-budget-ms",
            "1000",
        ],
    )
    .await;
    let (src, dst) = keys(&c);
    let mut conn = c.nodes[0].redis().await;
    let _: () = conn.set(&src, "v").await.unwrap();
    c.nodes[1]
        .node
        .faults
        .stall_forwards
        .store(true, Ordering::Relaxed);

    let err = rename(&mut conn, &src, &dst).await.unwrap_err();
    assert!(is_try_again(&err), "{err}");

    let exists_src: i64 = conn.exists(&src).await.unwrap();
    assert_eq!(exists_src, 0);
    let retry = rename(&mut conn, &src, &dst).await.unwrap_err();
    assert!(retry.to_string().contains("no such key"), "{retry}");
}

#[tokio::test]
async fn a_collection_too_large_for_a_forward_stays_in_place() {
    let c = start_cluster(
        3,
        &["--replication-factor", "2", "--max-value-bytes", "1024"],
    )
    .await;
    let (src, dst) = keys(&c);
    let mut conn = outsider(&c, &src).redis().await;
    let fields = 40_i64;
    for i in 0..fields {
        let _: () = conn
            .hset(&src, format!("field-{i}"), "x".repeat(64))
            .await
            .unwrap();
    }

    let rename_result = rename(&mut conn, &src, &dst).await;
    assert!(rename_result.is_err(), "{rename_result:?}");
    let err = rename_result.unwrap_err().to_string();
    assert!(err.contains("too large"), "{err}");

    let hlen_src: i64 = conn.hlen(&src).await.unwrap();
    assert_eq!(hlen_src, fields);
}

#[tokio::test]
async fn a_client_cannot_send_internal_commands() {
    let c = start_cluster(3, ARGS).await;
    let mut conn = c.nodes[0].redis().await;
    let err = redis::cmd("DISCUS.TAKE")
        .arg("k")
        .query_async::<String>(&mut conn)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown command"), "{err}");
    let err = redis::cmd("DISCUS.PUT")
        .arg("k")
        .arg("blob")
        .query_async::<String>(&mut conn)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown command"), "{err}");
}

#[tokio::test]
async fn rename_of_a_written_key_reads_back_on_one_connection() {
    let c = start_cluster(3, ARGS).await;
    let (src, dst) = keys(&c);
    // The entry node is the secondary of the destination and holds an older value, so a miss
    // cannot trigger read repair. With its replication stalled, only the write set of the
    // connection sends the read to the primary.
    let owners = c.nodes[0].node.owners(dst.as_bytes());
    let entry = c.by_id(owners[1]);
    let mut other = c.nodes[0].redis().await;
    let _: () = other.set(&dst, "old").await.unwrap();
    assert!(wait_until(Duration::from_secs(5), || entry.holds(dst.as_bytes())).await);
    let mut conn = entry.redis().await;
    let _: () = conn.set(&src, "v").await.unwrap();
    entry
        .node
        .faults
        .stall_bulk_ms
        .store(2_000, Ordering::Relaxed);

    assert_eq!(rename(&mut conn, &src, &dst).await.unwrap(), "OK");

    let get_dst: String = conn.get(&dst).await.unwrap();
    assert_eq!(get_dst, "v");
}
