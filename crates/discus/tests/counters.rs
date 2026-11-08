#![allow(clippy::unwrap_used)]

mod common;

use common::cluster::start_cluster;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Value};

async fn query(c: &mut MultiplexedConnection, args: &[&str]) -> redis::RedisResult<Value> {
    let mut cmd = redis::cmd(args[0]);
    for a in &args[1..] {
        cmd.arg(*a);
    }
    cmd.query_async(c).await
}

#[tokio::test]
async fn counters_work_on_one_node() {
    let mut c = common::connect(&[]).await;
    for want in 1..=3 {
        let n: i64 = c.incr("cnt:a", 1).await.unwrap();
        assert_eq!(n, want);
    }
    let n: i64 = redis::cmd("INCR")
        .arg("cnt:b")
        .arg("EX")
        .arg(100)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let ttl: i64 = c.ttl("cnt:b").await.unwrap();
    assert!((99..=100).contains(&ttl), "{ttl}");
    let _: () = c.set("cnt:s", "hello").await.unwrap();
    let s: String = c.get("cnt:s").await.unwrap();
    assert_eq!(s, "hello");
    let t: String = redis::cmd("TYPE")
        .arg("cnt:s")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(t, "string");
    let removed: i64 = c.del("cnt:s").await.unwrap();
    assert_eq!(removed, 1);
    let gone: Option<String> = c.get("cnt:s").await.unwrap();
    assert_eq!(gone, None);
}

#[tokio::test]
async fn scan_and_dbsize_exclude_counter_keys() {
    let mut c = common::connect(&[]).await;
    let _: () = c.set("plain", "1").await.unwrap();
    let _: i64 = c.incr("cnt:x", 1).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert_eq!(size, 1);
    let (_, keys): (String, Vec<String>) = redis::cmd("SCAN")
        .arg(0)
        .arg("COUNT")
        .arg(1000)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(keys, vec!["plain".to_string()]);
    let random: Option<String> = redis::cmd("RANDOMKEY").query_async(&mut c).await.unwrap();
    assert_eq!(random.as_deref(), Some("plain"));
}

#[tokio::test]
async fn a_mixed_multi_key_command_is_refused() {
    let mut c = common::connect(&[]).await;
    for args in [
        &["MGET", "cnt:a", "plain"][..],
        &["DEL", "plain", "cnt:a"][..],
        &["MSET", "cnt:a", "1", "plain", "2"][..],
    ] {
        let err = query(&mut c, args).await.unwrap_err();
        assert!(err.to_string().contains("cannot be mixed"), "{err}");
    }
}

#[tokio::test]
async fn mget_across_groups_merges_in_order() {
    let mut c = common::connect(&["--counter-groups", "8"]).await;
    let keys: Vec<String> = (0..12).map(|i| format!("cnt:m{i}")).collect();
    let mut mset = redis::cmd("MSET");
    for (i, k) in keys.iter().enumerate() {
        mset.arg(k).arg(i);
    }
    let _: () = mset.query_async(&mut c).await.unwrap();
    let mut mget = redis::cmd("MGET");
    for k in &keys {
        mget.arg(k);
    }
    mget.arg("cnt:missing");
    let got: Vec<Option<i64>> = mget.query_async(&mut c).await.unwrap();
    let mut want: Vec<Option<i64>> = (0..12).map(Some).collect();
    want.push(None);
    assert_eq!(got, want);
    let mut del = redis::cmd("DEL");
    for k in &keys {
        del.arg(k);
    }
    let removed: i64 = del.query_async(&mut c).await.unwrap();
    assert_eq!(removed, 12);
    let exists: i64 = c.exists(&keys).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn flushall_clears_counters() {
    let mut c = common::connect(&[]).await;
    let _: i64 = c.incr("cnt:f", 5).await.unwrap();
    let _: () = c.set("plain", "1").await.unwrap();
    let _: () = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();
    let n: Option<i64> = c.get("cnt:f").await.unwrap();
    assert_eq!(n, None);
    let p: Option<String> = c.get("plain").await.unwrap();
    assert_eq!(p, None);
}

#[tokio::test]
async fn other_commands_on_counter_keys_are_refused() {
    let mut c = common::connect(&[]).await;
    let err = query(&mut c, &["HSET", "cnt:h", "f", "v"])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("only string and expiry"), "{err}");
    let err = query(&mut c, &["SADD", "cnt:s", "m"]).await.unwrap_err();
    assert!(err.to_string().contains("only string"), "{err}");
}

#[tokio::test]
async fn a_full_reserve_returns_oom() {
    let mut c =
        common::connect(&["--max-memory", "1048576", "--counter-memory-percent", "1"]).await;
    let value = "x".repeat(1024);
    let mut stored = Vec::new();
    let mut oom = None;
    for i in 0..100 {
        let key = format!("cnt:big{i}");
        match c.set::<_, _, ()>(&key, &value).await {
            Ok(()) => stored.push(key),
            Err(e) => {
                oom = Some(e.to_string());
                break;
            }
        }
    }
    let oom = oom.expect("the reserve never filled");
    assert!(oom.contains("OOM"), "{oom}");
    assert!(!stored.is_empty());
    let removed: i64 = c.del(&stored[0]).await.unwrap();
    assert_eq!(removed, 1);
    let _: () = c.set("plain", &value).await.unwrap();
    let _: () = c.set(&stored[0], "small").await.unwrap();
}

#[tokio::test]
async fn counter_keys_never_replicate() {
    let c = start_cluster(3, &["--replication-factor", "3"]).await;
    for n in &c.nodes {
        let mut client = n.redis().await;
        let _: i64 = client.incr("cnt:x", 1).await.unwrap();
        let _: () = client.set("cnt:y", "v").await.unwrap();
    }
    for n in &c.nodes {
        assert!(!n.holds(b"cnt:x") && !n.holds(b"cnt:y"));
        let r = n.node.replicator().unwrap();
        for other in &c.nodes {
            assert_eq!(r.depth(other.id), 0);
        }
    }
    let mut client = c.nodes[0].redis().await;
    let n: i64 = client.get("cnt:x").await.unwrap();
    assert_eq!(n, 3);
}

#[tokio::test]
async fn every_entry_node_reaches_the_leader() {
    let c = start_cluster(3, &[]).await;
    let mut clients = Vec::new();
    for n in &c.nodes {
        clients.push(n.redis().await);
    }
    for i in 0..30 {
        let n: i64 = clients[i % 3].incr("cnt:y", 1).await.unwrap();
        assert_eq!(n, i as i64 + 1);
    }
}
