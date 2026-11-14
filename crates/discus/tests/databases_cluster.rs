#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::gossip::{wait_until, GossipNode, Net};
use common::run_in;
use discus_resp::Value;
use discus_wire::{encode_command, Forward, ForwardMode, Message, TraceContext};

const TEN: Duration = Duration::from_secs(10);

/// A key whose primary is `nodes[owner]`.
fn key_on(nodes: &[GossipNode], owner: usize, prefix: &str) -> String {
    (0..)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| nodes[0].primary(k.as_bytes()) == nodes[owner].id)
        .unwrap()
}

fn holds(n: &GossipNode, db: u32, key: &str) -> bool {
    n.node.store.read(db, key.as_bytes(), |e| e.is_some())
}

fn bulk(s: &str) -> Value {
    Value::bulk(bytes::Bytes::copy_from_slice(s.as_bytes()))
}

#[tokio::test]
async fn a_forwarded_write_keeps_its_database() {
    let net = Net::new();
    let nodes = net
        .start_agreed(3, &["--databases", "3", "--replication-factor", "2"])
        .await;
    let key = key_on(&nodes, 1, "fwd");
    assert_eq!(
        run_in(&nodes[0].node, 2, &["SET", &key, "v"]).await,
        Value::ok()
    );
    let owners = nodes[0].node.owners(key.as_bytes());
    let owners: Vec<&GossipNode> = nodes.iter().filter(|n| owners.contains(&n.id)).collect();
    assert_eq!(owners.len(), 2);
    let placed = wait_until(TEN, || {
        owners.iter().all(|n| holds(n, 2, &key)) && nodes.iter().all(|n| !holds(n, 0, &key))
    })
    .await;
    assert!(
        placed,
        "the write did not reach every owner in database 2 only"
    );
}

#[tokio::test]
async fn a_rename_and_an_mset_stay_in_their_database() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    let (k1, k2, k3) = (
        key_on(&nodes, 1, "one"),
        key_on(&nodes, 0, "two"),
        key_on(&nodes, 2, "three"),
    );
    assert_eq!(
        run_in(a, 2, &["MSET", &k1, "1", &k2, "2"]).await,
        Value::ok()
    );
    assert_eq!(run_in(a, 2, &["RENAME", &k1, &k3]).await, Value::ok());
    assert_eq!(
        run_in(a, 2, &["MGET", &k1, &k2, &k3]).await,
        Value::Array(vec![Value::Null, bulk("2"), bulk("1")])
    );
    assert_eq!(
        run_in(a, 0, &["EXISTS", &k1, &k2, &k3]).await,
        Value::Integer(0)
    );
}

#[tokio::test]
async fn a_forward_for_a_database_above_the_count_is_refused() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let seeds: Vec<_> = nodes.iter().map(|n| n.cluster_addr).collect();
    let d = net
        .start(&seeds, &["--databases", "1", "--lower-databases"])
        .await;
    assert!(
        wait_until(TEN, || d.is_normal()).await,
        "the fourth node did not join"
    );
    let a = &nodes[0].node;
    assert!(
        wait_until(TEN, || a.epochs().current().member(d.id).is_some()).await,
        "A does not know the fourth node"
    );
    let forward = Forward {
        command: encode_command(&["SET".into(), "k".into(), "v".into()]),
        epoch: a.epoch_id(),
        epoch_body: None,
        hops: 1,
        path: vec![a.id],
        mode: ForwardMode::Local,
        track: false,
        db: 2,
    };
    let reply = a
        .link()
        .unwrap()
        .transport
        .request(
            d.id,
            d.rpc_addr,
            Message::Forward(forward),
            TraceContext::default(),
            TEN,
            true,
        )
        .await
        .unwrap();
    let Message::ForwardReply(r) = reply.message else {
        panic!("not a reply: {reply:?}");
    };
    assert_eq!(r.value, Value::error(discus::databases::OUT_OF_RANGE));
    assert!(!holds(&d, 2, "k"));
}

fn int(v: Value) -> i64 {
    match v {
        Value::Integer(n) => n,
        other => panic!("not an integer: {other:?}"),
    }
}

fn text(v: Value) -> String {
    match v {
        Value::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// `count` keys with `prefix` in database `db`, written through `node`.
async fn fill(node: &std::sync::Arc<discus::Node>, db: u32, prefix: &str, count: usize) {
    for i in 0..count {
        let key = format!("{prefix}{i}");
        assert_eq!(run_in(node, db, &["SET", &key, "v"]).await, Value::ok());
    }
}

/// Run `args` on `node` in database `db` as a command from a redirect connection.
async fn run_redirect(node: &std::sync::Arc<discus::Node>, db: u32, args: &[&str]) -> Value {
    let cmd = common::cmd(args);
    let spec = discus::command::check(discus::command::lookup(&cmd[0]), &cmd).unwrap();
    let job = discus::cluster::Job {
        redirect: true,
        ..discus::cluster::Job::plain(cmd, spec, db)
    };
    discus::cluster::run(node, job, None, tracing::Span::none())
        .finish()
        .await
        .value
}

/// Every key that a full `SCAN` loop returns.
async fn scan_all(scan: impl AsyncFn(&str) -> Value) -> Vec<String> {
    let mut keys = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let Value::Array(parts) = scan(&cursor).await else {
            panic!("SCAN did not answer an array");
        };
        let [next, Value::Array(batch)] = <[Value; 2]>::try_from(parts).unwrap() else {
            panic!("SCAN did not answer a key array");
        };
        keys.extend(batch.into_iter().map(text));
        cursor = text(next);
        if cursor == "0" {
            return keys;
        }
    }
}

#[tokio::test]
async fn flushdb_clears_one_database() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    fill(a, 0, "zero", 20).await;
    fill(a, 1, "one", 15).await;
    fill(a, 2, "two", 10).await;
    assert_eq!(run_in(a, 1, &["FLUSHDB"]).await, Value::ok());
    assert!(
        wait_until(TEN, || nodes.iter().all(|n| n.node.store.db_len(1) == 0)).await,
        "a node still holds keys of database 1"
    );
    assert_eq!(int(run_in(a, 0, &["DBSIZE"]).await), 20);
    assert_eq!(int(run_in(a, 2, &["DBSIZE"]).await), 10);
}

#[tokio::test]
async fn flushdb_in_database_two_keeps_the_other_databases_on_every_node() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    fill(a, 0, "zero", 20).await;
    fill(a, 1, "one", 15).await;
    fill(a, 2, "two", 10).await;
    let before: Vec<(usize, usize)> = nodes
        .iter()
        .map(|n| (n.node.store.db_len(0), n.node.store.db_len(1)))
        .collect();
    assert_eq!(run_in(a, 2, &["FLUSHDB", "SYNC"]).await, Value::ok());
    assert!(
        wait_until(TEN, || nodes.iter().all(|n| n.node.store.db_len(2) == 0)).await,
        "a node still holds keys of database 2"
    );
    let after: Vec<(usize, usize)> = nodes
        .iter()
        .map(|n| (n.node.store.db_len(0), n.node.store.db_len(1)))
        .collect();
    assert_eq!(
        after, before,
        "FLUSHDB in database 2 changed database 0 or 1"
    );
    assert!(after.iter().all(|(zero, one)| *zero > 0 || *one > 0));
}

#[tokio::test]
async fn flushall_clears_every_database_on_every_node() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    fill(a, 0, "zero", 10).await;
    fill(a, 2, "two", 10).await;
    assert_eq!(run_in(a, 2, &["FLUSHALL"]).await, Value::ok());
    assert!(
        wait_until(TEN, || nodes.iter().all(|n| n.node.store.is_empty())).await,
        "a node still holds keys after FLUSHALL"
    );
}

#[tokio::test]
async fn dbsize_scan_and_randomkey_see_one_database() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    fill(a, 1, "one", 30).await;
    fill(a, 2, "two", 5).await;
    let mut want: Vec<String> = (0..5)
        .map(|i| format!("two{i}"))
        .filter(|k| holds(&nodes[0], 2, k))
        .collect();
    let mut got = scan_all(async |c: &str| run_in(a, 2, &["SCAN", c, "COUNT", "7"]).await).await;
    want.sort();
    got.sort();
    assert_eq!(got, want);
    match run_in(a, 2, &["RANDOMKEY"]).await {
        Value::Null => {}
        key => assert!(text(key).starts_with("two"), "a key of another database"),
    }
    assert_eq!(int(run_in(a, 2, &["DBSIZE"]).await), 5);
}

#[tokio::test]
async fn a_redirect_connection_counts_and_scans_its_database() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    fill(a, 0, "zero", 30).await;
    fill(a, 2, "two", 12).await;
    let primary_here = |db: u32, prefix: &str, count: usize| -> Vec<String> {
        let mut keys: Vec<String> = (0..count)
            .map(|i| format!("{prefix}{i}"))
            .filter(|k| a.epochs().current().primary(k.as_bytes()) == Some(a.id))
            .filter(|k| holds(&nodes[0], db, k))
            .collect();
        keys.sort();
        keys
    };
    let mut got =
        scan_all(async |c: &str| run_redirect(a, 2, &["SCAN", c, "COUNT", "5"]).await).await;
    got.sort();
    assert_eq!(got, primary_here(2, "two", 12));
    let copies = discus_membership::normal_count(a.epochs().current())
        .min(a.config.replication_factor)
        .max(1) as i64;
    assert_eq!(
        int(run_redirect(a, 2, &["DBSIZE"]).await),
        a.store.db_len(2) as i64 / copies
    );
}

#[tokio::test]
async fn info_keyspace_lists_each_database() {
    let net = Net::new();
    let nodes = net.start_agreed(1, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    fill(a, 0, "zero", 3).await;
    fill(a, 2, "two", 2).await;
    let info = text(run_in(a, 0, &["INFO", "keyspace"]).await);
    assert!(info.contains("db0:keys=3,expires=0,avg_ttl=0"), "{info}");
    assert!(info.contains("db2:keys=2,expires=0,avg_ttl=0"), "{info}");
    assert!(!info.contains("db1:"), "{info}");
    let discus = text(run_in(a, 0, &["INFO", "discus"]).await);
    assert!(discus.contains("\r\ndatabases:3\r\n"), "{discus}");
    assert!(discus.contains("cluster_databases:"), "{discus}");
}
