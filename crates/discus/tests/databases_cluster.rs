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

/// A cluster of `n` nodes with `extra` flags whose gate is open, so commands that change the
/// database of a key run.
async fn start_open(net: &Net, n: usize, extra: &[&str]) -> Vec<GossipNode> {
    let nodes = net.start_agreed(n, extra).await;
    for n in &nodes {
        n.node.databases.open_gate();
    }
    nodes
}

const SAME: &str = "ERR source and destination objects are the same";

#[tokio::test]
async fn move_moves_a_key_to_another_database() {
    let net = Net::new();
    let nodes = start_open(&net, 3, &["--databases", "3", "--replication-factor", "2"]).await;
    let a = &nodes[0].node;
    let key = key_on(&nodes, 1, "mv");
    assert_eq!(run_in(a, 0, &["SET", &key, "v"]).await, Value::ok());
    assert_eq!(run_in(a, 0, &["MOVE", &key, "2"]).await, Value::Integer(1));
    let owners = a.owners(key.as_bytes());
    let owners: Vec<&GossipNode> = nodes.iter().filter(|n| owners.contains(&n.id)).collect();
    assert_eq!(owners.len(), 2);
    let moved = wait_until(TEN, || {
        owners.iter().all(|n| holds(n, 2, &key)) && nodes.iter().all(|n| !holds(n, 0, &key))
    })
    .await;
    assert!(moved, "the key is not in database 2 only on every owner");
    assert_eq!(run_in(a, 2, &["GET", &key]).await, bulk("v"));
}

#[tokio::test]
async fn move_refuses_a_live_destination() {
    let net = Net::new();
    let nodes = start_open(&net, 3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    let key = key_on(&nodes, 2, "mv");
    assert_eq!(run_in(a, 0, &["SET", &key, "zero"]).await, Value::ok());
    assert_eq!(run_in(a, 2, &["SET", &key, "two"]).await, Value::ok());
    assert_eq!(run_in(a, 0, &["MOVE", &key, "2"]).await, Value::Integer(0));
    assert_eq!(run_in(a, 0, &["GET", &key]).await, bulk("zero"));
    assert_eq!(run_in(a, 2, &["GET", &key]).await, bulk("two"));
}

#[tokio::test]
async fn move_edge_cases() {
    let net = Net::new();
    let nodes = start_open(&net, 3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    assert_eq!(run_in(a, 0, &["SET", "k", "v"]).await, Value::ok());
    assert_eq!(run_in(a, 0, &["MOVE", "k", "0"]).await, Value::error(SAME));
    assert_eq!(
        run_in(a, 0, &["MOVE", "missing", "1"]).await,
        Value::Integer(0)
    );
    assert_eq!(
        run_in(a, 0, &["MOVE", "k", "3"]).await,
        Value::error(discus::databases::OUT_OF_RANGE)
    );
    assert_eq!(
        run_in(a, 0, &["MOVE", "k", "x"]).await,
        Value::error("ERR value is not an integer or out of range")
    );
    assert_eq!(run_in(a, 0, &["GET", "k"]).await, bulk("v"));
}

fn pttl(v: Value) -> i64 {
    let ms = int(v);
    assert!(ms > 0, "no deadline: {ms}");
    ms
}

#[tokio::test]
async fn copy_across_slots_and_databases() {
    let net = Net::new();
    let nodes = start_open(&net, 3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    let src = key_on(&nodes, 1, "src");
    let dst = key_on(&nodes, 2, "dst");
    assert_eq!(
        run_in(a, 0, &["HSET", &src, "f", "v"]).await,
        Value::Integer(1)
    );
    assert_eq!(
        run_in(a, 0, &["EXPIRE", &src, "100"]).await,
        Value::Integer(1)
    );
    let copy = ["COPY", src.as_str(), dst.as_str(), "DB", "1"];
    assert_eq!(run_in(a, 0, &copy).await, Value::Integer(1));
    assert_eq!(run_in(a, 1, &["HGET", &dst, "f"]).await, bulk("v"));
    assert_eq!(run_in(a, 1, &["HLEN", &dst]).await, Value::Integer(1));
    let from = pttl(run_in(a, 0, &["PTTL", &src]).await);
    let to = pttl(run_in(a, 1, &["PTTL", &dst]).await);
    assert!((from - to).abs() <= 1000, "deadlines {from} and {to}");
    assert_eq!(run_in(a, 0, &copy).await, Value::Integer(0));
    let replace = ["COPY", src.as_str(), dst.as_str(), "DB", "1", "REPLACE"];
    assert_eq!(run_in(a, 0, &replace).await, Value::Integer(1));
    assert_eq!(
        run_in(a, 0, &["HGET", &src, "f"]).await,
        bulk("v"),
        "COPY changed the source"
    );
}

#[tokio::test]
async fn copy_to_itself_is_refused() {
    let net = Net::new();
    let nodes = start_open(&net, 3, &["--databases", "3"]).await;
    let a = &nodes[0].node;
    assert_eq!(run_in(a, 0, &["SET", "k", "v"]).await, Value::ok());
    assert_eq!(run_in(a, 0, &["COPY", "k", "k"]).await, Value::error(SAME));
    assert_eq!(
        run_in(a, 0, &["COPY", "k", "k", "DB", "1"]).await,
        Value::Integer(1)
    );
    assert_eq!(run_in(a, 1, &["GET", "k"]).await, bulk("v"));
}

#[tokio::test]
async fn swapdb_is_refused() {
    let net = Net::new();
    let nodes = start_open(&net, 1, &["--databases", "3"]).await;
    assert_eq!(
        run_in(&nodes[0].node, 0, &["SWAPDB", "0", "1"]).await,
        Value::error("ERR SWAPDB is not allowed in cluster mode")
    );
}

/// A key whose primary is another node and whose second owner is `nodes[entry]`.
fn key_replicated_on(nodes: &[GossipNode], entry: usize, prefix: &str) -> String {
    (0..)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| {
            let owners = nodes[0].node.owners(k.as_bytes());
            owners.len() == 2 && owners[1] == nodes[entry].id
        })
        .unwrap()
}

/// Put `value` at `key` in database `db` of `n` only, with the lowest version: what a replica
/// holds when it missed every later write.
fn plant_stale(n: &GossipNode, db: u32, key: &str, value: &str) {
    n.node.store.write(
        db,
        key.as_bytes(),
        discus_placement::Version::zero(),
        |slot| {
            slot.set(Some(discus_store::Entry::new(
                discus_store::Value::Str(bytes::Bytes::copy_from_slice(value.as_bytes())),
                None,
            )))
        },
    );
}

async fn query(conn: &mut redis::aio::MultiplexedConnection, args: &[&str]) -> redis::Value {
    let mut cmd = redis::cmd(args[0]);
    for a in &args[1..] {
        cmd.arg(*a);
    }
    cmd.query_async(conn).await.unwrap()
}

fn data(s: &str) -> redis::Value {
    redis::Value::BulkString(s.as_bytes().to_vec())
}

/// The entry node is the replica of both keys and holds a stale copy of each in the target
/// database, and replication is held. So only the write set of the connection sends the read to
/// the primary, which has the new value.
#[tokio::test]
async fn a_move_and_a_copy_read_their_own_write_in_the_target_database() {
    let net = Net::new();
    let nodes = start_open(
        &net,
        3,
        &[
            "--databases",
            "3",
            "--replication-factor",
            "2",
            "--read-from-replicas",
        ],
    )
    .await;
    let (k, src, dst) = (
        key_replicated_on(&nodes, 0, "mv"),
        key_on(&nodes, 1, "src"),
        key_replicated_on(&nodes, 0, "dst"),
    );
    let mut conn = nodes[0].redis().await;
    assert_eq!(
        query(&mut conn, &["SET", &k, "moved"]).await,
        redis::Value::Okay
    );
    assert_eq!(
        query(&mut conn, &["SET", &src, "copied"]).await,
        redis::Value::Okay
    );
    for n in &nodes {
        n.node
            .faults
            .hold_replication
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
    plant_stale(&nodes[0], 2, &k, "stale");
    plant_stale(&nodes[0], 1, &dst, "stale");

    assert_eq!(
        query(&mut conn, &["MOVE", &k, "2"]).await,
        redis::Value::Int(1)
    );
    assert_eq!(query(&mut conn, &["SELECT", "2"]).await, redis::Value::Okay);
    assert_eq!(query(&mut conn, &["GET", &k]).await, data("moved"));

    assert_eq!(query(&mut conn, &["SELECT", "0"]).await, redis::Value::Okay);
    assert_eq!(
        query(&mut conn, &["COPY", &src, &dst, "DB", "1"]).await,
        redis::Value::Int(1)
    );
    assert_eq!(query(&mut conn, &["SELECT", "1"]).await, redis::Value::Okay);
    assert_eq!(query(&mut conn, &["GET", &dst]).await, data("copied"));
    for n in &nodes {
        n.node
            .faults
            .hold_replication
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}
