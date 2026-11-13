#![allow(clippy::unwrap_used)]

use std::sync::Arc;

mod common;

use bytes::Bytes;
use discus::command::{check, execute_local, lookup};
use discus::security::Security;
use discus::Node;
use discus_placement::mock::MockClock;
use discus_resp::Value;
use discus_store::{Version, NTP64};

const START: u64 = 1_700_000_000_000;

/// A node on mock clock slot `SLOT`. Each test uses its own slot.
fn node<const SLOT: usize>() -> Node {
    let clock = MockClock::<SLOT>;
    clock.set(START);
    let config = common::config(&["discus"]);
    let security = Arc::new(Security::from_config(&config).unwrap());
    Node::with_clocks(config, security, Arc::new(clock), clock.hlc_clock())
}

fn ms(n: u64) -> NTP64 {
    NTP64::from(std::time::Duration::from_millis(n))
}

fn run(node: &Node, args: &[&str]) -> Value {
    let cmd: Vec<Bytes> = args
        .iter()
        .map(|a| Bytes::copy_from_slice(a.as_bytes()))
        .collect();
    execute_local(node, check(lookup(&cmd[0]), &cmd).unwrap(), &cmd)
}

fn version(node: &Node, key: &str) -> Version {
    node.store
        .read(key.as_bytes(), |e| e.map(|e| e.version))
        .unwrap()
}

fn is_tryagain(v: &Value) -> bool {
    format!("{v:?}").contains("TRYAGAIN")
}

#[test]
fn set_stamps_the_entry_with_the_node_origin() {
    let node = node::<0>();
    run(&node, &["SET", "k", "v"]);
    let first = version(&node, "k");
    assert_eq!(first.origin(), node.id);
    assert!(first.time() <= ms(START) && first.time() > ms(START - 1));
    run(&node, &["SET", "k", "w"]);
    assert!(version(&node, "k") > first);
}

#[test]
fn collection_and_ttl_writes_stamp_the_entry() {
    let node = node::<1>();
    let cases: &[(&str, &[&str])] = &[
        ("h", &["HSET", "h", "f", "1"]),
        ("s", &["SADD", "s", "m"]),
        ("z", &["ZADD", "z", "1", "m"]),
        ("n", &["INCR", "n"]),
        ("a", &["APPEND", "a", "x"]),
    ];
    for (key, cmd) in cases {
        run(&node, cmd);
        let first = version(&node, key);
        assert_eq!(first.origin(), node.id, "{key}");
        run(&node, cmd);
        assert!(version(&node, key) > first, "{key}");
    }
    let before = version(&node, "h");
    run(&node, &["EXPIRE", "h", "100"]);
    assert!(version(&node, "h") > before);
    let before = version(&node, "h");
    run(&node, &["PERSIST", "h"]);
    assert!(version(&node, "h") > before);
    run(&node, &["MSET", "m1", "a", "m2", "b"]);
    assert_eq!(version(&node, "m1"), version(&node, "m2"));
}

#[test]
fn a_pinned_clock_refuses_writes_but_serves_reads() {
    let node = node::<2>();
    let clock = MockClock::<2>;
    run(&node, &["SET", "k", "v"]);
    clock.set(START - 2_000);
    assert!(is_tryagain(&run(&node, &["SET", "k", "w"])));
    assert!(is_tryagain(&run(&node, &["DEL", "k"])));
    assert!(is_tryagain(&run(&node, &["HSET", "h", "f", "1"])));
    assert!(is_tryagain(&run(&node, &["INCR", "n"])));
    assert!(is_tryagain(&run(&node, &["EXPIRE", "k", "10"])));
    assert_eq!(run(&node, &["GET", "k"]), Value::Bulk(Bytes::from("v")));
    assert!(node.hlc.refusals() >= 5);
    clock.set(START);
    assert_eq!(run(&node, &["SET", "k", "w"]), Value::Simple("OK".into()));
    assert_eq!(run(&node, &["GET", "k"]), Value::Bulk(Bytes::from("w")));
}

/// A write that changes nothing keeps the version and takes no applied stamp, so the key does not
/// replicate. A stale value then never wins over a newer write on another node by a new stamp.
#[test]
fn a_write_that_changes_nothing_keeps_the_version() {
    let node = node::<3>();
    run(&node, &["SET", "k", "1"]);
    run(&node, &["HSET", "h", "f", "1"]);
    let cases: &[(&str, &[&str])] = &[
        ("k", &["SET", "k", "2", "NX"]),
        ("k", &["PERSIST", "k"]),
        ("k", &["GETEX", "k"]),
        ("k", &["INCRBY", "k", "9223372036854775807"]),
        ("h", &["GETEX", "h", "PERSIST"]),
        ("h", &["GETDEL", "h"]),
        ("h", &["INCR", "h"]),
        ("h", &["APPEND", "h", "x"]),
        ("h", &["SADD", "h", "m"]),
        ("h", &["SET", "h", "v", "GET"]),
    ];
    for (key, cmd) in cases {
        let before = version(&node, key);
        Node::take_applied();
        run(&node, cmd);
        assert_eq!(version(&node, key), before, "{cmd:?}");
        assert_eq!(Node::take_applied(), None, "{cmd:?}");
    }
    run(&node, &["SET", "k", "2", "XX"]);
    let applied = Node::take_applied();
    assert!(applied.is_some());
    assert_eq!(Some(version(&node, "k")), applied);
}

#[test]
fn a_replicated_batch_drops_only_the_entries_too_far_ahead() {
    let node = Arc::new(node::<4>());
    let replicator = discus::replication::Replicator::new(&node);
    let entry = |at_ms: u64| discus_store::Entry {
        version: Version::new(ms(at_ms), discus_placement::NodeId::random()),
        ..discus_store::Entry::new(discus_store::Value::Str(Bytes::from_static(b"v")), None)
    };
    let entries = vec![
        (Bytes::from_static(b"near"), entry(START + 10)),
        (Bytes::from_static(b"far"), entry(START + 5_000)),
        (Bytes::from_static(b"old"), entry(START - 10)),
    ];
    assert_eq!(replicator.apply_entries(&node, entries), 2);
    assert!(node.store.entry(b"near").is_some());
    assert!(node.store.entry(b"old").is_some());
    assert!(node.store.entry(b"far").is_none());
    run(&node, &["SET", "k", "v"]);
    let stamped = version(&node, "k").time();
    assert!(stamped > ms(START + 10) && stamped < ms(START + 1_000));
}
