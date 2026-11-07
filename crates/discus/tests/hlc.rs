#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use discus::command::dispatch;
use discus::{Config, ConnState, Node};
use discus_placement::mock::MockClock;
use discus_resp::Value;
use discus_store::{Version, NTP64};

const START: u64 = 1_700_000_000_000;

/// A node on mock clock slot `SLOT`. Each test uses its own slot.
fn node<const SLOT: usize>() -> Node {
    let clock = MockClock::<SLOT>;
    clock.set(START);
    let config = Config::parse_from(["discus"]).unwrap();
    Node::with_clocks(config, Arc::new(clock), clock.hlc_clock())
}

fn ms(n: u64) -> NTP64 {
    NTP64::from(std::time::Duration::from_millis(n))
}

fn run(node: &Node, args: &[&str]) -> Value {
    let mut conn = ConnState::new(1, true);
    let cmd: Vec<Bytes> = args
        .iter()
        .map(|a| Bytes::copy_from_slice(a.as_bytes()))
        .collect();
    dispatch(node, &mut conn, &cmd)
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
