#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use discus::command::dispatch;
use discus::{Config, ConnState, Node};
use discus_resp::Value;
use discus_store::{ManualClock, Version};

const START: u64 = 1_700_000_000_000;

fn node() -> (Node, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(START));
    let config = Config::parse_from(["discus"]).unwrap();
    (Node::with_clock(config, clock.clone()), clock)
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
    let (node, _clock) = node();
    run(&node, &["SET", "k", "v"]);
    let first = version(&node, "k");
    assert_eq!(first.origin, node.id);
    assert_eq!(first.ts.wall_ms(), START);
    run(&node, &["SET", "k", "w"]);
    assert!(version(&node, "k") > first);
}

#[test]
fn collection_and_ttl_writes_stamp_the_entry() {
    let (node, _clock) = node();
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
        assert_eq!(first.origin, node.id, "{key}");
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
    let (node, clock) = node();
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
