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
