#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashMap;

use common::cluster::start_cluster;
use proptest::prelude::*;
use proptest::test_runner::{Config as PropConfig, TestRunner};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
enum Op {
    Set(u8, u16),
    Get(u8),
    Incr(u8),
    Mget(Vec<u8>),
    Mset(Vec<(u8, u16)>),
    Del(Vec<u8>),
}

fn op() -> impl Strategy<Value = Op> {
    let key = 0u8..8;
    prop_oneof![
        (key.clone(), any::<u16>()).prop_map(|(k, v)| Op::Set(k, v)),
        key.clone().prop_map(Op::Get),
        (0u8..4).prop_map(Op::Incr),
        prop::collection::vec(key.clone(), 1..6).prop_map(Op::Mget),
        prop::collection::vec((key.clone(), any::<u16>()), 1..5).prop_map(Op::Mset),
        prop::collection::vec(0u8..12, 1..5).prop_map(Op::Del),
    ]
}

fn wire(args: &[String]) -> bytes::Bytes {
    let args: Vec<bytes::Bytes> = args.iter().map(|a| a.clone().into()).collect();
    discus_wire::encode_command(&args)
}

fn bulk(v: Option<&String>) -> String {
    match v {
        Some(v) => format!("${}\r\n{v}\r\n", v.len()),
        None => "$-1\r\n".into(),
    }
}

/// Keys 0 to 7 hold strings. Keys 8 to 11 are the counters `c0` to `c3`.
fn name(prefix: &str, k: u8) -> String {
    if k < 8 {
        format!("{prefix}:k{k}")
    } else {
        format!("{prefix}:c{}", k - 8)
    }
}

/// The request bytes and the replies that a sequential model expects.
fn script(prefix: &str, ops: &[Op]) -> (Vec<u8>, Vec<u8>) {
    let mut model: HashMap<String, String> = HashMap::new();
    let (mut req, mut want) = (Vec::new(), String::new());
    for op in ops {
        let args: Vec<String> = match op {
            Op::Set(k, v) => {
                model.insert(name(prefix, *k), v.to_string());
                want.push_str("+OK\r\n");
                vec!["SET".into(), name(prefix, *k), v.to_string()]
            }
            Op::Get(k) => {
                want.push_str(&bulk(model.get(&name(prefix, *k))));
                vec!["GET".into(), name(prefix, *k)]
            }
            Op::Incr(c) => {
                let key = name(prefix, 8 + c);
                let n = model.get(&key).map_or(0, |v| v.parse::<i64>().unwrap()) + 1;
                model.insert(key.clone(), n.to_string());
                want.push_str(&format!(":{n}\r\n"));
                vec!["INCR".into(), key]
            }
            Op::Mget(ks) => {
                want.push_str(&format!("*{}\r\n", ks.len()));
                for k in ks {
                    want.push_str(&bulk(model.get(&name(prefix, *k))));
                }
                let mut a = vec!["MGET".to_string()];
                a.extend(ks.iter().map(|k| name(prefix, *k)));
                a
            }
            Op::Mset(pairs) => {
                let mut a = vec!["MSET".to_string()];
                for (k, v) in pairs {
                    model.insert(name(prefix, *k), v.to_string());
                    a.push(name(prefix, *k));
                    a.push(v.to_string());
                }
                want.push_str("+OK\r\n");
                a
            }
            Op::Del(ks) => {
                let mut a = vec!["DEL".to_string()];
                let mut n = 0;
                for k in ks {
                    if model.remove(&name(prefix, *k)).is_some() {
                        n += 1;
                    }
                    a.push(name(prefix, *k));
                }
                want.push_str(&format!(":{n}\r\n"));
                a
            }
        };
        req.extend(wire(&args));
    }
    (req, want.into_bytes())
}

async fn exchange(addr: std::net::SocketAddr, req: &[u8], want_len: usize) -> Vec<u8> {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(req).await.unwrap();
    let mut got = vec![0u8; want_len];
    s.read_exact(&mut got).await.unwrap();
    got
}

#[test]
fn pipelines_keep_order_across_nodes() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let c = rt.block_on(start_cluster(3, &[]));
    let addrs: Vec<_> = c.nodes.iter().map(|n| n.addr).collect();
    let mut runner = TestRunner::new(PropConfig::with_cases(24));
    let case = std::cell::Cell::new(0u32);
    runner
        .run(
            &(prop::collection::vec(op(), 1..200), 0usize..3),
            |(ops, entry)| {
                case.set(case.get() + 1);
                let prefix = format!("p{}", case.get());
                let (req, want) = script(&prefix, &ops);
                let got = rt.block_on(exchange(addrs[entry], &req, want.len()));
                prop_assert_eq!(
                    String::from_utf8_lossy(&got),
                    String::from_utf8_lossy(&want)
                );
                Ok(())
            },
        )
        .unwrap();
    drop(c);
}

#[tokio::test]
async fn same_key_order_holds_with_a_remote_hop() {
    let c = start_cluster(2, &[]).await;
    let remote = c.key_on(1, "order:r");
    let local = c.key_on(0, "order:l");
    let mut req = Vec::new();
    let mut want = String::new();
    for i in 1..=500 {
        req.extend(wire(&["INCR".into(), remote.clone()]));
        req.extend(wire(&["INCR".into(), local.clone()]));
        want.push_str(&format!(":{i}\r\n:{i}\r\n"));
    }
    let got = exchange(c.nodes[0].addr, &req, want.len()).await;
    assert_eq!(String::from_utf8_lossy(&got), want);
}
