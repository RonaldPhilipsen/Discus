//! The topology replies of `CLUSTER SLOTS`, `SHARDS`, `NODES`, and `INFO` in the Redis 7 shapes
//! (spec "CLUSTER and connection commands").

use std::collections::HashMap;

use bytes::Bytes;
use discus_placement::{Epoch, Member, NodeId, RANGES, SLOTS, SLOTS_PER_RANGE};
use discus_resp::{Protocol, Value};

use super::redirect::{endpoint, Endpoint};
use crate::command::CommandError;

/// The topology replies that a node caches per epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyKind {
    Slots,
    Shards,
    Nodes,
    Info,
}

/// The 40-character Redis form of a node ID: the 32 hex digits followed by `00000000`, so
/// clients that check the length accept it.
pub fn node_id(id: NodeId) -> String {
    format!("{id}00000000")
}

/// One run of consecutive slots with the same ordered owner list.
struct Run {
    start: u16,
    end: u16,
    owners: Vec<NodeId>,
}

/// The runs of the epoch, in slot order. Ranges without an owner are left out.
fn runs(epoch: &Epoch, rf: usize) -> Vec<Run> {
    let mut out: Vec<Run> = Vec::new();
    for range in 0..RANGES {
        let owners = epoch.range_owners(range, rf);
        if owners.is_empty() {
            continue;
        }
        let start = range * SLOTS_PER_RANGE;
        let end = start + SLOTS_PER_RANGE - 1;
        match out.last_mut() {
            Some(last) if last.end + 1 == start && last.owners == owners => last.end = end,
            _ => out.push(Run { start, end, owners }),
        }
    }
    out
}

/// The members and endpoints of every `NORMAL` member. A `NORMAL` member without a client
/// address makes the topology incomplete (spec "The member record").
fn endpoints(epoch: &Epoch) -> Result<HashMap<NodeId, (&Member, Endpoint)>, CommandError> {
    epoch
        .normal()
        .map(|m| {
            let to = endpoint(m).ok_or_else(|| {
                CommandError::Custom(format!(
                    "ERR the cluster topology is not complete, member {} has no client address",
                    node_id(m.id)
                ))
            })?;
            Ok((m.id, (m, to)))
        })
        .collect()
}

fn bulk(s: impl Into<String>) -> Value {
    Value::Bulk(Bytes::from(s.into()))
}

/// A map for RESP3, an array of alternating keys and values for RESP2.
fn pairs(protocol: Protocol, fields: Vec<(&str, Value)>) -> Value {
    let fields = fields.into_iter().map(|(k, v)| (bulk(k), v));
    match protocol {
        Protocol::Resp3 => Value::Map(fields.collect()),
        Protocol::Resp2 => Value::Array(fields.flat_map(|(k, v)| [k, v]).collect()),
    }
}

fn ip(member: &Member) -> String {
    member
        .client_addr
        .map(|a| a.ip().to_string())
        .unwrap_or_default()
}

/// `CLUSTER SLOTS`: `[start, end, primary, secondary, ...]` for each run. Each node is
/// `[endpoint, port, id, metadata]`. The metadata holds the IP when the endpoint is a host name.
pub fn slots(epoch: &Epoch, rf: usize, protocol: Protocol) -> Result<Value, CommandError> {
    let known = endpoints(epoch)?;
    let node = |id: &NodeId| {
        let (m, to) = &known[id];
        let meta = match m.client_host {
            Some(_) => vec![("ip", bulk(ip(m)))],
            None => vec![],
        };
        Value::Array(vec![
            bulk(to.host.clone()),
            Value::Integer(i64::from(to.port)),
            bulk(node_id(*id)),
            pairs(protocol, meta),
        ])
    };
    Ok(Value::Array(
        runs(epoch, rf)
            .iter()
            .map(|run| {
                let mut entry = vec![
                    Value::Integer(i64::from(run.start)),
                    Value::Integer(i64::from(run.end)),
                ];
                entry.extend(run.owners.iter().map(node));
                Value::Array(entry)
            })
            .collect(),
    ))
}

/// `CLUSTER SHARDS`: one shard for each distinct ordered owner list, in the order of its first
/// slot. A node appears in every shard that lists it.
pub fn shards(epoch: &Epoch, rf: usize, protocol: Protocol) -> Result<Value, CommandError> {
    let known = endpoints(epoch)?;
    let mut order: Vec<Vec<NodeId>> = Vec::new();
    let mut slots: HashMap<Vec<NodeId>, Vec<Value>> = HashMap::new();
    for run in runs(epoch, rf) {
        let pairs = slots.entry(run.owners.clone()).or_insert_with(|| {
            order.push(run.owners.clone());
            Vec::new()
        });
        pairs.push(Value::Integer(i64::from(run.start)));
        pairs.push(Value::Integer(i64::from(run.end)));
    }
    let node = |i: usize, id: &NodeId| {
        let (m, to) = &known[id];
        let mut fields = vec![
            ("id", bulk(node_id(*id))),
            ("port", Value::Integer(i64::from(to.port))),
            ("ip", bulk(ip(m))),
            ("endpoint", bulk(to.host.clone())),
        ];
        if let Some(host) = &m.client_host {
            fields.push(("hostname", bulk(host.clone())));
        }
        fields.extend([
            ("role", bulk(if i == 0 { "master" } else { "replica" })),
            ("replication-offset", Value::Integer(0)),
            ("health", bulk("online")),
        ]);
        pairs(protocol, fields)
    };
    Ok(Value::Array(
        order
            .into_iter()
            .map(|owners| {
                let nodes = owners.iter().enumerate().map(|(i, id)| node(i, id));
                pairs(
                    protocol,
                    vec![
                        (
                            "slots",
                            Value::Array(slots.remove(&owners).unwrap_or_default()),
                        ),
                        ("nodes", Value::Array(nodes.collect())),
                    ],
                )
            })
            .collect(),
    ))
}

/// The runs of slots that each member is the primary of.
fn primaries(epoch: &Epoch, rf: usize) -> HashMap<NodeId, Vec<(u16, u16)>> {
    let mut out: HashMap<NodeId, Vec<(u16, u16)>> = HashMap::new();
    for run in runs(epoch, rf) {
        let list = out.entry(run.owners[0]).or_default();
        match list.last_mut() {
            Some(last) if last.1 + 1 == run.start => last.1 = run.end,
            _ => list.push((run.start, run.end)),
        }
    }
    out
}

/// `CLUSTER NODES`: one line for each `NORMAL` member,
/// `<id> <ip>:<port>@<rpc port>[,<hostname>] <flags> - 0 <now ms> <generation> connected <slots>`.
/// The slots are the runs the member is the primary of, so the lines show no replicas.
pub fn nodes(epoch: &Epoch, rf: usize, me: NodeId, now_ms: u64) -> Result<String, CommandError> {
    let known = endpoints(epoch)?;
    let primaries = primaries(epoch, rf);
    let generation = epoch.id().generation;
    let mut out = String::new();
    for m in epoch.normal() {
        let (_, to) = &known[&m.id];
        let host = m
            .client_host
            .as_ref()
            .map_or(String::new(), |h| format!(",{h}"));
        let flags = if m.id == me {
            "myself,master"
        } else {
            "master"
        };
        out.push_str(&format!(
            "{} {}:{}@{}{host} {flags} - 0 {now_ms} {generation} connected",
            node_id(m.id),
            ip(m),
            to.port,
            m.rpc_addr.port(),
        ));
        for (start, end) in primaries.get(&m.id).into_iter().flatten() {
            if start == end {
                out.push_str(&format!(" {start}"));
            } else {
                out.push_str(&format!(" {start}-{end}"));
            }
        }
        out.push('\n');
    }
    Ok(out)
}

/// `CLUSTER INFO`. The state is `ok` while this node is `NORMAL` and every slot has a primary.
/// Both epoch fields show the generation of the epoch.
pub fn info(epoch: &Epoch, rf: usize, normal: bool) -> String {
    let runs = runs(epoch, rf);
    let assigned: u32 = runs.iter().map(|r| u32::from(r.end - r.start) + 1).sum();
    let size = primaries(epoch, rf).len();
    let ok = normal && assigned == u32::from(SLOTS);
    let generation = epoch.id().generation;
    let lines = [
        format!("cluster_state:{}", if ok { "ok" } else { "fail" }),
        format!("cluster_slots_assigned:{assigned}"),
        format!("cluster_slots_ok:{assigned}"),
        "cluster_slots_pfail:0".into(),
        "cluster_slots_fail:0".into(),
        format!("cluster_known_nodes:{}", epoch.normal().count()),
        format!("cluster_size:{size}"),
        format!("cluster_current_epoch:{generation}"),
        format!("cluster_my_epoch:{generation}"),
    ];
    lines.iter().map(|l| format!("{l}\r\n")).collect()
}

/// The reply of `kind`. `me` names the `myself` line of `CLUSTER NODES`.
pub fn build(
    kind: TopologyKind,
    epoch: &Epoch,
    rf: usize,
    protocol: Protocol,
    me: NodeId,
    normal: bool,
) -> Result<Value, CommandError> {
    match kind {
        TopologyKind::Slots => slots(epoch, rf, protocol),
        TopologyKind::Shards => shards(epoch, rf, protocol),
        TopologyKind::Nodes => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64);
            nodes(epoch, rf, me, now_ms).map(bulk)
        }
        TopologyKind::Info => Ok(bulk(info(epoch, rf, normal))),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use discus_placement::{ClusterId, EpochId, MemberState, Weight};

    use super::*;

    fn member(n: u128, client: bool) -> Member {
        let addr = SocketAddr::from(([10, 0, 0, n as u8], 7000));
        Member {
            id: NodeId::from_u128(n),
            addr,
            rpc_addr: SocketAddr::from(([10, 0, 0, n as u8], 7001)),
            client_addr: client.then(|| SocketAddr::from(([10, 0, 0, n as u8], 6380))),
            client_host: None,
            state: MemberState::Normal,
            weight: Weight::default(),
        }
    }

    fn epoch(members: Vec<Member>) -> Epoch {
        let id = EpochId {
            generation: 7,
            origin: members[0].id,
        };
        Epoch::new(id, ClusterId::from_u128(1), members).unwrap()
    }

    fn int(v: &Value) -> i64 {
        match v {
            Value::Integer(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }

    #[test]
    fn node_id_has_40_characters() {
        let id = NodeId::from_u128(0xabc);
        let text = node_id(id);
        assert_eq!(text.len(), 40);
        assert_eq!(text, format!("{id}00000000"));
    }

    #[test]
    fn slots_merges_runs() {
        // Find three members whose ranges 0 and 1 share an owner list and range 2 does not.
        let rf = 2;
        let e = (1u128..)
            .map(|base| epoch((0..3).map(|i| member(base * 10 + i, true)).collect()))
            .find(|e| {
                e.range_owners(0, rf) == e.range_owners(1, rf)
                    && e.range_owners(1, rf) != e.range_owners(2, rf)
            })
            .unwrap();
        let Value::Array(entries) = slots(&e, rf, Protocol::Resp2).unwrap() else {
            panic!("not an array");
        };
        let Value::Array(first) = &entries[0] else {
            panic!("not an entry");
        };
        assert_eq!(int(&first[0]), 0);
        assert_eq!(int(&first[1]), i64::from(2 * SLOTS_PER_RANGE - 1));
        assert_eq!(first.len(), 2 + rf);
        let Value::Array(primary) = &first[2] else {
            panic!("not a node");
        };
        let owner = e.range_owners(0, rf)[0];
        let ip = format!("10.0.0.{}", owner.as_u128() as u8);
        assert_eq!(primary[0], Value::bulk(ip.into_bytes()));
        assert_eq!(int(&primary[1]), 6380);
        assert_eq!(primary[2], Value::bulk(node_id(owner).into_bytes()));
        assert_eq!(primary[3], Value::Array(vec![]));
        let covered: i64 = entries
            .iter()
            .map(|entry| {
                let Value::Array(entry) = entry else {
                    panic!("not an entry");
                };
                int(&entry[1]) - int(&entry[0]) + 1
            })
            .sum();
        assert_eq!(covered, i64::from(SLOTS));
    }

    #[test]
    fn resp3_metadata_is_a_map_with_the_ip() {
        let mut m = member(1, true);
        m.client_host = Some("node-1.peers".into());
        let e = epoch(vec![m]);
        let Value::Array(entries) = slots(&e, 1, Protocol::Resp3).unwrap() else {
            panic!("not an array");
        };
        let Value::Array(entry) = &entries[0] else {
            panic!("not an entry");
        };
        let Value::Array(node) = &entry[2] else {
            panic!("not a node");
        };
        assert_eq!(node[0], Value::bulk(&b"node-1.peers"[..]));
        assert_eq!(
            node[3],
            Value::Map(vec![(
                Value::bulk(&b"ip"[..]),
                Value::bulk(&b"10.0.0.1"[..])
            )])
        );
        let Value::Array(entries) = slots(&e, 1, Protocol::Resp2).unwrap() else {
            panic!("not an array");
        };
        let Value::Array(entry) = &entries[0] else {
            panic!("not an entry");
        };
        let Value::Array(node) = &entry[2] else {
            panic!("not a node");
        };
        assert_eq!(
            node[3],
            Value::Array(vec![Value::bulk(&b"ip"[..]), Value::bulk(&b"10.0.0.1"[..])])
        );
    }

    #[test]
    fn missing_client_address_is_an_error() {
        let e = epoch(vec![member(1, true), member(2, false)]);
        let want = CommandError::Custom(format!(
            "ERR the cluster topology is not complete, member {} has no client address",
            node_id(NodeId::from_u128(2))
        ));
        assert_eq!(slots(&e, 2, Protocol::Resp2), Err(want.clone()));
        assert_eq!(shards(&e, 2, Protocol::Resp2), Err(want.clone()));
        assert_eq!(nodes(&e, 2, NodeId::from_u128(1), 0), Err(want));
    }

    #[test]
    fn nodes_lists_primary_ranges_only() {
        let rf = 3;
        let members: Vec<Member> = (1..=3).map(|n| member(n, true)).collect();
        let e = epoch(members);
        let me = NodeId::from_u128(2);
        let text = nodes(&e, rf, me, 1234).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        let mut primaries = 0u32;
        for (i, line) in lines.iter().enumerate() {
            let n = i as u128 + 1;
            let id = NodeId::from_u128(n);
            let fields: Vec<&str> = line.split(' ').collect();
            assert!(fields.len() >= 9, "{line}");
            assert_eq!(fields[0], node_id(id));
            assert_eq!(fields[1], format!("10.0.0.{n}:6380@7001"));
            let flags = if id == me { "myself,master" } else { "master" };
            assert_eq!(fields[2], flags);
            assert_eq!(&fields[3..8], ["-", "0", "1234", "7", "connected"]);
            for run in &fields[8..] {
                let (start, end) = match run.split_once('-') {
                    Some((s, e)) => (s.parse::<u16>().unwrap(), e.parse::<u16>().unwrap()),
                    None => (run.parse().unwrap(), run.parse().unwrap()),
                };
                for s in start..=end {
                    assert_eq!(e.slot_owners(s, rf)[0], id, "slot {s} on line {i}");
                    primaries += 1;
                }
            }
        }
        assert_eq!(primaries, u32::from(SLOTS));
    }

    #[test]
    fn nodes_appends_the_host_name() {
        let mut m = member(1, true);
        m.client_host = Some("node-1.peers".into());
        let text = nodes(&epoch(vec![m]), 1, NodeId::from_u128(1), 0).unwrap();
        assert!(
            text.contains(" 10.0.0.1:6380@7001,node-1.peers myself,master "),
            "{text}"
        );
        assert!(text.ends_with(" 0-16383\n"), "{text}");
    }

    #[test]
    fn shards_group_by_owner_list() {
        let rf = 2;
        let e = epoch((1..=3).map(|n| member(n, true)).collect());
        let Value::Array(shards) = shards(&e, rf, Protocol::Resp3).unwrap() else {
            panic!("not an array");
        };
        let mut lists = std::collections::HashSet::new();
        let mut covered = 0;
        for shard in &shards {
            let Value::Map(fields) = shard else {
                panic!("not a map");
            };
            assert_eq!(fields[0].0, Value::bulk(&b"slots"[..]));
            let Value::Array(pairs) = &fields[0].1 else {
                panic!("not slots");
            };
            for pair in pairs.chunks(2) {
                covered += int(&pair[1]) - int(&pair[0]) + 1;
            }
            let Value::Array(nodes) = &fields[1].1 else {
                panic!("not nodes");
            };
            assert_eq!(nodes.len(), rf);
            let roles: Vec<Value> = nodes
                .iter()
                .map(|n| {
                    let Value::Map(f) = n else { panic!() };
                    f.iter()
                        .find(|(k, _)| *k == Value::bulk(&b"role"[..]))
                        .unwrap()
                        .1
                        .clone()
                })
                .collect();
            assert_eq!(roles[0], Value::bulk(&b"master"[..]));
            assert_eq!(roles[1], Value::bulk(&b"replica"[..]));
            assert!(
                lists.insert(format!("{nodes:?}")),
                "two shards share a list"
            );
        }
        assert_eq!(covered, i64::from(SLOTS));
    }

    #[test]
    fn info_reports_the_state() {
        let e = epoch((1..=3).map(|n| member(n, true)).collect());
        let text = info(&e, 3, true);
        for line in [
            "cluster_state:ok",
            "cluster_slots_assigned:16384",
            "cluster_slots_ok:16384",
            "cluster_slots_pfail:0",
            "cluster_slots_fail:0",
            "cluster_known_nodes:3",
            "cluster_size:3",
            "cluster_current_epoch:7",
            "cluster_my_epoch:7",
        ] {
            assert!(text.contains(&format!("{line}\r\n")), "{line} in {text}");
        }
        assert!(info(&e, 3, false).starts_with("cluster_state:fail\r\n"));
    }
}
