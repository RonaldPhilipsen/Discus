//! Connection-level commands.

use std::fmt::Write;

use bytes::Bytes;
use discus_placement::{slot, Epoch};
use discus_resp::{Command, Protocol, Value};

use super::access::Access;
use super::error::CommandError;
use super::table::{names, CommandSpec};
use super::util::{glob_match, parse_i64, upper};
use crate::build;
use crate::cluster::topology::{self, TopologyKind};
use crate::node::{ConnState, Node};

/// The Redis version that Discus reports. Clients gate features on it.
pub const REDIS_VERSION: &str = "7.2.0";

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec::connection("PING", -1, ping),
    CommandSpec::connection("ECHO", 2, echo),
    CommandSpec::connection("QUIT", -1, quit).with_access(Access::Public),
    CommandSpec::connection("AUTH", -2, auth).with_access(Access::Public),
    CommandSpec::connection("HELLO", -1, hello).with_access(Access::Public),
    CommandSpec::connection("SELECT", 2, select),
    CommandSpec::connection("COMMAND", -1, command),
    CommandSpec::connection("CLIENT", -2, client),
    CommandSpec::connection("INFO", -1, info).with_access(Access::Read),
    CommandSpec::connection("CONFIG", -2, config).with_access(Access::Read),
    CommandSpec::connection("CLUSTER", -1, cluster),
    CommandSpec::connection("READONLY", 1, readonly),
    CommandSpec::connection("READWRITE", 1, readwrite),
    CommandSpec::connection("ASKING", 1, asking),
];

type Reply = Result<Value, CommandError>;

fn ping(_: &Node, _: &mut ConnState, cmd: &Command) -> Result<Value, CommandError> {
    match cmd.len() {
        1 => Ok(Value::simple("PONG")),
        2 => Ok(Value::Bulk(cmd[1].clone())),
        _ => Err(CommandError::WrongArity("PING".into())),
    }
}

fn readonly(_: &Node, conn: &mut ConnState, _: &Command) -> Reply {
    conn.readonly = true;
    Ok(Value::ok())
}

fn readwrite(_: &Node, conn: &mut ConnState, _: &Command) -> Reply {
    conn.readonly = false;
    Ok(Value::ok())
}

/// Discus never sends `-ASK`, so `ASKING` has no effect.
fn asking(_: &Node, _: &mut ConnState, _: &Command) -> Reply {
    Ok(Value::ok())
}

fn echo(_: &Node, _: &mut ConnState, cmd: &Command) -> Result<Value, CommandError> {
    Ok(Value::Bulk(cmd[1].clone()))
}

fn quit(_: &Node, conn: &mut ConnState, _: &Command) -> Result<Value, CommandError> {
    conn.closing = true;
    Ok(Value::ok())
}

fn auth(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    if cmd.len() > 3 {
        return Err(CommandError::Syntax);
    }
    // The username is an identity hint. The token is the credential.
    let user = (cmd.len() == 3).then(|| &cmd[1][..]);
    node.security
        .authenticate(conn, user, &cmd[cmd.len() - 1])?;
    Ok(Value::ok())
}

fn hello(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let mut protocol = conn.protocol;
    let mut i = 1;
    if cmd.len() > 1 {
        protocol = match parse_i64(&cmd[1]) {
            Ok(2) => Protocol::Resp2,
            Ok(3) => Protocol::Resp3,
            _ => {
                return Err(CommandError::Custom(
                    "NOPROTO unsupported protocol version".into(),
                ))
            }
        };
        i = 2;
    }
    let mut name = None;
    while i < cmd.len() {
        match upper(&cmd[i]).as_str() {
            "AUTH" if i + 2 < cmd.len() => {
                node.security
                    .authenticate(conn, Some(&cmd[i + 1]), &cmd[i + 2])?;
                i += 3;
            }
            "SETNAME" if i + 1 < cmd.len() => {
                name = Some(cmd[i + 1].clone());
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
    }
    if !node.security.is_authenticated(&conn.auth) {
        return Err(CommandError::NoAuth);
    }
    conn.protocol = protocol;
    if name.is_some() {
        conn.name = name;
    }
    let bulk = |s: &str| Value::Bulk(Bytes::copy_from_slice(s.as_bytes()));
    Ok(Value::Map(vec![
        (bulk("server"), bulk("redis")),
        (bulk("version"), bulk(REDIS_VERSION)),
        (
            bulk("proto"),
            Value::Integer(if protocol == Protocol::Resp3 { 3 } else { 2 }),
        ),
        (bulk("id"), Value::Integer(conn.id as i64)),
        (bulk("mode"), bulk("standalone")),
        (bulk("role"), bulk("master")),
        (bulk("modules"), Value::Array(vec![])),
    ]))
}

fn select(_: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    match parse_i64(&cmd[1])? {
        0 => Ok(Value::ok()),
        _ => Err(CommandError::Custom("ERR DB index is out of range".into())),
    }
}

fn command(_: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    match cmd.get(1).map(upper).as_deref() {
        None => Ok(Value::Array(
            names()
                .into_iter()
                .map(|n| Value::bulk(n.to_lowercase().into_bytes()))
                .collect(),
        )),
        Some("COUNT") => Ok(Value::Integer(names().len() as i64)),
        Some("DOCS") | Some("INFO") => Ok(Value::Array(vec![])),
        Some(_) => Err(CommandError::Syntax),
    }
}

fn client(_: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    match upper(&cmd[1]).as_str() {
        "SETNAME" if cmd.len() == 3 => {
            conn.name = (!cmd[2].is_empty()).then(|| cmd[2].clone());
            Ok(Value::ok())
        }
        "GETNAME" if cmd.len() == 2 => Ok(conn.name.clone().map_or(Value::Null, Value::Bulk)),
        "ID" if cmd.len() == 2 => Ok(Value::Integer(conn.id as i64)),
        "SETINFO" if cmd.len() == 4 => Ok(Value::ok()),
        _ => Err(CommandError::Custom(format!(
            "ERR unknown subcommand or wrong number of arguments for 'CLIENT {}'",
            upper(&cmd[1])
        ))),
    }
}

fn info(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let want = cmd.get(1).map(upper);
    let wants = |section: &str| match want.as_deref() {
        None | Some("ALL") | Some("DEFAULT") | Some("EVERYTHING") => true,
        Some(w) => w.eq_ignore_ascii_case(section),
    };
    let mut out = String::new();
    let mut section = |name: &str, lines: &[String]| {
        if wants(name) {
            if !out.is_empty() {
                out.push_str("\r\n");
            }
            let _ = write!(out, "# {name}\r\n");
            for l in lines {
                let _ = write!(out, "{l}\r\n");
            }
        }
    };
    section(
        "server",
        &[
            format!("redis_version:{REDIS_VERSION}"),
            "redis_mode:standalone".into(),
            format!("uptime_in_seconds:{}", node.started_at.elapsed().as_secs()),
            format!("process_id:{}", std::process::id()),
        ],
    );
    section(
        "clients",
        &[format!("connected_clients:{}", node.connected_clients())],
    );
    section(
        "memory",
        &[
            format!("used_memory:{}", node.store.used_memory()),
            format!("maxmemory:{}", node.config.max_memory),
        ],
    );
    section(
        "stats",
        &[
            format!("total_connections_received:{}", node.total_connections()),
            format!("evicted_keys:{}", node.store.evictions()),
        ],
    );
    section(
        "keyspace",
        &[format!("db0:keys={},expires=0,avg_ttl=0", node.store.len())],
    );
    section(
        "Cluster",
        &[format!(
            "cluster_enabled:{}",
            u8::from(node.config.topology_enabled())
        )],
    );
    section(
        "discus",
        &[
            format!("discus_build:{}", build::BUILD_ID),
            format!("discus_features:{}", build::FEATURES.join(",")),
            format!("discus_tls_backend:{}", build::TLS_BACKEND),
            format!("discus_trust_roots:{}", build::TRUST_ROOTS),
            format!("discus_node_id:{}", node.id),
            format!("discus_cluster_id:{}", node.cluster_id()),
            format!("discus_epoch:{}", node.epochs().current().id()),
            format!("discus_members:{}", node.epochs().current().members().len()),
            format!(
                "discus_replication_factor:{}",
                node.config.replication_factor
            ),
            format!("discus_hlc_pinned:{}", u8::from(node.hlc.is_pinned())),
            format!("discus_tombstones:{}", node.store.tombstones()),
            format!("discus_hlc_refusals:{}", node.hlc.refusals()),
            format!(
                "discus_cluster_addr:{}",
                node.cluster_addr()
                    .map_or("none".to_string(), |a| a.to_string())
            ),
            format!(
                "discus_rpc_addr:{}",
                node.rpc_addr()
                    .map_or("none".to_string(), |a| a.to_string())
            ),
            format!(
                "discus_redirect_addr:{}",
                node.config
                    .redirect_listen
                    .map_or("none".to_string(), |a| a.to_string())
            ),
            format!(
                "discus_client_addr:{}",
                node.client_addr()
                    .map_or("none".to_string(), |a| a.to_string())
            ),
            format!("discus_peers:{}", node.config.peers.len()),
            format!(
                "discus_membership:{}",
                match (node.membership(), node.link()) {
                    (Some(_), _) => "gossip",
                    (None, Some(_)) => "static",
                    (None, None) => "standalone",
                }
            ),
            format!(
                "discus_member_state:{}",
                node.membership()
                    .map_or("NORMAL", |m| m.local().state.name())
            ),
            format!(
                "discus_incarnation:{}",
                node.membership().map_or(0, |m| m.local().incarnation)
            ),
        ],
    );
    Ok(Value::Bulk(Bytes::from(out)))
}

fn config(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    if upper(&cmd[1]) != "GET" || cmd.len() != 3 {
        return Err(CommandError::Custom(
            "ERR unknown subcommand or wrong number of arguments for 'CONFIG'".into(),
        ));
    }
    let known = [
        ("maxmemory", node.config.max_memory.to_string()),
        ("databases", "1".to_string()),
        ("maxclients", "10000".to_string()),
    ];
    let pattern = cmd[2].to_ascii_lowercase();
    Ok(Value::Map(
        known
            .into_iter()
            .filter(|(name, _)| glob_match(&pattern, name.as_bytes()))
            .map(|(name, value)| {
                (
                    Value::bulk(name.as_bytes().to_vec()),
                    Value::bulk(value.into_bytes()),
                )
            })
            .collect(),
    ))
}

fn cluster(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    if !node.config.topology_enabled() {
        return Err(CommandError::Custom(
            "ERR Discus is not a Redis Cluster. Any node accepts any key, so clients do not need \
             topology awareness. Use a standalone client."
                .into(),
        ));
    }
    let Some(sub) = cmd.get(1).map(upper) else {
        return Err(CommandError::WrongArity("CLUSTER".into()));
    };
    let kind = match sub.as_str() {
        "SLOTS" => Some(TopologyKind::Slots),
        "SHARDS" => Some(TopologyKind::Shards),
        "NODES" => Some(TopologyKind::Nodes),
        "INFO" => Some(TopologyKind::Info),
        _ => None,
    };
    let arity = match sub.as_str() {
        "KEYSLOT" => 3,
        _ => 2,
    };
    let known = kind.is_some() || matches!(sub.as_str(), "KEYSLOT" | "MYID");
    if known && cmd.len() != arity {
        return Err(CommandError::WrongArity(format!("CLUSTER|{sub}")));
    }
    if let Some(kind) = kind {
        return match standalone_topology(node, conn, kind) {
            Some(reply) => reply,
            None => node
                .topology(kind, conn.protocol)
                .map(|reply| (*reply).clone()),
        };
    }
    match sub.as_str() {
        "KEYSLOT" => Ok(Value::Integer(i64::from(slot(&cmd[2])))),
        "MYID" => Ok(Value::bulk(topology::node_id(node.id).into_bytes())),
        "COUNTKEYSINSLOT" | "GETKEYSINSLOT" => Err(CommandError::Custom(format!(
            "ERR Discus does not support CLUSTER {sub}, because it keeps no index by slot"
        ))),
        _ => Err(CommandError::Custom(format!(
            "ERR Discus places slots by itself. CLUSTER {sub} is not supported"
        ))),
    }
}

/// A single node without a cluster and without an advertised address names the local address
/// of the connection that asks (spec "CLUSTER and connection commands"). That reply depends on
/// the connection, so it is not cached. `None` when the node advertises an address.
fn standalone_topology(node: &Node, conn: &ConnState, kind: TopologyKind) -> Option<Reply> {
    let local = conn.local_addr?;
    let epoch = node.epochs().current().clone();
    let [me] = epoch.members() else {
        return None;
    };
    if me.id != node.id || me.client_addr.is_some() || !node.is_normal() {
        return None;
    }
    let mut me = me.clone();
    me.client_addr = Some(local);
    me.client_host = node.host_for(local);
    let alone = Epoch::new(epoch.id(), epoch.cluster_id(), vec![me]).ok()?;
    let rf = node.config.replication_factor;
    Some(topology::build(
        kind,
        &alone,
        rf,
        conn.protocol,
        node.id,
        true,
    ))
}
