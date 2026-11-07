//! Connection-level commands.

use std::fmt::Write;

use bytes::Bytes;
use discus_resp::{Command, Protocol, Value};

use super::error::CommandError;
use super::table::{names, CommandSpec};
use super::util::{glob_match, parse_i64, upper};
use crate::build;
use crate::node::{ConnState, Node};

/// The Redis version that Discus reports. Clients gate features on it.
pub const REDIS_VERSION: &str = "7.2.0";

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "PING",
        arity: -1,
        handler: ping,
        needs_auth: true,
    },
    CommandSpec {
        name: "ECHO",
        arity: 2,
        handler: echo,
        needs_auth: true,
    },
    CommandSpec {
        name: "QUIT",
        arity: -1,
        handler: quit,
        needs_auth: false,
    },
    CommandSpec {
        name: "AUTH",
        arity: -2,
        handler: auth,
        needs_auth: false,
    },
    CommandSpec {
        name: "HELLO",
        arity: -1,
        handler: hello,
        needs_auth: false,
    },
    CommandSpec {
        name: "SELECT",
        arity: 2,
        handler: select,
        needs_auth: true,
    },
    CommandSpec {
        name: "COMMAND",
        arity: -1,
        handler: command,
        needs_auth: true,
    },
    CommandSpec {
        name: "CLIENT",
        arity: -2,
        handler: client,
        needs_auth: true,
    },
    CommandSpec {
        name: "INFO",
        arity: -1,
        handler: info,
        needs_auth: true,
    },
    CommandSpec {
        name: "CONFIG",
        arity: -2,
        handler: config,
        needs_auth: true,
    },
    CommandSpec {
        name: "CLUSTER",
        arity: -1,
        handler: cluster,
        needs_auth: true,
    },
];

type Reply = Result<Value, CommandError>;

fn ping(_: &Node, _: &mut ConnState, cmd: &Command) -> Result<Value, CommandError> {
    match cmd.len() {
        1 => Ok(Value::simple("PONG")),
        2 => Ok(Value::Bulk(cmd[1].clone())),
        _ => Err(CommandError::WrongArity("PING".into())),
    }
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
        "discus",
        &[
            format!("discus_build:{}", build::BUILD_ID),
            format!("discus_features:{}", build::FEATURES.join(",")),
            format!("discus_tls_backend:{}", build::TLS_BACKEND),
            format!("discus_trust_roots:{}", build::TRUST_ROOTS),
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

fn cluster(_: &Node, _: &mut ConnState, _: &Command) -> Reply {
    Err(CommandError::Custom(
        "ERR Discus is not a Redis Cluster. Any node accepts any key, so clients do not need \
         topology awareness. Use a standalone client."
            .into(),
    ))
}
