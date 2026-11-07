//! Hash commands.

use std::collections::HashMap;

use bytes::Bytes;
use discus_resp::{Command, Value};

use super::coll::{guard, mutate, view};
use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{check_key, check_value, parse_i64};
use crate::node::{ConnState, Node};

type Hash = HashMap<Bytes, Bytes>;
type Reply = Result<Value, CommandError>;

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "HSET",
        arity: -4,
        handler: hset,
        needs_auth: true,
    },
    CommandSpec {
        name: "HSETNX",
        arity: 4,
        handler: hsetnx,
        needs_auth: true,
    },
    CommandSpec {
        name: "HGET",
        arity: 3,
        handler: hget,
        needs_auth: true,
    },
    CommandSpec {
        name: "HMGET",
        arity: -3,
        handler: hmget,
        needs_auth: true,
    },
    CommandSpec {
        name: "HGETALL",
        arity: 2,
        handler: hgetall,
        needs_auth: true,
    },
    CommandSpec {
        name: "HDEL",
        arity: -3,
        handler: hdel,
        needs_auth: true,
    },
    CommandSpec {
        name: "HEXISTS",
        arity: 3,
        handler: hexists,
        needs_auth: true,
    },
    CommandSpec {
        name: "HLEN",
        arity: 2,
        handler: hlen,
        needs_auth: true,
    },
    CommandSpec {
        name: "HINCRBY",
        arity: 4,
        handler: hincrby,
        needs_auth: true,
    },
    CommandSpec {
        name: "HKEYS",
        arity: 2,
        handler: hkeys,
        needs_auth: true,
    },
    CommandSpec {
        name: "HVALS",
        arity: 2,
        handler: hvals,
        needs_auth: true,
    },
];

fn hset(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    if !cmd.len().is_multiple_of(2) {
        return Err(CommandError::WrongArity("HSET".into()));
    }
    check_key(node, &cmd[1])?;
    let mut grow = 0;
    for pair in cmd[2..].chunks(2) {
        check_key(node, &pair[0])?;
        check_value(node, &pair[1])?;
        grow += pair[0].len() + pair[1].len();
    }
    mutate(node, &cmd[1], grow, |h: &mut Hash| {
        let mut added = 0;
        for pair in cmd[2..].chunks(2) {
            if !h.contains_key(&pair[0]) {
                guard(node, h.len() + 1)?;
                added += 1;
            }
            h.insert(pair[0].clone(), pair[1].clone());
        }
        Ok(Value::Integer(added))
    })
}

fn hsetnx(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    check_key(node, &cmd[2])?;
    check_value(node, &cmd[3])?;
    mutate(
        node,
        &cmd[1],
        cmd[2].len() + cmd[3].len(),
        |h: &mut Hash| {
            if h.contains_key(&cmd[2]) {
                return Ok(Value::Integer(0));
            }
            guard(node, h.len() + 1)?;
            h.insert(cmd[2].clone(), cmd[3].clone());
            Ok(Value::Integer(1))
        },
    )
}

fn hget(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |h: Option<&Hash>| {
        h.and_then(|h| h.get(&cmd[2]))
            .map_or(Value::Null, |v| Value::Bulk(v.clone()))
    })
}

fn hmget(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |h: Option<&Hash>| {
        Value::Array(
            cmd[2..]
                .iter()
                .map(|f| {
                    h.and_then(|h| h.get(f))
                        .map_or(Value::Null, |v| Value::Bulk(v.clone()))
                })
                .collect(),
        )
    })
}

fn hgetall(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |h: Option<&Hash>| {
        Value::Map(
            h.into_iter()
                .flatten()
                .map(|(k, v)| (Value::Bulk(k.clone()), Value::Bulk(v.clone())))
                .collect(),
        )
    })
}

fn hdel(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    mutate(node, &cmd[1], 0, |h: &mut Hash| {
        Ok(Value::Integer(
            cmd[2..].iter().filter(|f| h.remove(*f).is_some()).count() as i64,
        ))
    })
}

fn hexists(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |h: Option<&Hash>| {
        Value::Integer(h.is_some_and(|h| h.contains_key(&cmd[2])) as i64)
    })
}

fn hlen(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |h: Option<&Hash>| {
        Value::Integer(h.map_or(0, |h| h.len()) as i64)
    })
}

fn hincrby(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    check_key(node, &cmd[2])?;
    let delta = parse_i64(&cmd[3])?;
    mutate(node, &cmd[1], cmd[2].len() + 20, |h: &mut Hash| {
        let current = match h.get(&cmd[2]) {
            None => 0,
            Some(v) => parse_i64(v)
                .map_err(|_| CommandError::Custom("ERR hash value is not an integer".into()))?,
        };
        let next = current.checked_add(delta).ok_or(CommandError::Overflow)?;
        if !h.contains_key(&cmd[2]) {
            guard(node, h.len() + 1)?;
        }
        h.insert(cmd[2].clone(), Bytes::from(next.to_string()));
        Ok(Value::Integer(next))
    })
}

fn hkeys(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |h: Option<&Hash>| {
        Value::Array(
            h.into_iter()
                .flatten()
                .map(|(k, _)| Value::Bulk(k.clone()))
                .collect(),
        )
    })
}

fn hvals(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |h: Option<&Hash>| {
        Value::Array(
            h.into_iter()
                .flatten()
                .map(|(_, v)| Value::Bulk(v.clone()))
                .collect(),
        )
    })
}
