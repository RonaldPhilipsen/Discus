//! Hash commands.

use std::collections::HashMap;

use bytes::Bytes;
use discus_resp::{Command, Value};

use super::coll::{mutate, view};
use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{check_key, check_value, parse_i64};
use crate::node::{ConnState, Node};

type Hash = HashMap<Bytes, Bytes>;
type Reply = Result<Value, CommandError>;

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec::write("HSET", -4, hset),
    CommandSpec::write("HSETNX", 4, hsetnx),
    CommandSpec::read("HGET", 3, hget),
    CommandSpec::read("HMGET", -3, hmget),
    CommandSpec::read("HGETALL", 2, hgetall),
    CommandSpec::write("HDEL", -3, hdel),
    CommandSpec::read("HEXISTS", 3, hexists),
    CommandSpec::read("HLEN", 2, hlen),
    CommandSpec::write("HINCRBY", 4, hincrby),
    CommandSpec::read("HKEYS", 2, hkeys),
    CommandSpec::read("HVALS", 2, hvals),
];

fn hset(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    if !cmd.len().is_multiple_of(2) {
        return Err(CommandError::WrongArity("HSET".into()));
    }
    let mut grow = 0;
    for pair in cmd[2..].chunks(2) {
        check_key(node, &pair[0])?;
        check_value(node, &pair[1])?;
        grow += pair[0].len() + pair[1].len();
    }
    let fields = cmd[2..].iter().step_by(2);
    mutate(node, conn.db, &cmd[1], grow, fields, |h: &mut Hash| {
        let before = h.len();
        for pair in cmd[2..].chunks(2) {
            h.insert(pair[0].clone(), pair[1].clone());
        }
        Ok(Value::Integer((h.len() - before) as i64))
    })
}

fn hsetnx(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[2])?;
    check_value(node, &cmd[3])?;
    let grow = cmd[2].len() + cmd[3].len();
    mutate(node, conn.db, &cmd[1], grow, [&cmd[2]], |h: &mut Hash| {
        if h.contains_key(&cmd[2]) {
            return Ok(Value::Integer(0));
        }
        h.insert(cmd[2].clone(), cmd[3].clone());
        Ok(Value::Integer(1))
    })
}

fn hget(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |h: Option<&Hash>| {
        h.and_then(|h| h.get(&cmd[2]))
            .map_or(Value::Null, |v| Value::Bulk(v.clone()))
    })
}

fn hmget(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |h: Option<&Hash>| {
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

fn hgetall(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |h: Option<&Hash>| {
        Value::Map(
            h.into_iter()
                .flatten()
                .map(|(k, v)| (Value::Bulk(k.clone()), Value::Bulk(v.clone())))
                .collect(),
        )
    })
}

fn hdel(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    mutate(node, conn.db, &cmd[1], 0, [], |h: &mut Hash| {
        Ok(Value::Integer(
            cmd[2..].iter().filter(|f| h.remove(*f).is_some()).count() as i64,
        ))
    })
}

fn hexists(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |h: Option<&Hash>| {
        Value::Integer(h.is_some_and(|h| h.contains_key(&cmd[2])) as i64)
    })
}

fn hlen(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |h: Option<&Hash>| {
        Value::Integer(h.map_or(0, |h| h.len()) as i64)
    })
}

fn hincrby(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[2])?;
    let delta = parse_i64(&cmd[3])?;
    mutate(
        node,
        conn.db,
        &cmd[1],
        cmd[2].len() + 20,
        [&cmd[2]],
        |h: &mut Hash| {
            let current = match h.get(&cmd[2]) {
                None => 0,
                Some(v) => parse_i64(v)
                    .map_err(|_| CommandError::Custom("ERR hash value is not an integer".into()))?,
            };
            let next = current.checked_add(delta).ok_or(CommandError::Overflow)?;
            h.insert(cmd[2].clone(), Bytes::from(next.to_string()));
            Ok(Value::Integer(next))
        },
    )
}

fn hkeys(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |h: Option<&Hash>| {
        Value::Array(
            h.into_iter()
                .flatten()
                .map(|(k, _)| Value::Bulk(k.clone()))
                .collect(),
        )
    })
}

fn hvals(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |h: Option<&Hash>| {
        Value::Array(
            h.into_iter()
                .flatten()
                .map(|(_, v)| Value::Bulk(v.clone()))
                .collect(),
        )
    })
}
