//! Set commands.

use std::collections::HashSet;

use bytes::Bytes;
use discus_resp::{Command, Value};

use super::coll::{guard, mutate, view};
use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{check_key, check_value, parse_i64};
use crate::node::{ConnState, Node};

type Set = HashSet<Bytes>;
type Reply = Result<Value, CommandError>;

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "SADD",
        arity: -3,
        handler: sadd,
        needs_auth: true,
    },
    CommandSpec {
        name: "SREM",
        arity: -3,
        handler: srem,
        needs_auth: true,
    },
    CommandSpec {
        name: "SMEMBERS",
        arity: 2,
        handler: smembers,
        needs_auth: true,
    },
    CommandSpec {
        name: "SISMEMBER",
        arity: 3,
        handler: sismember,
        needs_auth: true,
    },
    CommandSpec {
        name: "SMISMEMBER",
        arity: -3,
        handler: smismember,
        needs_auth: true,
    },
    CommandSpec {
        name: "SCARD",
        arity: 2,
        handler: scard,
        needs_auth: true,
    },
    CommandSpec {
        name: "SPOP",
        arity: -2,
        handler: spop,
        needs_auth: true,
    },
    CommandSpec {
        name: "SRANDMEMBER",
        arity: -2,
        handler: srandmember,
        needs_auth: true,
    },
];

fn sadd(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    for m in &cmd[2..] {
        check_value(node, m)?;
    }
    let grow = cmd[2..].iter().map(|m| m.len()).sum();
    mutate(node, &cmd[1], grow, |s: &mut Set| {
        let mut added = 0;
        for m in &cmd[2..] {
            if !s.contains(m) {
                guard(node, s.len() + 1)?;
                s.insert(m.clone());
                added += 1;
            }
        }
        Ok(Value::Integer(added))
    })
}

fn srem(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    mutate(node, &cmd[1], 0, |s: &mut Set| {
        Ok(Value::Integer(
            cmd[2..].iter().filter(|m| s.remove(*m)).count() as i64,
        ))
    })
}

fn smembers(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |s: Option<&Set>| {
        Value::Array(
            s.into_iter()
                .flatten()
                .map(|m| Value::Bulk(m.clone()))
                .collect(),
        )
    })
}

fn sismember(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |s: Option<&Set>| {
        Value::Integer(s.is_some_and(|s| s.contains(&cmd[2])) as i64)
    })
}

fn smismember(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |s: Option<&Set>| {
        Value::Array(
            cmd[2..]
                .iter()
                .map(|m| Value::Integer(s.is_some_and(|s| s.contains(m)) as i64))
                .collect(),
        )
    })
}

fn scard(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |s: Option<&Set>| {
        Value::Integer(s.map_or(0, |s| s.len()) as i64)
    })
}

fn parse_count(cmd: &Command) -> Result<Option<i64>, CommandError> {
    match cmd.len() {
        2 => Ok(None),
        3 => Ok(Some(parse_i64(&cmd[2])?)),
        _ => Err(CommandError::Syntax),
    }
}

/// Pick `n` distinct members at random.
fn pick_distinct(s: &Set, n: usize) -> Vec<Bytes> {
    let mut all: Vec<&Bytes> = s.iter().collect();
    let n = n.min(all.len());
    for i in 0..n {
        let j = fastrand::usize(i..all.len());
        all.swap(i, j);
    }
    all[..n].iter().map(|m| (*m).clone()).collect()
}

fn spop(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let count = parse_count(cmd)?;
    if count.is_some_and(|c| c < 0) {
        return Err(CommandError::Custom(
            "ERR value is out of range, must be positive".into(),
        ));
    }
    mutate(node, &cmd[1], 0, |s: &mut Set| {
        let popped = pick_distinct(s, count.unwrap_or(1) as usize);
        for m in &popped {
            s.remove(m);
        }
        Ok(match count {
            None => popped.into_iter().next().map_or(Value::Null, Value::Bulk),
            Some(_) => Value::Array(popped.into_iter().map(Value::Bulk).collect()),
        })
    })
}

fn srandmember(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let count = parse_count(cmd)?;
    view(node, &cmd[1], |s: Option<&Set>| {
        let empty = Set::new();
        let s = s.unwrap_or(&empty);
        match count {
            None => pick_distinct(s, 1)
                .into_iter()
                .next()
                .map_or(Value::Null, Value::Bulk),
            Some(c) if c >= 0 => Value::Array(
                pick_distinct(s, c as usize)
                    .into_iter()
                    .map(Value::Bulk)
                    .collect(),
            ),
            Some(c) => {
                let all: Vec<&Bytes> = s.iter().collect();
                let n = if all.is_empty() {
                    0
                } else {
                    c.unsigned_abs() as usize
                };
                Value::Array(
                    (0..n)
                        .map(|_| Value::Bulk(all[fastrand::usize(..all.len())].clone()))
                        .collect(),
                )
            }
        }
    })
}
