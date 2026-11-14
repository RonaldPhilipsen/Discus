//! Set commands.

use std::collections::HashSet;

use bytes::Bytes;
use discus_resp::{Command, Value};

use super::coll::{mutate, view};
use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{check_value, parse_i64};
use crate::node::{ConnState, Node};

type Set = HashSet<Bytes>;
type Reply = Result<Value, CommandError>;

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec::write("SADD", -3, sadd),
    CommandSpec::write("SREM", -3, srem),
    CommandSpec::read("SMEMBERS", 2, smembers),
    CommandSpec::read("SISMEMBER", 3, sismember),
    CommandSpec::read("SMISMEMBER", -3, smismember),
    CommandSpec::read("SCARD", 2, scard),
    CommandSpec::write("SPOP", -2, spop),
    CommandSpec::read("SRANDMEMBER", -2, srandmember),
];

fn sadd(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    for m in &cmd[2..] {
        check_value(node, m)?;
    }
    let grow = cmd[2..].iter().map(|m| m.len()).sum();
    mutate(node, conn.db, &cmd[1], grow, &cmd[2..], |s: &mut Set| {
        let before = s.len();
        s.extend(cmd[2..].iter().cloned());
        Ok(Value::Integer((s.len() - before) as i64))
    })
}

fn srem(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    mutate(node, conn.db, &cmd[1], 0, [], |s: &mut Set| {
        Ok(Value::Integer(
            cmd[2..].iter().filter(|m| s.remove(*m)).count() as i64,
        ))
    })
}

fn smembers(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |s: Option<&Set>| {
        Value::Array(
            s.into_iter()
                .flatten()
                .map(|m| Value::Bulk(m.clone()))
                .collect(),
        )
    })
}

fn sismember(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |s: Option<&Set>| {
        Value::Integer(s.is_some_and(|s| s.contains(&cmd[2])) as i64)
    })
}

fn smismember(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |s: Option<&Set>| {
        Value::Array(
            cmd[2..]
                .iter()
                .map(|m| Value::Integer(s.is_some_and(|s| s.contains(m)) as i64))
                .collect(),
        )
    })
}

fn scard(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |s: Option<&Set>| {
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

fn spop(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let count = parse_count(cmd)?;
    if count.is_some_and(|c| c < 0) {
        return Err(CommandError::Custom(
            "ERR value is out of range, must be positive".into(),
        ));
    }
    mutate(node, conn.db, &cmd[1], 0, [], |s: &mut Set| {
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

fn srandmember(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let count = parse_count(cmd)?;
    // A negative count repeats members, so cap it: the reply size follows the client's number.
    if count
        .is_some_and(|c| c < 0 && c.unsigned_abs() as usize > node.config.max_collection_members)
    {
        return Err(CommandError::Custom("ERR count is too large".into()));
    }
    view(node, conn.db, &cmd[1], |s: Option<&Set>| {
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
