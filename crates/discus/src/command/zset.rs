//! Sorted set commands.

use std::ops::Bound;

use bytes::Bytes;
use discus_resp::{Command, Protocol, Value};
use discus_store::ZSet;

use super::coll::{guard, mutate, view};
use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{check_key, check_value, parse_f64, parse_i64, upper};
use crate::node::{ConnState, Node};

type Reply = Result<Value, CommandError>;

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "ZADD",
        arity: -4,
        handler: zadd,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZSCORE",
        arity: 3,
        handler: zscore,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZMSCORE",
        arity: -3,
        handler: zmscore,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZREM",
        arity: -3,
        handler: zrem,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZCARD",
        arity: 2,
        handler: zcard,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZRANGE",
        arity: -4,
        handler: zrange,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZREVRANGE",
        arity: -4,
        handler: zrevrange,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZRANGEBYSCORE",
        arity: -4,
        handler: zrangebyscore,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZINCRBY",
        arity: 4,
        handler: zincrby,
        needs_auth: true,
    },
    CommandSpec {
        name: "ZCOUNT",
        arity: 4,
        handler: zcount,
        needs_auth: true,
    },
];

fn score_value(s: f64) -> Value {
    Value::Double(s)
}

/// A bound such as `1.5`, `(1.5`, `-inf`, or `+inf`.
fn parse_bound(b: &[u8]) -> Result<Bound<f64>, CommandError> {
    let min_max = || CommandError::Custom("ERR min or max is not a float".into());
    let (exclusive, digits) = match b.strip_prefix(b"(") {
        Some(rest) => (true, rest),
        None => (false, b),
    };
    let f = parse_f64(digits).map_err(|_| min_max())?;
    Ok(if exclusive {
        Bound::Excluded(f)
    } else {
        Bound::Included(f)
    })
}

/// Shape ranked members for the reply. RESP3 nests `[member, score]` pairs.
fn members_reply(items: Vec<(&Bytes, f64)>, with_scores: bool, protocol: Protocol) -> Value {
    if !with_scores {
        return Value::Array(
            items
                .into_iter()
                .map(|(m, _)| Value::Bulk(m.clone()))
                .collect(),
        );
    }
    if protocol == Protocol::Resp3 {
        return Value::Array(
            items
                .into_iter()
                .map(|(m, s)| Value::Array(vec![Value::Bulk(m.clone()), score_value(s)]))
                .collect(),
        );
    }
    Value::Array(
        items
            .into_iter()
            .flat_map(|(m, s)| {
                [
                    Value::Bulk(m.clone()),
                    Value::Bulk(Bytes::from(format!("{s}"))),
                ]
            })
            .collect(),
    )
}

fn zadd(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    let (mut nx, mut xx, mut ch) = (false, false, false);
    let mut i = 2;
    while i < cmd.len() {
        match upper(&cmd[i]).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "CH" => ch = true,
            _ => break,
        }
        i += 1;
    }
    if nx && xx {
        return Err(CommandError::Custom(
            "ERR XX and NX options at the same time are not compatible".into(),
        ));
    }
    let rest = &cmd[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return Err(CommandError::Syntax);
    }
    let mut pairs = Vec::with_capacity(rest.len() / 2);
    let mut grow = 0;
    for p in rest.chunks(2) {
        check_value(node, &p[1])?;
        grow += p[1].len() + 8;
        pairs.push((parse_f64(&p[0])?, p[1].clone()));
    }
    mutate(node, &cmd[1], grow, |z: &mut ZSet| {
        if !xx {
            let fresh: std::collections::HashSet<&Bytes> = pairs
                .iter()
                .map(|(_, m)| m)
                .filter(|m| z.score(m).is_none())
                .collect();
            guard(node, z.len() + fresh.len())?;
        }
        let (mut added, mut changed) = (0, 0);
        for (score, member) in pairs {
            match z.score(&member) {
                None if xx => {}
                None => {
                    z.insert(member, score);
                    added += 1;
                }
                Some(_) if nx => {}
                Some(old) if old != score => {
                    z.insert(member, score);
                    changed += 1;
                }
                Some(_) => {}
            }
        }
        Ok(Value::Integer(if ch { added + changed } else { added }))
    })
}

fn zscore(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |z: Option<&ZSet>| {
        z.and_then(|z| z.score(&cmd[2]))
            .map_or(Value::Null, score_value)
    })
}

fn zmscore(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |z: Option<&ZSet>| {
        Value::Array(
            cmd[2..]
                .iter()
                .map(|m| z.and_then(|z| z.score(m)).map_or(Value::Null, score_value))
                .collect(),
        )
    })
}

fn zrem(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    mutate(node, &cmd[1], 0, |z: &mut ZSet| {
        Ok(Value::Integer(
            cmd[2..].iter().filter(|m| z.remove(m)).count() as i64,
        ))
    })
}

fn zcard(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    view(node, &cmd[1], |z: Option<&ZSet>| {
        Value::Integer(z.map_or(0, |z| z.len()) as i64)
    })
}

fn ranked(node: &Node, conn: &ConnState, cmd: &Command, reverse: bool) -> Reply {
    let with_scores = match cmd.len() {
        4 => false,
        5 if upper(&cmd[4]) == "WITHSCORES" => true,
        _ => return Err(CommandError::Syntax),
    };
    let (start, stop) = (parse_i64(&cmd[2])?, parse_i64(&cmd[3])?);
    view(node, &cmd[1], |z: Option<&ZSet>| {
        let items = z.map(|z| z.range(start, stop, reverse)).unwrap_or_default();
        members_reply(items, with_scores, conn.protocol)
    })
}

fn zrange(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    ranked(node, conn, cmd, false)
}

fn zrevrange(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    ranked(node, conn, cmd, true)
}

fn zrangebyscore(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let (min, max) = (parse_bound(&cmd[2])?, parse_bound(&cmd[3])?);
    let (mut with_scores, mut limit) = (false, None);
    let mut i = 4;
    while i < cmd.len() {
        match upper(&cmd[i]).as_str() {
            "WITHSCORES" => with_scores = true,
            "LIMIT" if i + 2 < cmd.len() => {
                limit = Some((parse_i64(&cmd[i + 1])?, parse_i64(&cmd[i + 2])?));
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    view(node, &cmd[1], |z: Option<&ZSet>| {
        let mut items = z.map(|z| z.range_by_score(min, max)).unwrap_or_default();
        if let Some((offset, count)) = limit {
            let offset = offset.max(0) as usize;
            let take = if count < 0 {
                usize::MAX
            } else {
                count as usize
            };
            items = items.into_iter().skip(offset).take(take).collect();
        }
        members_reply(items, with_scores, conn.protocol)
    })
}

fn zincrby(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    check_value(node, &cmd[3])?;
    let delta = parse_f64(&cmd[2])?;
    mutate(node, &cmd[1], cmd[3].len() + 8, |z: &mut ZSet| {
        let next = z.score(&cmd[3]).unwrap_or(0.0) + delta;
        if next.is_nan() {
            return Err(CommandError::Custom(
                "ERR resulting score is not a number (NaN)".into(),
            ));
        }
        if z.score(&cmd[3]).is_none() {
            guard(node, z.len() + 1)?;
        }
        z.insert(cmd[3].clone(), next);
        Ok(score_value(next))
    })
}

fn zcount(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let (min, max) = (parse_bound(&cmd[2])?, parse_bound(&cmd[3])?);
    view(node, &cmd[1], |z: Option<&ZSet>| {
        Value::Integer(z.map_or(0, |z| z.count(min, max)) as i64)
    })
}
