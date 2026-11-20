//! List commands. The blocking ones (`BLPOP` and the like) stay unsupported: a client would wait
//! on a key that another node owns.

use std::collections::VecDeque;

use bytes::Bytes;
use discus_resp::{Command, Value};
use discus_store::rank_range;

use super::coll::{mutate, view};
use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{check_value, parse_i64};
use crate::node::{ConnState, Node};

type List = VecDeque<Bytes>;
type Reply = Result<Value, CommandError>;

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec::write("LPUSH", -3, lpush).with_fast(),
    CommandSpec::write("RPUSH", -3, rpush).with_fast(),
    CommandSpec::write("LPOP", -2, lpop)
        .with_fast()
        .with_reads_old()
        .with_null_array(),
    CommandSpec::write("RPOP", -2, rpop)
        .with_fast()
        .with_reads_old()
        .with_null_array(),
    CommandSpec::read("LLEN", 2, llen).with_fast(),
    CommandSpec::read("LRANGE", 4, lrange),
    CommandSpec::read("LINDEX", 3, lindex),
    CommandSpec::write("LTRIM", 4, ltrim),
];

#[derive(Clone, Copy)]
enum End {
    Head,
    Tail,
}

fn push(node: &Node, conn: &ConnState, cmd: &Command, end: End) -> Reply {
    for m in &cmd[2..] {
        check_value(node, m)?;
    }
    let grow = cmd[2..].iter().map(|m| m.len()).sum();
    mutate(node, conn.db, &cmd[1], grow, &cmd[2..], |l: &mut List| {
        for m in &cmd[2..] {
            match end {
                End::Head => l.push_front(m.clone()),
                End::Tail => l.push_back(m.clone()),
            }
        }
        Ok(Value::Integer(l.len() as i64))
    })
}

fn lpush(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    push(node, conn, cmd, End::Head)
}

fn rpush(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    push(node, conn, cmd, End::Tail)
}

fn pop(node: &Node, conn: &ConnState, cmd: &Command, end: End) -> Reply {
    // Valkey gives this error for a count that is not an integer too, and before it looks at
    // the key.
    let count = match cmd.len() {
        2 => None,
        3 => match parse_i64(&cmd[2]) {
            Ok(c) if c >= 0 => Some(c as usize),
            _ => {
                return Err(CommandError::Custom(
                    "ERR value is out of range, must be positive".into(),
                ))
            }
        },
        _ => {
            return Err(CommandError::WrongArity(
                String::from_utf8_lossy(&cmd[0]).into_owned(),
            ))
        }
    };
    mutate(node, conn.db, &cmd[1], 0, [], |l: &mut List| {
        // A stored list is never empty, so an empty one is a missing key. `cluster::run` makes
        // the nil of a call with a count a null array.
        if l.is_empty() {
            return Ok(Value::Null);
        }
        let take = |l: &mut List| match end {
            End::Head => l.pop_front(),
            End::Tail => l.pop_back(),
        };
        Ok(match count {
            None => take(l).map_or(Value::Null, Value::Bulk),
            Some(n) => Value::Array(
                std::iter::from_fn(|| take(l))
                    .take(n)
                    .map(Value::Bulk)
                    .collect(),
            ),
        })
    })
}

fn lpop(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    pop(node, conn, cmd, End::Head)
}

fn rpop(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    pop(node, conn, cmd, End::Tail)
}

fn llen(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    view(node, conn.db, &cmd[1], |l: Option<&List>| {
        Value::Integer(l.map_or(0, |l| l.len()) as i64)
    })
}

fn lrange(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let (start, stop) = (parse_i64(&cmd[2])?, parse_i64(&cmd[3])?);
    view(node, conn.db, &cmd[1], |l: Option<&List>| {
        let items = l
            .and_then(|l| Some((l, rank_range(l.len(), start, stop)?)))
            .map(|(l, ranks)| l.range(ranks).cloned().map(Value::Bulk).collect())
            .unwrap_or_default();
        Value::Array(items)
    })
}

fn lindex(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let index = parse_i64(&cmd[2])?;
    view(node, conn.db, &cmd[1], |l: Option<&List>| {
        l.and_then(|l| {
            let i = if index < 0 {
                index.checked_add(l.len() as i64)?
            } else {
                index
            };
            l.get(usize::try_from(i).ok()?).cloned()
        })
        .map_or(Value::Null, Value::Bulk)
    })
}

fn ltrim(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let (start, stop) = (parse_i64(&cmd[2])?, parse_i64(&cmd[3])?);
    mutate(node, conn.db, &cmd[1], 0, [], |l: &mut List| {
        match rank_range(l.len(), start, stop) {
            Some(ranks) => {
                l.truncate(ranks.end() + 1);
                l.drain(..*ranks.start());
            }
            None => l.clear(),
        }
        Ok(Value::ok())
    })
}
