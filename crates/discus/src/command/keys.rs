//! Expiry commands.

use discus_resp::{Command, Value};

use super::error::CommandError;
use super::table::CommandSpec;
use super::util::parse_i64;
use crate::node::{ConnState, Node};

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "EXPIRE",
        arity: 3,
        handler: expire,
        needs_auth: true,
    },
    CommandSpec {
        name: "PEXPIRE",
        arity: 3,
        handler: pexpire,
        needs_auth: true,
    },
    CommandSpec {
        name: "EXPIREAT",
        arity: 3,
        handler: expireat,
        needs_auth: true,
    },
    CommandSpec {
        name: "PEXPIREAT",
        arity: 3,
        handler: pexpireat,
        needs_auth: true,
    },
    CommandSpec {
        name: "TTL",
        arity: 2,
        handler: ttl,
        needs_auth: true,
    },
    CommandSpec {
        name: "PTTL",
        arity: 2,
        handler: pttl,
        needs_auth: true,
    },
    CommandSpec {
        name: "PERSIST",
        arity: 2,
        handler: persist,
        needs_auth: true,
    },
];

type Reply = Result<Value, CommandError>;

fn invalid(who: &str) -> CommandError {
    CommandError::Custom(format!("ERR invalid expire time in '{who}' command"))
}

/// Set an absolute deadline. A deadline that is not in the future deletes the key.
fn set_deadline(node: &Node, key: &[u8], deadline_ms: i128) -> Reply {
    let now = node.store.now_ms() as i128;
    node.store.write(key, |slot| match slot {
        None => Ok(Value::Integer(0)),
        Some(_) if deadline_ms <= now => {
            *slot = None;
            Ok(Value::Integer(1))
        }
        Some(e) => {
            e.expires_at_ms = Some(deadline_ms as u64);
            Ok(Value::Integer(1))
        }
    })
}

/// `n` units of `unit_ms` milliseconds, as a signed value that cannot overflow.
fn span(arg: &[u8], unit_ms: i128, who: &str) -> Result<i128, CommandError> {
    let n = parse_i64(arg)? as i128;
    let ms = n * unit_ms;
    if ms.abs() > (i64::MAX / 2) as i128 {
        return Err(invalid(who));
    }
    Ok(ms)
}

fn expire(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let ms = span(&cmd[2], 1000, "expire")?;
    set_deadline(node, &cmd[1], node.store.now_ms() as i128 + ms)
}

fn pexpire(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let ms = span(&cmd[2], 1, "pexpire")?;
    set_deadline(node, &cmd[1], node.store.now_ms() as i128 + ms)
}

fn expireat(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    set_deadline(node, &cmd[1], span(&cmd[2], 1000, "expireat")?)
}

fn pexpireat(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    set_deadline(node, &cmd[1], span(&cmd[2], 1, "pexpireat")?)
}

/// Remaining time: -2 for a missing key, -1 for a key without expiry.
fn remaining(node: &Node, key: &[u8], to_unit: impl Fn(u64) -> i64) -> Reply {
    let now = node.store.now_ms();
    Ok(Value::Integer(node.store.read(key, |e| match e {
        None => -2,
        Some(e) => match e.expires_at_ms {
            None => -1,
            Some(t) => to_unit(t.saturating_sub(now)),
        },
    })))
}

fn ttl(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    remaining(node, &cmd[1], |ms| ((ms + 500) / 1000) as i64)
}

fn pttl(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    remaining(node, &cmd[1], |ms| ms as i64)
}

fn persist(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    node.store.write(&cmd[1], |slot| match slot {
        Some(e) if e.expires_at_ms.is_some() => {
            e.expires_at_ms = None;
            Ok(Value::Integer(1))
        }
        _ => Ok(Value::Integer(0)),
    })
}
