//! String commands.

use bytes::Bytes;
use discus_resp::{Command, Value};
use discus_store::{Entry, Value as Stored};

use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{admit, check_key, check_value, parse_f64, parse_i64, upper};
use crate::node::{ConnState, Node};

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "GET",
        arity: 2,
        handler: get,
        needs_auth: true,
    },
    CommandSpec {
        name: "SET",
        arity: -3,
        handler: set,
        needs_auth: true,
    },
    CommandSpec {
        name: "GETDEL",
        arity: 2,
        handler: getdel,
        needs_auth: true,
    },
    CommandSpec {
        name: "GETEX",
        arity: -2,
        handler: getex,
        needs_auth: true,
    },
    CommandSpec {
        name: "MGET",
        arity: -2,
        handler: mget,
        needs_auth: true,
    },
    CommandSpec {
        name: "MSET",
        arity: -3,
        handler: mset,
        needs_auth: true,
    },
    CommandSpec {
        name: "INCR",
        arity: -2,
        handler: incr,
        needs_auth: true,
    },
    CommandSpec {
        name: "DECR",
        arity: -2,
        handler: decr,
        needs_auth: true,
    },
    CommandSpec {
        name: "INCRBY",
        arity: -3,
        handler: incrby,
        needs_auth: true,
    },
    CommandSpec {
        name: "DECRBY",
        arity: -3,
        handler: decrby,
        needs_auth: true,
    },
    CommandSpec {
        name: "INCRBYFLOAT",
        arity: 3,
        handler: incrbyfloat,
        needs_auth: true,
    },
    CommandSpec {
        name: "APPEND",
        arity: 3,
        handler: append,
        needs_auth: true,
    },
    CommandSpec {
        name: "STRLEN",
        arity: 2,
        handler: strlen,
        needs_auth: true,
    },
    CommandSpec {
        name: "DEL",
        arity: -2,
        handler: del,
        needs_auth: true,
    },
    CommandSpec {
        name: "EXISTS",
        arity: -2,
        handler: exists,
        needs_auth: true,
    },
    CommandSpec {
        name: "TYPE",
        arity: 2,
        handler: type_,
        needs_auth: true,
    },
];

type Reply = Result<Value, CommandError>;

fn new_entry(value: Bytes, expires_at_ms: Option<u64>) -> Entry {
    Entry::new(Stored::Str(value), expires_at_ms)
}

fn get(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    node.store.read(&cmd[1], |e| match e {
        None => Ok(Value::Null),
        Some(Entry {
            value: Stored::Str(b),
            ..
        }) => Ok(Value::Bulk(b.clone())),
        Some(_) => Err(CommandError::WrongType),
    })
}

/// An expiry option such as `EX 10`, as an absolute deadline.
#[derive(Clone, Copy)]
pub enum Expiry {
    At(u64),
    Keep,
    Persist,
}

/// Convert `EX`, `PX`, `EXAT`, or `PXAT` and its argument into an absolute deadline.
pub fn deadline(option: &str, arg: &[u8], now_ms: u64, who: &str) -> Result<u64, CommandError> {
    let n = parse_i64(arg)?;
    let invalid = || CommandError::Custom(format!("ERR invalid expire time in '{who}' command"));
    if n <= 0 {
        return Err(invalid());
    }
    let n = n as u64;
    let ms = match option {
        "EX" => n.checked_mul(1000).and_then(|v| v.checked_add(now_ms)),
        "PX" => n.checked_add(now_ms),
        "EXAT" => n.checked_mul(1000),
        "PXAT" => Some(n),
        _ => None,
    };
    ms.ok_or_else(invalid)
}

fn set(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    check_value(node, &cmd[2])?;
    let (mut nx, mut xx, mut get_old) = (false, false, false);
    let mut expiry: Option<Expiry> = None;
    let now = node.store.now_ms();
    let mut i = 3;
    while i < cmd.len() {
        let opt = upper(&cmd[i]);
        match opt.as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GET" => get_old = true,
            "KEEPTTL" if expiry.is_none() => expiry = Some(Expiry::Keep),
            "EX" | "PX" | "EXAT" | "PXAT" if expiry.is_none() => {
                let arg = cmd.get(i + 1).ok_or(CommandError::Syntax)?;
                expiry = Some(Expiry::At(deadline(&opt, arg, now, "set")?));
                i += 1;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    if nx && xx {
        return Err(CommandError::Syntax);
    }
    admit(node, cmd[1].len() + cmd[2].len())?;
    let value = cmd[2].clone();
    node.write(&cmd[1], node.stamp()?, |slot| {
        let old = match slot.as_ref() {
            None => None,
            Some(Entry {
                value: Stored::Str(b),
                ..
            }) => Some(b.clone()),
            Some(_) if get_old => return Err(CommandError::WrongType),
            Some(_) => None,
        };
        let exists = slot.is_some();
        let skipped = (nx && exists) || (xx && !exists);
        let reply = |old: Option<Bytes>| match (get_old, old) {
            (true, Some(b)) => Value::Bulk(b),
            (true, None) => Value::Null,
            (false, _) => Value::ok(),
        };
        if skipped {
            return Ok(if get_old { reply(old) } else { Value::Null });
        }
        let expires = match expiry {
            Some(Expiry::At(t)) => Some(t),
            Some(Expiry::Keep) => slot.as_ref().and_then(|e| e.expires_at_ms),
            _ => None,
        };
        slot.set(Some(new_entry(value, expires)));
        Ok(reply(old))
    })
}

fn getdel(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    node.write(&cmd[1], node.stamp()?, |slot| {
        match slot.as_ref() {
            None => return Ok(Value::Null),
            Some(Entry {
                value: Stored::Str(_),
                ..
            }) => {}
            Some(_) => return Err(CommandError::WrongType),
        }
        Ok(match slot.take() {
            Some(Entry {
                value: Stored::Str(b),
                ..
            }) => Value::Bulk(b),
            _ => Value::Null,
        })
    })
}

fn getex(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let now = node.store.now_ms();
    let expiry = match cmd.len() {
        2 => None,
        3 if upper(&cmd[2]) == "PERSIST" => Some(Expiry::Persist),
        4 => {
            let opt = upper(&cmd[2]);
            if !matches!(opt.as_str(), "EX" | "PX" | "EXAT" | "PXAT") {
                return Err(CommandError::Syntax);
            }
            Some(Expiry::At(deadline(&opt, &cmd[3], now, "getex")?))
        }
        _ => return Err(CommandError::Syntax),
    };
    let deadline = match expiry {
        Some(Expiry::At(t)) => Some(Some(t)),
        Some(Expiry::Persist) => Some(None),
        _ => None,
    };
    node.write(&cmd[1], node.stamp()?, |slot| {
        let reply = match slot.as_ref() {
            None => return Ok(Value::Null),
            Some(Entry {
                value: Stored::Str(b),
                ..
            }) => Value::Bulk(b.clone()),
            Some(_) => return Err(CommandError::WrongType),
        };
        if let Some(d) = deadline {
            if slot.as_ref().is_some_and(|e| e.expires_at_ms != d) {
                if let Some(e) = slot.as_mut() {
                    e.expires_at_ms = d;
                }
            }
        }
        Ok(reply)
    })
}

fn mget(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let values = cmd[1..]
        .iter()
        .map(|key| {
            node.store.read(key, |e| match e {
                Some(Entry {
                    value: Stored::Str(b),
                    ..
                }) => Value::Bulk(b.clone()),
                _ => Value::Null,
            })
        })
        .collect();
    Ok(Value::Array(values))
}

fn mset(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    if cmd.len().is_multiple_of(2) {
        return Err(CommandError::WrongArity("MSET".into()));
    }
    for pair in cmd[1..].chunks(2) {
        check_key(node, &pair[0])?;
        check_value(node, &pair[1])?;
    }
    // One stamp for the whole command. Each key still has its own version.
    let stamp = node.stamp()?;
    for pair in cmd[1..].chunks(2) {
        admit(node, pair[0].len() + pair[1].len())?;
        node.write(&pair[0], stamp, |slot| {
            slot.set(Some(new_entry(pair[1].clone(), None)))
        });
    }
    Ok(Value::ok())
}

/// Add `delta` to the integer at `key`. A missing key counts as zero. The TTL is kept.
fn add_integer(node: &Node, key: &Bytes, delta: i64) -> Reply {
    check_key(node, key)?;
    admit(node, key.len() + 20)?;
    node.write(key, node.stamp()?, |slot| {
        let current = match slot.as_ref() {
            None => 0,
            Some(Entry {
                value: Stored::Str(b),
                ..
            }) => parse_i64(b)?,
            Some(_) => return Err(CommandError::WrongType),
        };
        let next = current.checked_add(delta).ok_or(CommandError::Overflow)?;
        let expires = slot.as_ref().and_then(|e| e.expires_at_ms);
        slot.set(Some(new_entry(Bytes::from(next.to_string()), expires)));
        Ok(Value::Integer(next))
    })
}

/// The table admits `INCR key EX ...` for counter keys (spec section 10.4). A normal key keeps
/// the Redis arity.
fn exact_arity(cmd: &Command, arity: usize) -> Result<(), CommandError> {
    if cmd.len() != arity {
        return Err(CommandError::WrongArity(upper(&cmd[0])));
    }
    Ok(())
}

fn incr(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    exact_arity(cmd, 2)?;
    add_integer(node, &cmd[1], 1)
}

fn decr(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    exact_arity(cmd, 2)?;
    add_integer(node, &cmd[1], -1)
}

fn incrby(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    exact_arity(cmd, 3)?;
    add_integer(node, &cmd[1], parse_i64(&cmd[2])?)
}

fn decrby(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    exact_arity(cmd, 3)?;
    let delta = parse_i64(&cmd[2])?
        .checked_neg()
        .ok_or(CommandError::Overflow)?;
    add_integer(node, &cmd[1], delta)
}

fn incrbyfloat(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    admit(node, cmd[1].len() + 32)?;
    let delta = parse_f64(&cmd[2])?;
    node.write(&cmd[1], node.stamp()?, |slot| {
        let current = match slot.as_ref() {
            None => 0.0,
            Some(Entry {
                value: Stored::Str(b),
                ..
            }) => parse_f64(b)?,
            Some(_) => return Err(CommandError::WrongType),
        };
        let next = current + delta;
        if !next.is_finite() {
            return Err(CommandError::Custom(
                "ERR increment would produce NaN or Infinity".into(),
            ));
        }
        let text = Bytes::from(format!("{next}"));
        let expires = slot.as_ref().and_then(|e| e.expires_at_ms);
        slot.set(Some(new_entry(text.clone(), expires)));
        Ok(Value::Bulk(text))
    })
}

fn append(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    admit(node, cmd[2].len())?;
    node.write(&cmd[1], node.stamp()?, |slot| {
        let (mut bytes, expires) = match slot.as_ref() {
            None => (Vec::new(), None),
            Some(Entry {
                value: Stored::Str(b),
                expires_at_ms,
                ..
            }) => (b.to_vec(), *expires_at_ms),
            Some(_) => return Err(CommandError::WrongType),
        };
        bytes.extend_from_slice(&cmd[2]);
        check_value(node, &bytes)?;
        let len = bytes.len() as i64;
        slot.set(Some(new_entry(Bytes::from(bytes), expires)));
        Ok(Value::Integer(len))
    })
}

fn strlen(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    node.store.read(&cmd[1], |e| match e {
        None => Ok(Value::Integer(0)),
        Some(Entry {
            value: Stored::Str(b),
            ..
        }) => Ok(Value::Integer(b.len() as i64)),
        Some(_) => Err(CommandError::WrongType),
    })
}

fn del(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    // A stamped write, not `Store::remove`, so the delete leaves a tombstone.
    let stamp = node.stamp()?;
    let removed = cmd[1..]
        .iter()
        .filter(|k| node.write(k, stamp, |slot| slot.take().is_some()))
        .count();
    Ok(Value::Integer(removed as i64))
}

fn exists(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    let found = cmd[1..]
        .iter()
        .filter(|k| node.store.read(k, |e| e.is_some()))
        .count();
    Ok(Value::Integer(found as i64))
}

fn type_(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    Ok(node.store.read(&cmd[1], |e| match e {
        None => Value::simple("none"),
        Some(e) => Value::simple(e.value.kind()),
    }))
}
