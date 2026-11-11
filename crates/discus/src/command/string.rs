//! String commands.

use bytes::Bytes;
use discus_resp::{Command, Value};
use discus_store::{Entry, Value as Stored};

use super::error::CommandError;
use super::float::{incr_by_float, parse_float_delta};
use super::table::CommandSpec;
use super::util::{admit, check_key, check_mset, check_value, parse_i64, upper};
use crate::cluster::route::Route;
use crate::node::{ConnState, Node};

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec::read("GET", 2, get),
    CommandSpec::write("SET", -3, set),
    CommandSpec::write("GETDEL", 2, getdel),
    CommandSpec::write("GETEX", -2, getex),
    CommandSpec::read("MGET", -2, mget).with_route(Route::MultiRead { sum: false }),
    CommandSpec::write("MSET", -3, mset).with_route(Route::MultiWrite { pairs: true }),
    CommandSpec::write("INCR", -2, incr),
    CommandSpec::write("DECR", -2, decr),
    CommandSpec::write("INCRBY", -3, incrby),
    CommandSpec::write("DECRBY", -3, decrby),
    CommandSpec::write("INCRBYFLOAT", 3, incrbyfloat),
    CommandSpec::write("APPEND", 3, append),
    CommandSpec::read("STRLEN", 2, strlen),
    CommandSpec::write("DEL", -2, del).with_route(Route::MultiWrite { pairs: false }),
    CommandSpec::read("EXISTS", -2, exists).with_route(Route::MultiRead { sum: true }),
    CommandSpec::read("TYPE", 2, type_),
];

type Reply = Result<Value, CommandError>;

fn new_entry(value: Bytes, expires_at_ms: Option<u64>) -> Entry {
    Entry::new(Stored::Str(value), expires_at_ms)
}

/// The string of an entry: `None` for a missing key, `WrongType` for another type.
fn str_of(e: Option<&Entry>) -> Result<Option<&Bytes>, CommandError> {
    match e {
        None => Ok(None),
        Some(Entry {
            value: Stored::Str(b),
            ..
        }) => Ok(Some(b)),
        Some(_) => Err(CommandError::WrongType),
    }
}

fn bulk_or_null(b: Option<&Bytes>) -> Value {
    b.map_or(Value::Null, |b| Value::Bulk(b.clone()))
}

fn get(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    node.store.read(&cmd[1], |e| Ok(bulk_or_null(str_of(e)?)))
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

/// The options of `SET` after the value.
#[derive(Clone, Copy, Default)]
pub struct SetOptions {
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
    pub expiry: Option<Expiry>,
}

pub fn set_options(cmd: &Command, now_ms: u64) -> Result<SetOptions, CommandError> {
    let mut o = SetOptions::default();
    let mut i = 3;
    while i < cmd.len() {
        let opt = upper(&cmd[i]);
        match opt.as_str() {
            "NX" => o.nx = true,
            "XX" => o.xx = true,
            "GET" => o.get = true,
            "KEEPTTL" if o.expiry.is_none() => o.expiry = Some(Expiry::Keep),
            "EX" | "PX" | "EXAT" | "PXAT" if o.expiry.is_none() => {
                let arg = cmd.get(i + 1).ok_or(CommandError::Syntax)?;
                o.expiry = Some(Expiry::At(deadline(&opt, arg, now_ms, "set")?));
                i += 1;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    if o.nx && o.xx {
        return Err(CommandError::Syntax);
    }
    Ok(o)
}

fn set(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    check_value(node, &cmd[2])?;
    let o = set_options(cmd, node.store.now_ms())?;
    admit(node, cmd[1].len() + cmd[2].len())?;
    let value = cmd[2].clone();
    node.write(&cmd[1], node.stamp()?, |slot| {
        let old = match str_of(slot.as_ref()) {
            Err(e) if o.get => return Err(e),
            old => old.ok().flatten().cloned(),
        };
        let exists = slot.is_some();
        let skipped = (o.nx && exists) || (o.xx && !exists);
        let reply = |old: Option<Bytes>| match (o.get, old) {
            (true, Some(b)) => Value::Bulk(b),
            (true, None) => Value::Null,
            (false, _) => Value::ok(),
        };
        if skipped {
            return Ok(if o.get { reply(old) } else { Value::Null });
        }
        let expires = match o.expiry {
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
        let Some(b) = str_of(slot.as_ref())?.cloned() else {
            return Ok(Value::Null);
        };
        slot.take();
        Ok(Value::Bulk(b))
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
        let Some(b) = str_of(slot.as_ref())? else {
            return Ok(Value::Null);
        };
        let reply = Value::Bulk(b.clone());
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
            node.store
                .read(key, |e| bulk_or_null(str_of(e).ok().flatten()))
        })
        .collect();
    Ok(Value::Array(values))
}

fn mset(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_mset(node, cmd)?;
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
        let current = str_of(slot.as_ref())?.map_or(Ok(0), |b| parse_i64(b))?;
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
    let delta = parse_float_delta(&cmd[2])?;
    node.write(&cmd[1], node.stamp()?, |slot| {
        let text = incr_by_float(str_of(slot.as_ref())?.map(|b| &b[..]), delta)?;
        let expires = slot.as_ref().and_then(|e| e.expires_at_ms);
        slot.set(Some(new_entry(text.clone(), expires)));
        Ok(Value::Bulk(text))
    })
}

fn append(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    check_key(node, &cmd[1])?;
    admit(node, cmd[2].len())?;
    node.write(&cmd[1], node.stamp()?, |slot| {
        let mut bytes = str_of(slot.as_ref())?.map_or_else(Vec::new, |b| b.to_vec());
        let expires = slot.as_ref().and_then(|e| e.expires_at_ms);
        bytes.extend_from_slice(&cmd[2]);
        check_value(node, &bytes)?;
        let len = bytes.len() as i64;
        slot.set(Some(new_entry(Bytes::from(bytes), expires)));
        Ok(Value::Integer(len))
    })
}

fn strlen(node: &Node, _: &mut ConnState, cmd: &Command) -> Reply {
    node.store.read(&cmd[1], |e| {
        Ok(Value::Integer(str_of(e)?.map_or(0, |b| b.len() as i64)))
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
