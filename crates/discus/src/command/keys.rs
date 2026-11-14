//! Expiry commands.

use discus_resp::{Command, Value};

use super::error::CommandError;
use super::table::{Access, CommandSpec};
use super::util::{admit, check_key, parse_i64, upper};
use crate::cluster::route::Route;
use crate::databases;
use crate::node::{ConnState, Node};
use discus_placement::Version;
use discus_store::entry::{estimate_size, ENTRY_OVERHEAD};
use discus_store::Entry;

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec::write("EXPIRE", 3, expire),
    CommandSpec::write("PEXPIRE", 3, pexpire),
    CommandSpec::write("EXPIREAT", 3, expireat),
    CommandSpec::write("PEXPIREAT", 3, pexpireat),
    CommandSpec::read("TTL", 2, ttl),
    CommandSpec::read("PTTL", 2, pttl),
    CommandSpec::write("PERSIST", 2, persist),
    CommandSpec::write("RENAME", 3, rename).with_route(Route::Rename),
    CommandSpec::write("MOVE", 3, move_key),
    CommandSpec::write("COPY", -3, copy).with_route(Route::Copy),
    // The flags of Valkey for `COMMAND`. The connection route still answers at the entry node, so
    // it never routes as a write.
    CommandSpec::connection("SWAPDB", 3, swapdb)
        .with_access(Access::Write)
        .with_fast(),
    CommandSpec::read("SCAN", -2, scan).with_route(Route::Local),
    CommandSpec::read("RANDOMKEY", 1, randomkey).with_route(Route::Local),
    CommandSpec::read("DBSIZE", 1, dbsize).with_route(Route::Fanout),
    CommandSpec::write("FLUSHDB", -1, flush).with_route(Route::Broadcast),
    CommandSpec::write("FLUSHALL", -1, flush).with_route(Route::Broadcast),
];

type Reply = Result<Value, CommandError>;

fn invalid(who: &str) -> CommandError {
    CommandError::Custom(format!("ERR invalid expire time in '{who}' command"))
}

/// Set an absolute deadline. A deadline that is not in the future deletes the key.
fn set_deadline(node: &Node, db: u32, key: &bytes::Bytes, deadline_ms: i128) -> Reply {
    let now = node.store.now_ms() as i128;
    node.write(db, key, node.stamp()?, |slot| {
        if slot.is_none() {
            return Ok(Value::Integer(0));
        }
        if deadline_ms <= now {
            slot.set(None);
        } else if let Some(e) = slot.as_mut() {
            e.expires_at_ms = Some(deadline_ms as u64);
        }
        Ok(Value::Integer(1))
    })
}

/// `n` units of `unit_ms` milliseconds, as a signed value that cannot overflow.
pub(crate) fn span(arg: &[u8], unit_ms: i128, who: &str) -> Result<i128, CommandError> {
    let n = parse_i64(arg)? as i128;
    let ms = n * unit_ms;
    if ms.abs() > (i64::MAX / 2) as i128 {
        return Err(invalid(who));
    }
    Ok(ms)
}

fn expire(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let ms = span(&cmd[2], 1000, "expire")?;
    set_deadline(node, conn.db, &cmd[1], node.store.now_ms() as i128 + ms)
}

fn pexpire(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let ms = span(&cmd[2], 1, "pexpire")?;
    set_deadline(node, conn.db, &cmd[1], node.store.now_ms() as i128 + ms)
}

fn expireat(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    set_deadline(node, conn.db, &cmd[1], span(&cmd[2], 1000, "expireat")?)
}

fn pexpireat(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    set_deadline(node, conn.db, &cmd[1], span(&cmd[2], 1, "pexpireat")?)
}

/// Remaining time of a key with `expires` (`None` for a missing key): -2 when missing, -1 when
/// it never expires.
pub(crate) fn ttl_value(
    expires: Option<Option<u64>>,
    now: u64,
    to_unit: impl Fn(u64) -> i64,
) -> Value {
    Value::Integer(match expires {
        None => -2,
        Some(None) => -1,
        Some(Some(t)) => to_unit(t.saturating_sub(now)),
    })
}

fn remaining(node: &Node, db: u32, key: &[u8], to_unit: impl Fn(u64) -> i64) -> Reply {
    let now = node.store.now_ms();
    let expires = node.store.read(db, key, |e| e.map(|e| e.expires_at_ms));
    Ok(ttl_value(expires, now, to_unit))
}

fn ttl(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    remaining(node, conn.db, &cmd[1], |ms| ((ms + 500) / 1000) as i64)
}

fn pttl(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    remaining(node, conn.db, &cmd[1], |ms| ms as i64)
}

fn persist(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    node.write(conn.db, &cmd[1], node.stamp()?, |slot| {
        if !slot.as_ref().is_some_and(|e| e.expires_at_ms.is_some()) {
            return Ok(Value::Integer(0));
        }
        if let Some(e) = slot.as_mut() {
            e.expires_at_ms = None;
        }
        Ok(Value::Integer(1))
    })
}

/// Move the entry of `cmd[1]` to `cmd[2]`, with its deadline. The take and the put are two
/// writes, so the move is not atomic.
fn rename(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let (from, to) = (&cmd[1], &cmd[2]);
    check_key(node, to)?;
    let size = node
        .store
        .read(conn.db, from, |e| e.map(|e| estimate_size(to, e)));
    let Some(size) = size else {
        return Err(CommandError::NoSuchKey);
    };
    if from == to {
        return Ok(Value::ok());
    }
    admit(node, size.saturating_sub(ENTRY_OVERHEAD) as usize)?;
    let stamp = node.stamp()?;
    let Some(taken) = node.write(conn.db, from, stamp, |slot| slot.take()) else {
        return Err(CommandError::NoSuchKey);
    };
    node.write(conn.db, to, stamp, |slot| {
        slot.set(Some(Entry::new(taken.value, taken.expires_at_ms)));
    });
    Ok(Value::ok())
}

fn same_objects() -> CommandError {
    CommandError::Custom("ERR source and destination objects are the same".into())
}

/// Database `n` as a target. It must be below the cluster count, and this node must hold it.
fn target_db(node: &Node, n: i64) -> Result<u32, CommandError> {
    let count = node.databases.cluster().min(node.databases.local());
    if n < 0 || n >= i64::from(count) {
        return Err(CommandError::Custom(databases::OUT_OF_RANGE.into()));
    }
    Ok(n as u32)
}

/// Write `entry` at `key` in `db` with the version `stamp`. Without `replace`, a live entry
/// stays. True if it wrote.
fn place(
    node: &Node,
    db: u32,
    key: &bytes::Bytes,
    entry: Entry,
    replace: bool,
    stamp: Version,
) -> Result<bool, CommandError> {
    admit(
        node,
        estimate_size(key, &entry).saturating_sub(ENTRY_OVERHEAD) as usize,
    )?;
    Ok(node.write(db, key, stamp, |slot| {
        if !replace && slot.is_some() {
            return false;
        }
        slot.set(Some(Entry::new(entry.value, entry.expires_at_ms)));
        true
    }))
}

/// `MOVE key db`. Both databases hold the key in the same slot, so the primary does both writes.
/// The destination goes first, so a failure between the writes keeps both copies and never
/// loses the value.
fn move_key(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let key = &cmd[1];
    let n = parse_i64(&cmd[2])?;
    if n == i64::from(conn.db) {
        return Err(same_objects());
    }
    let db = target_db(node, n)?;
    node.databases.check_gate()?;
    let Some(entry) = node.store.read(conn.db, key, |e| e.cloned()) else {
        return Ok(Value::Integer(0));
    };
    let seen = entry.version;
    // Both writes take one stamp, so the stamp that the reply reports covers the destination too,
    // and the write set of the connection can record it.
    let stamp = node.stamp()?;
    if !place(node, db, key, entry, false, stamp)? {
        return Ok(Value::Integer(0));
    }
    // A write that landed on the source after the read keeps it.
    node.write(conn.db, key, stamp, |slot| {
        if slot.as_ref().is_some_and(|e| e.version == seen) {
            slot.set(None);
        }
    });
    Ok(Value::Integer(1))
}

/// The options of `COPY source destination [DB db] [REPLACE]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CopyTarget {
    pub db: u32,
    pub replace: bool,
}

/// Parse and check `COPY` from database `db`, in the order of Valkey. The gate applies even
/// without `DB`, because a member without it cannot run the steps of a `COPY`.
pub(crate) fn copy_target(node: &Node, db: u32, cmd: &Command) -> Result<CopyTarget, CommandError> {
    let mut target = CopyTarget { db, replace: false };
    let mut i = 3;
    while i < cmd.len() {
        match upper(&cmd[i]).as_str() {
            "REPLACE" => target.replace = true,
            "DB" if i + 1 < cmd.len() => {
                target.db = target_db(node, parse_i64(&cmd[i + 1])?)?;
                i += 1;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    node.databases.check_gate()?;
    if target.db == db && cmd[1] == cmd[2] {
        return Err(same_objects());
    }
    check_key(node, &cmd[2])?;
    Ok(target)
}

/// `COPY` on a node without the cluster link. With the link, `cluster::copy` runs it.
fn copy(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let target = copy_target(node, conn.db, cmd)?;
    let Some(entry) = node.store.read(conn.db, &cmd[1], |e| e.cloned()) else {
        return Ok(Value::Integer(0));
    };
    let copied = place(
        node,
        target.db,
        &cmd[2],
        entry,
        target.replace,
        node.stamp()?,
    )?;
    Ok(Value::Integer(i64::from(copied)))
}

fn swapdb(_: &Node, _: &mut ConnState, _: &Command) -> Reply {
    Err(CommandError::Custom(
        "ERR SWAPDB is not allowed in cluster mode".into(),
    ))
}

fn scan(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    scan_where(node, conn.db, cmd, |_| true)
}

/// `SCAN` for a redirect connection: only the keys that this node is primary for, so a client
/// that scans every node sees each key once.
pub fn scan_primary(node: &Node, db: u32, cmd: &Command) -> Value {
    let epoch = node.epochs().current().clone();
    scan_where(node, db, cmd, |key| epoch.primary(key) == Some(node.id))
        .unwrap_or_else(|e| e.to_value())
}

fn scan_where(node: &Node, db: u32, cmd: &Command, keep: impl Fn(&[u8]) -> bool) -> Reply {
    let cursor = std::str::from_utf8(&cmd[1])
        .ok()
        .and_then(|c| c.parse::<u64>().ok())
        .ok_or_else(|| CommandError::Custom("ERR invalid cursor".into()))?;
    let (mut pattern, mut count) = (None, 10usize);
    let mut i = 2;
    while i < cmd.len() {
        match upper(&cmd[i]).as_str() {
            "MATCH" if i + 1 < cmd.len() => pattern = Some(cmd[i + 1].clone()),
            "COUNT" if i + 1 < cmd.len() => {
                let n = parse_i64(&cmd[i + 1])?;
                if n < 1 {
                    return Err(CommandError::Syntax);
                }
                count = n as usize;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 2;
    }
    let (next, keys) = node.store.scan(db, cursor, count, pattern.as_deref());
    Ok(Value::Array(vec![
        Value::bulk(next.to_string().into_bytes()),
        Value::Array(
            keys.into_iter()
                .filter(|k| keep(k))
                .map(Value::Bulk)
                .collect(),
        ),
    ]))
}

fn randomkey(node: &Node, conn: &mut ConnState, _: &Command) -> Reply {
    Ok(node
        .store
        .random_key(conn.db)
        .map_or(Value::Null, Value::Bulk))
}

fn dbsize(node: &Node, conn: &mut ConnState, _: &Command) -> Reply {
    Ok(Value::Integer(node.store.db_len(conn.db) as i64))
}

fn flush(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    if cmd.len() > 2
        || cmd
            .get(1)
            .is_some_and(|m| !matches!(upper(m).as_str(), "ASYNC" | "SYNC"))
    {
        return Err(CommandError::Syntax);
    }
    // In-flight batches can still land after this point, so a flush is best-effort.
    if cmd[0].eq_ignore_ascii_case(b"FLUSHDB") {
        if let Some(r) = node.replicator() {
            r.flush_db(conn.db);
        }
        node.store.clear_db(conn.db);
    } else {
        if let Some(r) = node.replicator() {
            r.flush();
            r.reset_handoffs();
        }
        node.store.clear();
    }
    Ok(Value::ok())
}
