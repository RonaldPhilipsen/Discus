//! Commands that only nodes of the cluster send to each other. They run the two halves of a
//! `RENAME` or a `COPY` whose keys have different primaries, and the counter part of a flush on
//! the leader of a counter group.
//!
//! These specs stay out of the command table, so `prepare` never finds them for a client.

use discus_resp::{Command, Value};
use discus_store::entry::{estimate_size, ENTRY_OVERHEAD};
use discus_store::{DbKey, Entry};
use discus_wire::{decode_frame, encode_frame, Batch, Frame, Limits, Message, TraceContext};

use super::error::CommandError;
use super::table::CommandSpec;
use super::util::{admit, upper};
use crate::cluster::route::Route;
use crate::node::{ConnState, Node};

pub const TAKE: &CommandSpec = &CommandSpec::write("DISCUS.TAKE", 2, take);
pub const PUT: &CommandSpec = &CommandSpec::write("DISCUS.PUT", -3, put);
/// `DISCUS.PEEK` reads the primary: a lagging replica would give `COPY` an old value, and the put
/// would store it with a fresh version. It changes nothing, so it records and replicates nothing.
pub const PEEK: &CommandSpec =
    &CommandSpec::read("DISCUS.PEEK", 2, peek).with_route(Route::Single { write: true });

/// `DISCUS.COUNTERFLUSH <group> <flush>`: run the flush `FLUSHALL` or `DISCUS.FLUSHDB` in the
/// counter group `<group>`, in the database of the forward. A flush names no key, so the leader
/// cannot find the group from the flush alone. The counter router takes it before any handler,
/// so the handler runs only on a node without counter groups.
pub const COUNTER_FLUSH: &CommandSpec =
    &CommandSpec::write("DISCUS.COUNTERFLUSH", 3, no_counters).with_route(Route::Local);

const COMMANDS: &[&CommandSpec] = &[TAKE, PUT, PEEK, COUNTER_FLUSH];

/// Look up an internal command by name, ignoring case. Only the forward path calls this.
pub fn lookup_internal(name: &[u8]) -> Option<&'static CommandSpec> {
    COMMANDS
        .iter()
        .find(|spec| name.eq_ignore_ascii_case(spec.name.as_bytes()))
        .copied()
}

type Reply = Result<Value, CommandError>;

fn no_counters(_: &Node, _: &mut ConnState, _: &Command) -> Reply {
    Err(CommandError::Custom(
        "ERR this node runs no counter groups".into(),
    ))
}

/// One entry in the replication codec, which handles every value type.
fn encode(node: &Node, key: DbKey, entry: Entry) -> Vec<u8> {
    let mut batch = Batch::new(node.epoch_id());
    batch.entries.push((key, entry));
    encode_frame(&Frame {
        request_id: 0,
        trace: TraceContext::default(),
        message: Message::Entries(batch),
    })
}

fn decode(node: &Node, blob: &[u8]) -> Result<Entry, CommandError> {
    let limits = Limits {
        max_frame: node.config.cluster_max_frame_bytes,
    };
    let malformed = || CommandError::Custom("ERR malformed entry in DISCUS.PUT".into());
    let frame = decode_frame(blob, &limits).map_err(|_| malformed())?;
    let Message::Entries(batch) = frame.message else {
        return Err(malformed());
    };
    let (_, entry) = batch.entries.into_iter().next().ok_or_else(malformed)?;
    Ok(entry)
}

/// Remove the entry at `cmd[1]` and return it encoded. An entry too large for one forward stays.
/// The encoding runs outside the shard lock, so a large collection does not stall the shard. The
/// removal then checks the version, so a write that lands in between is not lost.
fn take(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let key = &cmd[1];
    let Some(entry) = node.store.read(conn.db, key, |e| e.cloned()) else {
        return Ok(Value::Null);
    };
    let seen = entry.version;
    let blob = encode(node, DbKey::new(conn.db, key.clone()), entry);
    if blob.len() > node.config.max_value_bytes {
        return Err(CommandError::ValueTooLarge);
    }
    node.write(conn.db, key, node.stamp()?, |slot| match slot.as_ref() {
        None => Ok(Value::Null),
        Some(e) if e.version != seen => Err(CommandError::ClusterTryAgain),
        Some(_) => {
            slot.set(None);
            Ok(Value::bulk(blob))
        }
    })
}

/// The entry at `cmd[1]`, encoded, or nil. An entry too large for one forward is refused.
fn peek(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let key = &cmd[1];
    let Some(entry) = node.store.read(conn.db, key, |e| e.cloned()) else {
        return Ok(Value::Null);
    };
    let blob = encode(node, DbKey::new(conn.db, key.clone()), entry);
    if blob.len() > node.config.max_value_bytes {
        return Err(CommandError::ValueTooLarge);
    }
    Ok(Value::bulk(blob))
}

/// Write the entry that `DISCUS.TAKE` or `DISCUS.PEEK` returned, with its deadline. The
/// destination key takes a fresh version here. With `NX`, a live entry stays and the reply is nil.
fn put(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let nx = match &cmd[3..] {
        [] => false,
        [flag] if upper(flag) == "NX" => true,
        _ => return Err(CommandError::Syntax),
    };
    let entry = decode(node, &cmd[2])?;
    admit(
        node,
        estimate_size(&cmd[1], &entry).saturating_sub(ENTRY_OVERHEAD) as usize,
    )?;
    Ok(node.write(conn.db, &cmd[1], node.stamp()?, |slot| {
        if nx && slot.is_some() {
            return Value::Null;
        }
        slot.set(Some(Entry::new(entry.value, entry.expires_at_ms)));
        Value::ok()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::table::Access;

    #[test]
    fn peek_reads_the_primary_with_the_read_scope() {
        assert_eq!(PEEK.route, Route::Single { write: true });
        assert_eq!(PEEK.access, Access::Read);
    }
}
