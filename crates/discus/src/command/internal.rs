//! Commands that only nodes of the cluster send to each other. They run the two halves of a
//! `RENAME` whose keys have different primaries.
//!
//! These specs stay out of the command table, so `prepare` never finds them for a client.

use discus_resp::{Command, Value};
use discus_store::entry::{estimate_size, ENTRY_OVERHEAD};
use discus_store::{DbKey, Entry};
use discus_wire::{decode_frame, encode_frame, Batch, Frame, Limits, Message, TraceContext};

use super::error::CommandError;
use super::table::CommandSpec;
use super::util::admit;
use crate::node::{ConnState, Node};

pub const TAKE: &CommandSpec = &CommandSpec::write("DISCUS.TAKE", 2, take);
pub const PUT: &CommandSpec = &CommandSpec::write("DISCUS.PUT", 3, put);

const COMMANDS: &[&CommandSpec] = &[TAKE, PUT];

/// Look up an internal command by name, ignoring case. Only the forward path calls this.
pub fn lookup_internal(name: &[u8]) -> Option<&'static CommandSpec> {
    COMMANDS
        .iter()
        .find(|spec| name.eq_ignore_ascii_case(spec.name.as_bytes()))
        .copied()
}

type Reply = Result<Value, CommandError>;

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

/// Write the entry that `DISCUS.TAKE` returned, with its deadline. The destination key takes a
/// fresh version here.
fn put(node: &Node, conn: &mut ConnState, cmd: &Command) -> Reply {
    let entry = decode(node, &cmd[2])?;
    admit(
        node,
        estimate_size(&cmd[1], &entry).saturating_sub(ENTRY_OVERHEAD) as usize,
    )?;
    node.write(conn.db, &cmd[1], node.stamp()?, |slot| {
        slot.set(Some(Entry::new(entry.value, entry.expires_at_ms)));
    });
    Ok(Value::ok())
}
