//! The in-memory Raft log and state machine (spec section 10.2). A restarted pod has a new
//! `NodeId` and new replica IDs, so nothing here needs to survive the process.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use discus_resp::Value;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use parking_lot::Mutex;

use super::{Peer, TypeConfig};
use crate::counter::budget::{Budget, Tracked};
use crate::counter::state::{CounterEntry, CounterState, Limits};

/// The internal command that replaces the state with a snapshot. A new instance writes it first,
/// so the state it starts from reaches every learner through the log or a snapshot.
pub const RESTORE: &[u8] = b"RESTORE";

#[derive(Debug, Default)]
struct Log {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    entries: BTreeMap<u64, Entry<TypeConfig>>,
    purged: Option<LogId<u64>>,
}

#[derive(Debug, Clone, Default)]
pub struct LogStore(Arc<Mutex<Log>>);

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let log = self.0.lock();
        Ok(log.entries.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let log = self.0.lock();
        let last = log
            .entries
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(log.purged);
        Ok(LogState {
            last_purged_log_id: log.purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.0.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.0.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.0.lock().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.0.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut log = self.0.lock();
            for e in entries {
                log.entries.insert(e.log_id.index, e);
            }
        }
        // The log is memory, so an append is durable once it is in the map.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.0.lock().entries.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut log = self.0.lock();
        let keep = log.entries.split_off(&(log_id.index + 1));
        log.entries = keep;
        log.purged = Some(log_id);
        Ok(())
    }
}

/// The state machine of one replica.
#[derive(Debug)]
pub struct Sm {
    pub state: Tracked,
    pub last_applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, Peer>,
    limits: Limits,
    current: Option<(SnapshotMeta<u64, Peer>, Vec<u8>)>,
    snapshots: u64,
    /// Snapshots that this replica installed from a leader.
    pub installed: u64,
}

impl Sm {
    pub fn new(budget: Arc<Budget>, limits: Limits) -> Sm {
        Sm {
            state: Tracked::new(CounterState::new(), budget),
            last_applied: None,
            membership: StoredMembership::default(),
            limits,
            current: None,
            snapshots: 0,
            installed: 0,
        }
    }
}

pub fn encode_state(state: &CounterState) -> Vec<u8> {
    let entries: Vec<(Bytes, Bytes, u64)> = state
        .entries()
        .map(|(k, e)| (k.clone(), e.value.clone(), e.expires_at_ms.unwrap_or(0)))
        .collect();
    discus_wire::encode_counter_snapshot(state.clock_ms(), &entries)
}

pub fn decode_state(bytes: &[u8]) -> Option<CounterState> {
    let (clock, entries) = discus_wire::decode_counter_snapshot(bytes).ok()?;
    Some(CounterState::from_parts(
        clock,
        entries
            .into_iter()
            .map(|(k, v, t)| {
                (
                    k,
                    CounterEntry {
                        value: v,
                        expires_at_ms: (t != 0).then_some(t),
                    },
                )
            })
            .collect(),
    ))
}

#[derive(Debug, Clone)]
pub struct SmStore(pub Arc<Mutex<Sm>>);

fn io_error(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageError::IO {
        source: openraft::StorageIOError::read_snapshot(
            None,
            openraft::AnyError::error(e.to_string()),
        ),
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SmStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut sm = self.0.lock();
        let data = encode_state(sm.state.get());
        sm.snapshots += 1;
        let meta = SnapshotMeta {
            last_log_id: sm.last_applied,
            last_membership: sm.membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                sm.last_applied.map_or(0, |l| l.index),
                sm.snapshots
            ),
        };
        sm.current = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for SmStore {
    type SnapshotBuilder = SmStore;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, Peer>), StorageError<u64>> {
        let sm = self.0.lock();
        Ok((sm.last_applied, sm.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Value>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut sm = self.0.lock();
        let mut replies = Vec::new();
        for e in entries {
            sm.last_applied = Some(e.log_id);
            let reply = match e.payload {
                EntryPayload::Blank => Value::Null,
                EntryPayload::Normal(op) if op.command[0][..] == *RESTORE => {
                    if let Some(state) = op.command.get(1).and_then(|b| decode_state(b)) {
                        sm.state.replace(state);
                    }
                    Value::ok()
                }
                EntryPayload::Normal(op) => {
                    let limits = sm.limits;
                    sm.state.apply(op.stamp_ms, &op.command, &limits)
                }
                EntryPayload::Membership(m) => {
                    sm.membership = StoredMembership::new(Some(e.log_id), m);
                    Value::Null
                }
            };
            replies.push(reply);
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, Peer>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let state = decode_state(&data).ok_or_else(|| io_error("a counter snapshot is invalid"))?;
        let mut sm = self.0.lock();
        sm.state.replace(state);
        sm.last_applied = meta.last_log_id;
        sm.membership = meta.last_membership.clone();
        sm.current = Some((meta.clone(), data));
        sm.installed += 1;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let sm = self.0.lock();
        Ok(sm.current.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}
