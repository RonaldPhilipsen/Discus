//! The in-memory Raft log and state machine (spec section 10.2). A restarted pod has a new
//! `NodeId` and new replica IDs, so nothing here needs to survive the process.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::sync::Arc;

use discus_resp::Value;
use futures::{Stream, TryStreamExt};
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend, RaftLogReader, RaftSnapshotBuilder, StoredMembership};
use openraft_legacy::network_v1::SnapshotReceiverFactory;
use parking_lot::Mutex;

use super::{Entry, LogId, TypeConfig, Vote};

type SnapshotData = Cursor<Vec<u8>>;
type Snapshot = SnapshotOf<TypeConfig, SnapshotData>;
type SnapshotMeta = SnapshotMetaOf<TypeConfig>;
use crate::counter::budget::{Budget, Tracked};
use crate::counter::state::{CounterState, Limits};

/// The internal command that replaces the state with a snapshot. A new instance writes it first,
/// so the state it starts from reaches every learner through the log or a snapshot.
pub const RESTORE: &[u8] = b"RESTORE";

#[derive(Debug, Default)]
struct Log {
    vote: Option<Vote>,
    committed: Option<LogId>,
    entries: BTreeMap<u64, Entry>,
    purged: Option<LogId>,
}

#[derive(Debug, Clone, Default)]
pub struct LogStore(Arc<Mutex<Log>>);

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let log = self.0.lock();
        Ok(log.entries.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.0.lock().vote)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
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

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.0.lock().vote = Some(*vote);
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        self.0.lock().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.0.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut log = self.0.lock();
            for e in entries {
                log.entries.insert(e.log_id.index, e);
            }
        }
        // The log is memory, so an append is durable once it is in the map.
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let start = last_log_id.map_or(0, |l| l.index + 1);
        self.0.lock().entries.split_off(&start);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
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
    pub last_applied: Option<LogId>,
    pub membership: StoredMembershipOf<TypeConfig>,
    limits: Limits,
    current: Option<(SnapshotMeta, Vec<u8>)>,
    /// Snapshots that this replica installed from a leader.
    pub installed: u64,
    /// The final state was read (see [`super::Replica::stop`]). No later entry applies or gets a
    /// reply.
    pub sealed: bool,
}

impl Sm {
    pub fn new(budget: Arc<Budget>, limits: Limits) -> Sm {
        Sm {
            state: Tracked::new(CounterState::new(), budget),
            last_applied: None,
            membership: StoredMembership::default(),
            limits,
            current: None,
            installed: 0,
            sealed: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SmStore(pub Arc<Mutex<Sm>>);

impl RaftSnapshotBuilder<TypeConfig> for SmStore {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
        let mut sm = self.0.lock();
        let data = sm.state.get().encode();
        let meta = SnapshotMeta {
            last_log_id: sm.last_applied,
            last_membership: sm.membership.clone(),
        };
        sm.current = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftStateMachine<TypeConfig> for SmStore {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = SmStore;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembershipOf<TypeConfig>), io::Error> {
        let sm = self.0.lock();
        Ok((sm.last_applied, sm.membership.clone()))
    }

    /// The whole batch applies under one lock, as in openraft 0.9, so a reader never sees a part
    /// of it. Each reply leaves after its entry is applied.
    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let entries: Vec<EntryResponder<TypeConfig>> = entries.try_collect().await?;
        let mut sm = self.0.lock();
        if sm.sealed {
            // The dropped responders fail the writes, so their clients see them in doubt.
            return Ok(());
        }
        for (e, responder) in entries {
            sm.last_applied = Some(e.log_id);
            let reply = match e.payload {
                EntryPayload::Blank => Value::Null,
                EntryPayload::Normal(op) if op.command[0][..] == *RESTORE => {
                    if let Some(state) = op.command.get(1).and_then(|b| CounterState::decode(b)) {
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
            if let Some(responder) = responder {
                responder.send(reply);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let state = CounterState::decode(&data).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "a counter snapshot is invalid")
        })?;
        let mut sm = self.0.lock();
        sm.state.replace(state);
        sm.last_applied = meta.last_log_id;
        sm.membership = meta.last_membership.clone();
        sm.current = Some((meta.clone(), data));
        sm.installed += 1;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        let sm = self.0.lock();
        Ok(sm.current.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(data.clone()),
        }))
    }
}

/// A leader sends a snapshot in chunks, which the follower collects in memory.
impl SnapshotReceiverFactory<TypeConfig> for SmStore {
    type SnapshotReceiver = SnapshotData;

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotData, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counter::cmd;
    use crate::counter::raft::Op;
    use openraft::impls::leader_id_adv::LeaderId;

    fn incr(index: u64) -> Result<EntryResponder<TypeConfig>, io::Error> {
        let log_id = LogId::new(
            LeaderId {
                term: 1,
                node_id: 1,
            },
            index,
        );
        let payload = EntryPayload::Normal(Op {
            stamp_ms: 10,
            command: cmd(&["INCR", "cnt:x"]),
        });
        Ok((Entry { log_id, payload }, None))
    }

    /// The worker can reach the lock after the final state was read. Its entries must not apply.
    #[tokio::test]
    async fn a_sealed_state_machine_applies_nothing() {
        let limits = Limits {
            max_value_bytes: 1 << 20,
        };
        let budget = Arc::new(Budget::new(u64::MAX, None));
        let mut store = SmStore(Arc::new(Mutex::new(Sm::new(budget, limits))));
        store.apply(futures::stream::iter([incr(1)])).await.unwrap();
        store.0.lock().sealed = true;
        store.apply(futures::stream::iter([incr(2)])).await.unwrap();
        let sm = store.0.lock();
        assert_eq!(sm.state.get().get(b"cnt:x").unwrap().value, "1");
        assert_eq!(sm.last_applied.map(|l| l.index), Some(1));
    }
}
