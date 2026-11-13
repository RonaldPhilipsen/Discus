//! Raft groups on `openraft` (spec section 10.2).
//!
//! A replica is one node's part in one Raft instance of one group. Its Raft node ID is a random
//! `u64` that lives as long as the replica. A node that discards its counter state, or joins an
//! instance again, gets a new ID, so it can never vote with a log it no longer holds.

pub mod convert;
pub mod network;
pub mod store;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::{Message, RaftReply, Regime};
use openraft::async_runtime::WatchReceiver;
use openraft::{Config, Membership, RaftMetrics, ReadPolicy, ServerState, SnapshotPolicy};
use openraft_legacy::network_v1::ChunkedSnapshotReceiver;
use parking_lot::Mutex;

use super::budget::Budget;
use super::state::{CounterState, Limits};
use network::{Contacts, NetFactory, RaftLink};
use store::{LogStore, Sm, SmStore, RESTORE};

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Op,
        R = Value,
        Node = Peer,
);

pub type Raft = openraft::Raft<TypeConfig, SmStore>;
pub type Entry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;
pub type Peers = Membership<u64, Peer>;

/// One log entry: the leader stamp and the command (spec section 10.4).
#[derive(Debug, Clone, PartialEq)]
pub struct Op {
    pub stamp_ms: u64,
    pub command: Command,
}

/// The command name only: a `RESTORE` entry carries the whole state.
impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.command.first().map(|n| String::from_utf8_lossy(n));
        write!(
            f,
            "{} ({} args) @{}",
            name.as_deref().unwrap_or(""),
            self.command.len().saturating_sub(1),
            self.stamp_ms
        )
    }
}

/// The node behind a replica ID.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Peer {
    pub node: NodeId,
}

#[derive(Debug, Clone, Copy)]
pub struct RaftTiming {
    pub heartbeat_ms: u64,
    pub election_ms: u64,
    pub snapshot_entries: u64,
}

impl RaftTiming {
    fn config(&self) -> Result<Config, String> {
        Config {
            cluster_name: "discus-counter".into(),
            heartbeat_interval: self.heartbeat_ms.max(1),
            election_timeout_min: self.election_ms.max(self.heartbeat_ms + 1),
            election_timeout_max: 2 * self.election_ms.max(self.heartbeat_ms + 1),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(self.snapshot_entries.max(1)),
            max_in_snapshot_log_to_keep: 100,
            snapshot_max_chunk_size: 1 << 20,
            ..Config::default()
        }
        .validate()
        .map_err(|e| e.to_string())
    }
}

pub struct Replica {
    pub raft: Raft,
    pub sm: Arc<Mutex<Sm>>,
    pub replica: u64,
    pub regime: Regime,
    pub contacts: Arc<Contacts>,
    pub started: Instant,
}

impl Replica {
    /// Start an empty replica. It waits for a leader, unless [`Replica::bootstrap`] makes it the
    /// only voter.
    pub async fn start(
        timing: &RaftTiming,
        limits: Limits,
        group: u16,
        regime: Regime,
        replica: u64,
        link: Arc<dyn RaftLink>,
        budget: Arc<Budget>,
    ) -> Result<Replica, String> {
        let contacts = Arc::new(Contacts::default());
        let net = NetFactory {
            link,
            group,
            regime,
            contacts: contacts.clone(),
        };
        let sm = Arc::new(Mutex::new(Sm::new(budget, limits)));
        let raft = Raft::new(
            replica,
            Arc::new(timing.config()?),
            net,
            LogStore::default(),
            SmStore(sm.clone()),
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(Replica {
            raft,
            sm,
            replica,
            regime,
            contacts,
            started: Instant::now(),
        })
    }

    /// Make this replica the only voter and start from `initial`. The state travels as the first
    /// command, so a learner receives it through the log or a snapshot.
    pub async fn bootstrap(
        &self,
        me: NodeId,
        initial: &CounterState,
        stamp_ms: u64,
        wait: Duration,
    ) -> Result<(), String> {
        let members = BTreeMap::from([(self.replica, Peer { node: me })]);
        self.raft
            .initialize(members)
            .await
            .map_err(|e| e.to_string())?;
        self.raft
            .wait(Some(wait))
            .state(ServerState::Leader, "bootstrap")
            .await
            .map_err(|e| e.to_string())?;
        let restore = vec![Bytes::from_static(RESTORE), Bytes::from(initial.encode())];
        tokio::time::timeout(
            wait,
            self.raft.client_write(Op {
                stamp_ms,
                command: restore,
            }),
        )
        .await
        .map_err(|_| "the restore entry did not commit in time".to_string())?
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Answer `RAFT_VOTE`, `RAFT_APPEND`, and `RAFT_SNAPSHOT`. The caller checks the group, the
    /// regime, and the target.
    pub async fn handle(&self, message: Message) -> Message {
        let reply = match message {
            Message::RaftAppend(a) => match convert::append_of(a) {
                Some(req) => {
                    let last = req.last_log_id();
                    match self.raft.append_entries(req).await {
                        Ok(r) => convert::append_reply_to(&r, last),
                        Err(_) => RaftReply::refused(),
                    }
                }
                None => RaftReply::refused(),
            },
            Message::RaftVote(v) => match self.raft.vote(convert::vote_req_of(v)).await {
                Ok(r) => convert::vote_reply_to(&r),
                Err(_) => RaftReply::refused(),
            },
            Message::RaftSnapshot(s) => match convert::snapshot_of(s) {
                Some(req) => match self.raft.install_snapshot(req).await {
                    Ok(r) => convert::snapshot_reply_to(&r),
                    Err(_) => RaftReply::refused(),
                },
                None => RaftReply::refused(),
            },
            _ => RaftReply::refused(),
        };
        Message::RaftReply(reply)
    }

    pub(super) fn with_metrics<R>(&self, f: impl FnOnce(&RaftMetrics<TypeConfig>) -> R) -> R {
        f(&self.raft.metrics().borrow_watched())
    }

    /// A linearizable read point: the leader confirms its leadership with a quorum and waits
    /// until it applied every entry committed before (spec section 10.2).
    pub async fn ensure_linearizable(&self) -> Result<(), String> {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn with_membership<R>(&self, f: impl FnOnce(&Peers) -> R) -> R {
        self.with_metrics(|m| f(m.membership_config.membership()))
    }

    pub fn is_leader(&self) -> bool {
        self.with_metrics(|m| {
            m.state == ServerState::Leader && m.current_leader == Some(self.replica)
        })
    }

    pub fn leader_node(&self) -> Option<NodeId> {
        self.with_metrics(|m| {
            m.membership_config
                .membership()
                .get_node(&m.current_leader?)
                .map(|p| p.node)
        })
    }

    /// The voters of the current membership, by replica ID and node. During a joint change it is
    /// the union of both sets.
    pub fn voters(&self) -> Vec<(u64, NodeId)> {
        self.with_membership(|m| listed(m, m.voter_ids()).collect())
    }

    pub fn learners(&self) -> Vec<(u64, NodeId)> {
        self.with_membership(|m| listed(m, m.non_write_voter_ids()).collect())
    }

    pub fn voter_nodes(&self) -> Vec<NodeId> {
        self.with_membership(|m| listed(m, m.voter_ids()).map(|(_, n)| n).collect())
    }

    pub fn learner_nodes(&self) -> Vec<NodeId> {
        self.with_membership(|m| listed(m, m.non_write_voter_ids()).map(|(_, n)| n).collect())
    }

    pub fn is_voter(&self) -> bool {
        self.with_membership(|m| m.voter_ids().any(|id| id == self.replica))
    }

    pub fn is_learner(&self) -> bool {
        self.with_membership(|m| m.non_write_voter_ids().any(|id| id == self.replica))
    }

    /// The newest term of a leader that this replica accepted: its committed vote, else the
    /// term of its last applied entry. The term of its own candidacy does not count.
    pub fn leader_term(&self) -> u64 {
        self.with_metrics(|m| {
            let voted = if m.vote.is_committed() {
                m.vote.leader_id.term
            } else {
                0
            };
            let logged = m.last_applied.as_ref().map_or(0, |l| l.leader_id.term);
            voted.max(logged)
        })
    }

    /// The voter configs of the current membership. A joint change has two.
    pub fn voter_configs(&self) -> Vec<BTreeSet<u64>> {
        self.with_membership(|m| m.get_joint_config().clone())
    }

    /// The newest membership entry has not been applied here yet.
    pub fn change_pending(&self) -> bool {
        self.with_metrics(|m| m.membership_config.log_id().as_ref() > m.last_applied.as_ref())
    }

    /// The state that this replica applied.
    pub fn state(&self) -> CounterState {
        self.sm.lock().state.get().clone()
    }

    pub fn holds_expired(&self, now_ms: u64) -> bool {
        self.sm.lock().state.get().expired_keys(now_ms)
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }
}

/// The replicas `ids` of membership `m` with their nodes.
fn listed<'a>(
    m: &'a Peers,
    ids: impl Iterator<Item = u64> + 'a,
) -> impl Iterator<Item = (u64, NodeId)> + 'a {
    ids.filter_map(|id| m.get_node(&id).map(|p| (id, p.node)))
}

/// A random replica ID. Zero means "no replica" on the wire, so it is never used.
pub fn new_replica_id() -> u64 {
    fastrand::u64(1..)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use futures::future::BoxFuture;

    #[derive(Default)]
    struct MemLink(Mutex<HashMap<NodeId, Arc<Replica>>>);

    impl RaftLink for MemLink {
        fn send(
            &self,
            to: NodeId,
            message: Message,
            _: Duration,
        ) -> BoxFuture<'static, Result<Message, String>> {
            let target = self.0.lock().get(&to).cloned();
            Box::pin(async move {
                match target {
                    Some(r) => Ok(r.handle(message).await),
                    None => Err("no such node".into()),
                }
            })
        }
    }

    const LIMITS: Limits = Limits {
        max_value_bytes: 1 << 20,
    };

    fn timing(snapshot_entries: u64) -> RaftTiming {
        RaftTiming {
            heartbeat_ms: 50,
            election_ms: 300,
            snapshot_entries,
        }
    }

    use crate::counter::cmd;

    fn regime() -> Regime {
        Regime {
            generation: 1,
            origin: NodeId::from_u128(1),
        }
    }

    async fn replica(
        link: &Arc<MemLink>,
        node: u128,
        snapshot_entries: u64,
        budget: &Arc<Budget>,
    ) -> Arc<Replica> {
        let r = Replica::start(
            &timing(snapshot_entries),
            LIMITS,
            0,
            regime(),
            new_replica_id(),
            link.clone(),
            budget.clone(),
        )
        .await
        .unwrap();
        let r = Arc::new(r);
        link.0.lock().insert(NodeId::from_u128(node), r.clone());
        r
    }

    async fn wait_applied(r: &Replica, key: &str, want: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let got = r.state().get(key.as_bytes()).map(|e| e.value.clone());
            if got.as_deref() == Some(want.as_bytes()) {
                return;
            }
            assert!(Instant::now() < deadline, "{key} is {got:?}, not {want}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A replica applies the membership change on its own schedule, after the data can arrive.
    async fn wait_learners(r: &Replica, want: usize) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let got = r.learners().len();
            if got == want {
                return;
            }
            assert!(Instant::now() < deadline, "{got} learners, not {want}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_replicas_elect_replicate_and_snapshot() {
        let link = Arc::new(MemLink::default());
        let budget = Arc::new(Budget::new(u64::MAX, None));
        let a = replica(&link, 1, 50, &budget).await;
        let b = replica(&link, 2, 50, &budget).await;
        let c = replica(&link, 3, 50, &budget).await;
        let mut initial = CounterState::new();
        for i in 0..10 {
            initial.apply(1, &cmd(&["SET", &format!("k{i}"), "v"]), &LIMITS);
        }
        a.bootstrap(NodeId::from_u128(1), &initial, 5, Duration::from_secs(5))
            .await
            .unwrap();
        for (r, n) in [(&b, 2u128), (&c, 3)] {
            a.raft
                .add_learner(
                    r.replica,
                    Peer {
                        node: NodeId::from_u128(n),
                    },
                    true,
                )
                .await
                .unwrap();
        }
        a.raft
            .change_membership(BTreeSet::from([a.replica, b.replica, c.replica]), false)
            .await
            .unwrap();
        assert_eq!(a.voters().len(), 3);
        for _ in 0..300 {
            a.raft
                .client_write(Op {
                    stamp_ms: 10,
                    command: cmd(&["INCR", "cnt:x"]),
                })
                .await
                .unwrap();
        }
        for r in [&a, &b, &c] {
            wait_applied(r, "cnt:x", "300").await;
            wait_applied(r, "k9", "v").await;
        }
        assert_eq!(b.leader_node(), Some(NodeId::from_u128(1)));

        let d = replica(&link, 4, 50, &budget).await;
        a.raft
            .add_learner(
                d.replica,
                Peer {
                    node: NodeId::from_u128(4),
                },
                true,
            )
            .await
            .unwrap();
        wait_applied(&d, "cnt:x", "300").await;
        wait_applied(&d, "k0", "v").await;
        assert!(d.sm.lock().installed >= 1, "the learner got no snapshot");
        wait_learners(&d, 1).await;
        for r in [&a, &b, &c, &d] {
            r.shutdown().await;
        }
    }

    #[tokio::test]
    async fn a_lost_peer_is_refused() {
        let link = Arc::new(MemLink::default());
        let budget = Arc::new(Budget::new(u64::MAX, None));
        let a = replica(&link, 1, 50, &budget).await;
        let reply = a.handle(Message::RaftLeader(vec![])).await;
        assert_eq!(reply, Message::RaftReply(RaftReply::refused()));
        a.shutdown().await;
    }
}
