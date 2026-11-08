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
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::{Message, RaftReply, Regime};
use openraft::{Config, Raft, RaftMetrics, ServerState, SnapshotPolicy};
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

/// One log entry: the leader stamp and the command (spec section 10.4).
#[derive(Debug, Clone, PartialEq)]
pub struct Op {
    pub stamp_ms: u64,
    pub command: Command,
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}",
            String::from_utf8_lossy(&self.command[0]),
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
    pub limits: Limits,
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
    pub raft: Raft<TypeConfig>,
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
        let sm = Arc::new(Mutex::new(Sm::new(budget, timing.limits)));
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
        let restore = vec![
            Bytes::from_static(RESTORE),
            Bytes::from(store::encode_state(initial)),
        ];
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
                Some(req) => match self.raft.append_entries(req).await {
                    Ok(r) => convert::append_reply_to(&r),
                    Err(_) => RaftReply::refused(),
                },
                None => RaftReply::refused(),
            },
            Message::RaftVote(v) => match self.raft.vote(convert::vote_req_of(v)).await {
                Ok(r) => convert::vote_reply_to(&r),
                Err(_) => RaftReply::refused(),
            },
            Message::RaftSnapshot(s) => {
                match self.raft.install_snapshot(convert::snapshot_of(s)).await {
                    Ok(r) => convert::snapshot_reply_to(&r),
                    Err(_) => RaftReply::refused(),
                }
            }
            _ => RaftReply::refused(),
        };
        Message::RaftReply(reply)
    }

    pub fn metrics(&self) -> RaftMetrics<u64, Peer> {
        self.raft.metrics().borrow().clone()
    }

    pub fn is_leader(&self) -> bool {
        let m = self.metrics();
        m.state == ServerState::Leader && m.current_leader == Some(self.replica)
    }

    /// The node of a replica ID in the current membership.
    pub fn node_of(&self, replica: u64) -> Option<NodeId> {
        self.metrics()
            .membership_config
            .membership()
            .get_node(&replica)
            .map(|p| p.node)
    }

    pub fn leader_node(&self) -> Option<NodeId> {
        let m = self.metrics();
        let leader = m.current_leader?;
        m.membership_config
            .membership()
            .get_node(&leader)
            .map(|p| p.node)
    }

    /// The voters of the current membership, by replica ID and node. During a joint change it is
    /// the union of both sets.
    pub fn voters(&self) -> Vec<(u64, NodeId)> {
        let m = self.metrics();
        let membership = m.membership_config.membership();
        let ids: BTreeSet<u64> = membership.voter_ids().collect();
        ids.into_iter()
            .filter_map(|id| membership.get_node(&id).map(|p| (id, p.node)))
            .collect()
    }

    pub fn learners(&self) -> Vec<(u64, NodeId)> {
        let m = self.metrics();
        let membership = m.membership_config.membership();
        membership
            .learner_ids()
            .filter_map(|id| membership.get_node(&id).map(|p| (id, p.node)))
            .collect()
    }

    pub fn in_joint_change(&self) -> bool {
        self.metrics()
            .membership_config
            .membership()
            .get_joint_config()
            .len()
            > 1
    }

    /// The state that this replica applied.
    pub fn state(&self) -> CounterState {
        self.sm.lock().state.get().clone()
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }
}

/// A random replica ID. Zero means "no replica" on the wire, so it is never used.
pub fn new_replica_id() -> u64 {
    loop {
        let id = fastrand::u64(..);
        if id != 0 {
            return id;
        }
    }
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

    fn timing(snapshot_entries: u64) -> RaftTiming {
        RaftTiming {
            heartbeat_ms: 50,
            election_ms: 300,
            snapshot_entries,
            limits: Limits {
                max_value_bytes: 1 << 20,
            },
        }
    }

    fn cmd(parts: &[&str]) -> Command {
        parts
            .iter()
            .map(|p| Bytes::copy_from_slice(p.as_bytes()))
            .collect()
    }

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
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let got = r.state().get(key.as_bytes()).map(|e| e.value.clone());
            if got.as_deref() == Some(want.as_bytes()) {
                return;
            }
            assert!(Instant::now() < deadline, "{key} is {got:?}, not {want}");
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
        let limits = timing(50).limits;
        for i in 0..10 {
            initial.apply(1, &cmd(&["SET", &format!("k{i}"), "v"]), &limits);
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
        assert_eq!(d.learners().len(), 1);
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
