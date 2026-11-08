//! Raft mode of a group (spec sections 10.2 and 10.3): the up switch, membership moves toward
//! the target voters, quorum-loss recovery, and the down switch.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::{CounterMode, GroupHint, Message, RaftConfig, RaftReply, Regime};
use futures::future::BoxFuture;
use openraft::error::{ClientWriteError, RaftError};
use openraft::ChangeMembers;
use parking_lot::Mutex;
use tracing::Instrument;

use super::group::{Group, Role};
use super::lf::LfLeader;
use super::raft::network::RaftLink;
use super::raft::{new_replica_id, Op, Peer, Replica};
use super::state::{self, CounterState};
use super::{Counters, Refusal, View};

/// How long a membership change may take.
const CHANGE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a new instance may take to elect itself and commit its state.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5);
/// A replica that its own membership no longer lists leaves after this long.
const ORPHAN_AFTER: Duration = Duration::from_secs(5);

/// The Raft role of a node in one group.
pub struct RaftRole {
    pub replica: Arc<Replica>,
    last_stamp: Mutex<u64>,
    /// Since when this replica leads. A new leader has heard from nobody yet, so it measures the
    /// silence of a voter from here, never from the start of the replica.
    leading_since: Mutex<Option<std::time::Instant>>,
}

impl RaftRole {
    pub fn new(replica: Replica) -> Arc<RaftRole> {
        Arc::new(RaftRole {
            replica: Arc::new(replica),
            last_stamp: Mutex::new(0),
            leading_since: Mutex::new(None),
        })
    }

    pub fn regime(&self) -> Regime {
        self.replica.regime
    }

    fn stamp(&self, now_ms: u64) -> u64 {
        let mut last = self.last_stamp.lock();
        *last = now_ms.max(*last);
        *last
    }

    /// Run one command on the leader: a read after ReadIndex, a write through the log.
    pub async fn execute(
        &self,
        cmd: &Command,
        now_ms: u64,
        timeout: Duration,
    ) -> Result<Value, Refusal> {
        let span = crate::telemetry::raft_span();
        self.run(cmd, now_ms, timeout).instrument(span).await
    }

    async fn run(&self, cmd: &Command, now_ms: u64, timeout: Duration) -> Result<Value, Refusal> {
        let raft = &self.replica.raft;
        if state::is_read(cmd) {
            return match tokio::time::timeout(timeout, raft.ensure_linearizable()).await {
                Ok(Ok(_)) => Ok(self.replica.sm.lock().state.get().read(now_ms, cmd)),
                _ => Err(Refusal::NotLeader),
            };
        }
        let op = Op {
            stamp_ms: self.stamp(now_ms),
            command: cmd.clone(),
        };
        match tokio::time::timeout(timeout, raft.client_write(op)).await {
            Ok(Ok(resp)) => Ok(resp.data),
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))) => {
                Err(Refusal::NotLeader)
            }
            _ => Err(Refusal::InDoubt),
        }
    }

    /// This replica leads the group. A leader that removed itself keeps the Raft leader state
    /// for a short while, but it is no voter and does not lead.
    pub fn leads(&self) -> bool {
        self.replica.is_leader() && self.is_voter()
    }

    /// "leader", "voter", or "learner" in the membership this replica knows.
    pub fn role_name(&self) -> &'static str {
        if self.leads() {
            "leader"
        } else if self.is_voter() {
            "voter"
        } else {
            "learner"
        }
    }

    pub fn is_voter(&self) -> bool {
        self.replica
            .voters()
            .iter()
            .any(|(id, _)| *id == self.replica.replica)
    }

    /// The hint of the Raft leader that this replica knows.
    pub fn hint(&self, g: u16) -> Option<GroupHint> {
        let m = self.replica.metrics();
        let leader = self.replica.leader_node()?;
        Some(GroupHint {
            group: g,
            mode: CounterMode::Raft,
            regime: self.regime(),
            leader,
            term: m.current_term,
            follower: None,
            voters: self.replica.voters().into_iter().map(|(_, n)| n).collect(),
            learners: self
                .replica
                .learners()
                .into_iter()
                .map(|(_, n)| n)
                .collect(),
        })
    }
}

/// The learners to promote in one change, from the caught-up target learners in `ready`.
///
/// A single voter takes at least two new voters at once. One voter plus one learner would make a
/// group of two, whose quorum needs both. A node that joins again discards its replica (spec
/// section 6.3), and its replica ID never answers again. If that happens to the one new voter,
/// neither the joint change nor any later change can commit, and only a new instance could free
/// the group. Two new voters let the change commit with either of them.
fn promotion(voters: usize, ready: &[(u64, NodeId)]) -> Option<BTreeSet<u64>> {
    let take = match voters {
        0 | 1 if ready.len() < 2 => return None,
        0 | 1 => ready.len(),
        _ => 1,
    };
    let promote: BTreeSet<u64> = ready.iter().take(take).map(|(id, _)| *id).collect();
    (!promote.is_empty()).then_some(promote)
}

/// The Raft network of a node: every Raft message goes on the bulk lane.
pub struct Link(pub std::sync::Weak<Counters>);

impl RaftLink for Link {
    fn send(
        &self,
        to: NodeId,
        message: Message,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<Message, String>> {
        let counters = self.0.upgrade();
        Box::pin(async move {
            let counters = counters.ok_or("the node is gone")?;
            counters.send(to, message, timeout).await
        })
    }
}

impl Group {
    /// A new instance with this node as the only voter, starting from `state` (spec sections
    /// 10.2 and 10.3).
    pub(super) async fn create_instance(
        &self,
        counters: &Arc<Counters>,
        view: &View,
        seen: u64,
        state: CounterState,
        why: &str,
    ) -> bool {
        let regime = self.next_regime(view.me);
        let replica = match Replica::start(
            &counters.settings.raft,
            self.g,
            regime,
            new_replica_id(),
            counters.raft_link(),
            counters.budget().clone(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(group = self.g, error = %e, "cannot start a Raft replica");
                return false;
            }
        };
        let now_ms = counters.now_ms();
        if let Err(e) = replica
            .bootstrap(view.me, &state, now_ms, BOOTSTRAP_TIMEOUT)
            .await
        {
            tracing::warn!(group = self.g, error = %e, "cannot bootstrap a Raft group");
            replica.shutdown().await;
            return false;
        }
        let installed = self.set_role_if(seen, Role::Raft(RaftRole::new(replica)));
        if installed {
            tracing::info!(group = self.g, %regime, why, "created a Raft group");
            discus_telemetry::global()
                .counter_mode_switch(discus_telemetry::attrs::CounterMode::Raft);
        }
        installed
    }

    /// The Raft branch of a step.
    pub(super) async fn raft_step(
        &self,
        counters: &Arc<Counters>,
        view: &View,
        seen: u64,
        r: &Arc<RaftRole>,
    ) {
        let replica = &r.replica;
        let voters = replica.voters();
        let me_voter = voters.iter().any(|(id, _)| *id == replica.replica);
        if view.normal_count() < 3 {
            self.down_switch(counters, view, seen, r, me_voter).await;
            return;
        }
        // Two of three voters are gone: the survivor reconfigures to itself (spec section 10.2).
        // This comes before the leader branch, because a leader without a quorum cannot remove
        // the lost voters.
        let others_alive = voters
            .iter()
            .any(|(id, n)| *id != replica.replica && view.normal(*n));
        if me_voter && voters.len() > 1 && !others_alive && view.me_normal {
            let state = replica.state();
            self.create_instance(counters, view, seen, state, "the other voters are gone")
                .await;
            return;
        }
        if r.leads() {
            self.reconcile(counters, view, r).await;
            return;
        }
        *r.leading_since.lock() = None;
        // A learner of a group whose voters are all gone: the top target rebuilds it empty.
        if !me_voter && !voters.is_empty() && !voters.iter().any(|(_, n)| view.normal(*n)) {
            if view.ranked(self.g).first() == Some(&view.me) {
                self.create_instance(
                    counters,
                    view,
                    seen,
                    CounterState::new(),
                    "every voter is gone",
                )
                .await;
            } else {
                self.set_role_if(seen, Role::Idle);
            }
            return;
        }
        let known_newer = self
            .inner()
            .known
            .as_ref()
            .is_some_and(|k| k.regime > r.regime());
        let listed = me_voter
            || replica
                .learners()
                .iter()
                .any(|(id, _)| *id == replica.replica);
        let orphan = !listed && replica.started.elapsed() > ORPHAN_AFTER;
        if known_newer || orphan {
            self.set_role_if(seen, Role::Idle);
        }
    }

    /// Below three `NORMAL` members the group goes back to leader-follower mode at once. The
    /// Raft leader takes the lease, else a surviving voter (spec section 10.3).
    async fn down_switch(
        &self,
        counters: &Arc<Counters>,
        view: &View,
        seen: u64,
        r: &Arc<RaftRole>,
        me_voter: bool,
    ) {
        if !view.me_normal {
            // A `LEAVING` replica cannot take the lease, and the survivors switch without it.
            self.set_role_if(seen, Role::Idle);
            return;
        }
        let replica = &r.replica;
        let leader = replica.leader_node().filter(|l| view.normal(*l));
        let takes = if r.leads() {
            true
        } else if me_voter && leader.is_none() {
            if self.lease_kind() {
                true
            } else {
                let voters: BTreeSet<NodeId> =
                    replica.voters().into_iter().map(|(_, n)| n).collect();
                view.ranked(self.g).into_iter().find(|n| voters.contains(n)) == Some(view.me)
            }
        } else {
            false
        };
        if !takes {
            return;
        }
        let lease_until = if self.lease_kind() {
            match self.acquire(counters).await {
                Some(Ok(until)) => Some(until),
                _ => return,
            }
        } else {
            None
        };
        let state = replica.state();
        let regime = self.next_regime(view.me);
        let leader = LfLeader::new(
            regime,
            state,
            counters.budget(),
            lease_until,
            counters.settings.lease_margin,
        );
        if self.set_role_if(seen, Role::Leader(leader.clone())) {
            tracing::info!(group = self.g, %regime, "switched down to leader-follower mode");
            discus_telemetry::global()
                .counter_mode_switch(discus_telemetry::attrs::CounterMode::LeaderFollower);
            self.start_leader(counters, view, leader).await;
        }
    }

    /// Move the membership one change toward the target voters (spec section 10.2).
    async fn reconcile(&self, counters: &Arc<Counters>, view: &View, r: &Arc<RaftRole>) {
        let replica = &r.replica;
        let leading_since = *r
            .leading_since
            .lock()
            .get_or_insert_with(std::time::Instant::now);
        if replica.in_joint_change() {
            return;
        }
        let settings = &counters.settings;
        let targets: Vec<NodeId> = view.ranked(self.g).into_iter().take(3).collect();
        let voters = replica.voters();
        let learners = replica.learners();
        let raft = &replica.raft;
        let gone = voters.iter().find(|(id, n)| {
            *id != replica.replica
                && (!view.epoch.member(*n).is_some_and(|m| {
                    m.state == discus_placement::MemberState::Normal
                        || m.state == discus_placement::MemberState::Leaving
                }) || replica.contacts.silent_for(*id, leading_since) > settings.voter_timeout)
        });
        // Each move takes the rate limit only when it acts. A group with nothing to move must
        // not take the slot, or it can starve the groups that have a move for many seconds.
        if let Some((id, n)) = gone {
            if !counters.may_change() {
                return;
            }
            tracing::info!(group = self.g, voter = %n, "removing a lost voter");
            let change = ChangeMembers::RemoveVoters(BTreeSet::from([*id]));
            let _ =
                tokio::time::timeout(CHANGE_TIMEOUT, raft.change_membership(change, false)).await;
            return;
        }
        // A node that joins again discards its replica, and the replica ID never comes back. The
        // node stays listed under that ID, so it gets no new replica until the learner goes.
        let lost_learner = learners.iter().find(|(id, _)| {
            replica.contacts.silent_for(*id, leading_since) > settings.voter_timeout
        });
        if let Some((id, n)) = lost_learner {
            if !counters.may_change() {
                return;
            }
            tracing::info!(group = self.g, learner = %n, "removing a lost learner");
            let change = ChangeMembers::RemoveNodes(BTreeSet::from([*id]));
            let _ =
                tokio::time::timeout(CHANGE_TIMEOUT, raft.change_membership(change, false)).await;
            return;
        }
        let listed =
            |n: &NodeId| voters.iter().any(|(_, v)| v == n) || learners.iter().any(|(_, l)| l == n);
        if let Some(t) = targets.iter().find(|t| !listed(t)) {
            if !counters.may_change() {
                return;
            }
            let ask = Message::RaftConfig(RaftConfig {
                group: self.g,
                regime: r.regime(),
                replica: 0,
                leader: view.me,
            });
            if let Ok(Message::RaftConfig(reply)) = counters.send(*t, ask, CHANGE_TIMEOUT).await {
                if reply.replica != 0 {
                    tracing::info!(group = self.g, learner = %t, "adding a learner");
                    // A new learner has not replied yet. Its silence counts from here.
                    replica.contacts.saw(reply.replica);
                    let peer = Peer { node: *t };
                    let _ = tokio::time::timeout(
                        CHANGE_TIMEOUT,
                        raft.add_learner(reply.replica, peer, false),
                    )
                    .await;
                }
            }
            return;
        }
        let metrics = replica.metrics();
        let last = metrics.last_log_index.unwrap_or(0);
        let caught_up = |id: &u64| {
            metrics
                .replication
                .as_ref()
                .and_then(|m| m.get(id).cloned().flatten())
                .is_some_and(|l| l.index + settings.catchup_entries >= last)
        };
        let ready: Vec<(u64, NodeId)> = learners
            .iter()
            .filter(|(id, n)| targets.contains(n) && caught_up(id))
            .copied()
            .collect();
        if let Some(promote) = promotion(voters.len(), &ready) {
            if !counters.may_change() {
                return;
            }
            let nodes: Vec<NodeId> = ready
                .iter()
                .filter(|(id, _)| promote.contains(id))
                .map(|(_, n)| *n)
                .collect();
            tracing::info!(group = self.g, voters = ?nodes, "promoting learners");
            let change = ChangeMembers::AddVoterIds(promote);
            let _ =
                tokio::time::timeout(CHANGE_TIMEOUT, raft.change_membership(change, false)).await;
            return;
        }
        let complete = targets.iter().all(|t| voters.iter().any(|(_, v)| v == t));
        if complete {
            // Others first, so the leader steps down only when nothing else is left to move.
            let extra = voters
                .iter()
                .filter(|(_, n)| !targets.contains(n))
                .min_by_key(|(_, n)| *n == view.me);
            if let Some((id, n)) = extra {
                if !counters.may_change() {
                    return;
                }
                tracing::info!(group = self.g, voter = %n, "removing a voter that is not a target");
                let change = ChangeMembers::RemoveVoters(BTreeSet::from([*id]));
                let _ = tokio::time::timeout(CHANGE_TIMEOUT, raft.change_membership(change, false))
                    .await;
                return;
            }
        }
        if let Some((id, _)) = learners.iter().find(|(_, n)| !targets.contains(n)) {
            if !counters.may_change() {
                return;
            }
            let change = ChangeMembers::RemoveNodes(BTreeSet::from([*id]));
            let _ =
                tokio::time::timeout(CHANGE_TIMEOUT, raft.change_membership(change, false)).await;
        }
    }

    /// Every voter of the best known Raft regime is gone: the top target creates the group
    /// again, empty (spec section 10.2).
    pub(super) async fn rebuild_if_lost(&self, counters: &Arc<Counters>, view: &View, seen: u64) {
        let Some(known) = self.inner().known.clone() else {
            return;
        };
        if self.leader(view).is_some() {
            self.inner().leaderless = None;
            return;
        }
        if known.mode != CounterMode::Raft || view.ranked(self.g).first() != Some(&view.me) {
            return;
        }
        let alive: Vec<NodeId> = known
            .voters
            .iter()
            .copied()
            .filter(|v| *v != view.me && view.normal(*v))
            .collect();
        if !alive.is_empty() {
            // The known voters can be an old view. After a long time without a leader, ask:
            // a `NORMAL` member that answers without a replica of the group is no voter.
            let leaderless = {
                let mut inner = self.inner();
                *inner.leaderless.get_or_insert_with(std::time::Instant::now)
            };
            if leaderless.elapsed() < 2 * counters.settings.lease_duration || !self.may_ask() {
                return;
            }
            let (answered, holders) = counters.holders(self.g).await;
            if !alive
                .iter()
                .all(|v| answered.contains(v) && !holders.contains(v))
            {
                return;
            }
        }
        self.create_instance(
            counters,
            view,
            seen,
            CounterState::new(),
            "every voter is gone",
        )
        .await;
    }

    /// `RAFT_FORCE_RECONFIGURE`: a voter makes itself the only voter of a new instance with its
    /// state, without waiting for the detector (spec sections 10.2 and 14.2).
    pub async fn force_reconfigure(&self, counters: &Arc<Counters>, view: &View) -> bool {
        let seen = self.version();
        let Role::Raft(r) = self.role() else {
            return false;
        };
        if !r.is_voter() || !view.me_normal {
            return false;
        }
        let state = r.replica.state();
        self.create_instance(counters, view, seen, state, "a forced reconfiguration")
            .await
    }

    /// Answer `RAFT_CONFIG`: prepare a learner replica for the regime of the leader.
    pub async fn on_raft_config(
        &self,
        counters: &Arc<Counters>,
        view: &View,
        c: &RaftConfig,
    ) -> RaftConfig {
        let refuse = RaftConfig { replica: 0, ..*c };
        if !view.me_normal {
            return refuse;
        }
        let seen = self.version();
        let role = self.role();
        match &role {
            Role::Raft(r) if r.regime() == c.regime => {
                return RaftConfig {
                    replica: r.replica.replica,
                    ..*c
                }
            }
            other if other.regime().is_some_and(|reg| reg > c.regime) => return refuse,
            _ => {}
        }
        let replica = match Replica::start(
            &counters.settings.raft,
            self.g,
            c.regime,
            new_replica_id(),
            counters.raft_link(),
            counters.budget().clone(),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => return refuse,
        };
        let id = replica.replica;
        let role = RaftRole::new(replica);
        if !self.set_role_if(seen, Role::Raft(role.clone())) {
            role.replica.shutdown().await;
            return refuse;
        }
        {
            let mut inner = self.inner();
            inner.max_generation = inner.max_generation.max(c.regime.generation);
        }
        RaftConfig { replica: id, ..*c }
    }

    /// Answer a Raft RPC with the replica of its regime and target.
    pub async fn on_raft(&self, header: &discus_wire::RaftHeader, message: Message) -> Message {
        match self.role() {
            Role::Raft(r) if r.regime() == header.regime && r.replica.replica == header.target => {
                r.replica.handle(message).await
            }
            _ => Message::RaftReply(RaftReply::refused()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(n: u64) -> Vec<(u64, NodeId)> {
        (1..=n).map(|i| (i, NodeId::from_u128(i.into()))).collect()
    }

    #[test]
    fn a_single_voter_never_takes_one_new_voter() {
        assert_eq!(promotion(1, &ready(1)), None);
        assert_eq!(promotion(1, &ready(0)), None);
        assert_eq!(promotion(1, &ready(2)), Some(BTreeSet::from([1, 2])));
        assert_eq!(promotion(1, &ready(3)), Some(BTreeSet::from([1, 2, 3])));
    }

    #[test]
    fn a_group_of_two_or_more_takes_one_voter_at_a_time() {
        assert_eq!(promotion(2, &ready(0)), None);
        assert_eq!(promotion(2, &ready(2)), Some(BTreeSet::from([1])));
        assert_eq!(promotion(3, &ready(1)), Some(BTreeSet::from([1])));
    }
}
