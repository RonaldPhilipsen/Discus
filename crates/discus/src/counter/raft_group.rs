//! Raft mode of a group (spec sections 10.2 and 10.3): the up switch, membership moves toward
//! the target voters, quorum-loss recovery, and the down switch.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::{CounterMode, GroupHint, Message, RaftConfig, RaftReply, Regime};
use futures::future::BoxFuture;
use openraft::error::{ClientWriteError, RaftError};
use openraft::ChangeMembers;
use parking_lot::Mutex;
use tracing::Instrument;

use super::group::{Group, Role};
use super::raft::network::RaftLink;
use super::raft::{Op, Peer, Replica};
use super::state::{self, CounterState};
use super::{Counters, Refusal, Settings, View};

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
    leading_since: Mutex<Option<Instant>>,
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
        self.replica.is_voter()
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
            voters: self.replica.voter_nodes(),
            learners: self.replica.learner_nodes(),
            ..GroupHint::default()
        })
    }
}

/// The next voter set of a leader, or `None` when the voters stay. `last` is the newest config
/// of the membership, and `joint` says that an older config still counts too.
///
/// The goal is the whole target set or nothing. A group of two needs both voters, and a node that
/// joins again discards its replica for good. Until `want` targets are ready, a dead voter stays
/// listed, which costs no more fault tolerance than removing it. A joint config ends in `desired`
/// when that is whole, and else in `last`. A change needs a live majority of `last`, or its first
/// step never commits and blocks every later change.
fn voter_goal(
    desired: BTreeSet<u64>,
    last: &BTreeSet<u64>,
    joint: bool,
    want: usize,
    live: impl Fn(&u64) -> bool,
) -> Option<BTreeSet<u64>> {
    if 2 * last.iter().filter(|id| live(id)).count() <= last.len() {
        return None;
    }
    if !desired.is_empty() && desired.len() == want && (joint || desired != *last) {
        return Some(desired);
    }
    joint.then(|| last.clone())
}

/// The `NORMAL` voters form a majority of `voters`, so they can elect a leader without the
/// others.
fn normal_quorum(voters: &[NodeId], normal: impl Fn(NodeId) -> bool) -> bool {
    2 * voters.iter().filter(|n| normal(**n)).count() > voters.len()
}

/// One membership change of a leader toward the target voters.
enum Move {
    RemoveNode((u64, NodeId), &'static str),
    AddLearner(NodeId),
    Voters(BTreeSet<u64>),
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
        let replica = match counters.start_replica(self.g, regime).await {
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
        let me_voter = replica.is_voter();
        if view.normal_count() < 3 {
            self.down_switch(counters, view, seen, r).await;
            return;
        }
        // A voter set without a live majority can commit nothing, so no membership change can
        // repair it (spec section 10.2). This comes before the leader branch, and it covers a
        // joint change whose new voters went away. The leader acts, or else the top live voter.
        let live = |(id, n): &(u64, NodeId)| {
            *id == replica.replica || (view.normal(*n) && !replica.contacts.refused(*id))
        };
        let quorum = replica.voter_configs().iter().all(|c| {
            2 * voters
                .iter()
                .filter(|v| c.contains(&v.0) && live(v))
                .count()
                > c.len()
        });
        if me_voter && !quorum && view.me_normal {
            let top = view
                .ranked(self.g)
                .into_iter()
                .find(|n| voters.iter().any(|v| v.1 == *n && live(v)));
            let leader_up = replica.leader_node().is_some_and(|l| view.normal(l));
            if r.leads() || (top == Some(view.me) && !leader_up) {
                // The peers that refused can have moved on to a newer regime.
                if counters
                    .ask_now(self.g)
                    .await
                    .is_some_and(|best| best.regime > r.regime())
                {
                    self.set_role_if(seen, Role::Idle);
                    return;
                }
                let state = replica.state();
                self.create_instance(counters, view, seen, state, "the other voters are gone")
                    .await;
                return;
            }
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
        let listed = me_voter || replica.is_learner();
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
    ) {
        if !view.me_normal {
            // A `LEAVING` replica cannot take the lease, and the survivors switch without it.
            self.set_role_if(seen, Role::Idle);
            return;
        }
        let replica = &r.replica;
        let leader = replica.leader_node().filter(|l| view.normal(*l));
        let voters = replica.voter_nodes();
        // A voter that does not lead holds only the entries that it applied. While the `NORMAL`
        // voters can still elect a leader, an old leader that is `LEAVING` or about to be
        // `DEAD` can still commit with them, so the voter waits for the next leader to switch.
        let takes = if r.leads() {
            true
        } else if r.is_voter() && leader.is_none() && !normal_quorum(&voters, |n| view.normal(n)) {
            self.lease_kind()
                || view.ranked(self.g).into_iter().find(|n| voters.contains(n)) == Some(view.me)
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
        if r.leads() {
            // The read index waits until this leader applied every entry that a quorum
            // committed, also those of earlier leaders.
            let linearizable = tokio::time::timeout(
                counters.settings.timeout,
                replica.raft.ensure_linearizable(),
            )
            .await;
            if !matches!(linearizable, Ok(Ok(_))) {
                return;
            }
        }
        // A leader applies a write before its reply leaves, and after the shutdown no write
        // commits through this replica. So the state read after the shutdown holds every write
        // that this leader acknowledged.
        replica.shutdown().await;
        let why = "switched down to leader-follower mode";
        let Some(leader) = self.become_leader(counters, seen, replica.state(), lease_until, why)
        else {
            return;
        };
        discus_telemetry::global()
            .counter_mode_switch(discus_telemetry::attrs::CounterMode::LeaderFollower);
        self.start_leader(counters, view, leader).await;
    }

    /// The next membership change toward the target voters, in priority order: a lost learner
    /// goes, a joint change ends, a missing target joins, the voters become the targets in one
    /// change, and last the learners that are no target leave.
    fn next_move(
        &self,
        settings: &Settings,
        view: &View,
        replica: &Replica,
        leading_since: Instant,
    ) -> Option<Move> {
        let targets: Vec<NodeId> = view.ranked(self.g).into_iter().take(3).collect();
        let voters = replica.voters();
        let learners = replica.learners();
        let lost = |id: &u64| {
            replica.contacts.refused(*id)
                || replica.contacts.silent_for(*id, leading_since) > settings.voter_timeout
        };
        let alive = |(id, n): &(u64, NodeId)| {
            *id == replica.replica
                || (view.epoch.member(*n).is_some_and(|m| {
                    m.state == discus_placement::MemberState::Normal
                        || m.state == discus_placement::MemberState::Leaving
                }) && !lost(id))
        };
        // A node that joins again discards its replica, and it refuses the old ID. The node
        // stays listed under that ID, so it gets no new replica until the learner goes.
        if let Some(l) = learners.iter().find(|(id, _)| lost(id)) {
            return Some(Move::RemoveNode(*l, "removing a lost learner"));
        }
        let metrics = replica.metrics();
        let last_index = metrics.last_log_index.unwrap_or(0);
        let caught_up = |id: &u64| {
            metrics
                .replication
                .as_ref()
                .and_then(|m| m.get(id).cloned().flatten())
                .is_some_and(|l| l.index + settings.catchup_entries >= last_index)
        };
        // One replica per target: its live voter, else a learner that caught up.
        let desired: BTreeSet<u64> = targets
            .iter()
            .filter_map(|t| {
                voters
                    .iter()
                    .find(|v| v.1 == *t && alive(v))
                    .or_else(|| learners.iter().find(|l| l.1 == *t && caught_up(&l.0)))
                    .map(|v| v.0)
            })
            .collect();
        let configs = replica.voter_configs();
        let joint = configs.len() > 1;
        let last = configs.last().cloned().unwrap_or_default();
        let live = |id: &u64| voters.iter().any(|v| v.0 == *id && alive(v));
        let goal = voter_goal(desired, &last, joint, targets.len(), live).map(Move::Voters);
        if joint && goal.is_some() {
            return goal;
        }
        // A dead replica does not list its node. A target whose replica went away gets a new one
        // under a new ID, and the voter change then drops the old ID.
        let listed = |t: &NodeId| {
            voters.iter().any(|v| v.1 == *t && alive(v))
                || learners.iter().any(|l| l.1 == *t && !lost(&l.0))
        };
        if let Some(t) = targets.iter().find(|t| !listed(t)) {
            return Some(Move::AddLearner(*t));
        }
        goal.or_else(|| {
            learners
                .iter()
                .find(|(_, n)| !targets.contains(n))
                .map(|l| Move::RemoveNode(*l, "removing a learner that is not a target"))
        })
    }

    /// Move the membership one change toward the target voters (spec section 10.2).
    async fn reconcile(&self, counters: &Arc<Counters>, view: &View, r: &Arc<RaftRole>) {
        let replica = &r.replica;
        let leading_since = *r.leading_since.lock().get_or_insert_with(Instant::now);
        // openraft refuses a change until the last one commits. A proposal now only burns the
        // rate limit, and an uncommittable one is the case of the quorum check in `raft_step`.
        if replica.change_pending() {
            return;
        }
        let Some(next) = self.next_move(&counters.settings, view, replica, leading_since) else {
            return;
        };
        // Each move takes the rate limit only when it acts. A group with nothing to move must
        // not take the slot, or it can starve the groups that have a move for many seconds.
        if !counters.may_change() {
            return;
        }
        let raft = &replica.raft;
        let change = match next {
            Move::RemoveNode((id, n), why) => {
                tracing::info!(group = self.g, learner = %n, "{why}");
                ChangeMembers::RemoveNodes(BTreeSet::from([id]))
            }
            // The leader can be absent from the new set. It commits the change and steps down.
            Move::Voters(ids) => {
                tracing::info!(group = self.g, voters = ?ids, "changing the voters");
                ChangeMembers::ReplaceAllVoters(ids)
            }
            Move::AddLearner(t) => {
                let ask = Message::RaftConfig(RaftConfig {
                    group: self.g,
                    regime: r.regime(),
                    replica: 0,
                    leader: view.me,
                });
                let Ok(Message::RaftConfig(reply)) = counters.send(t, ask, CHANGE_TIMEOUT).await
                else {
                    return;
                };
                if reply.replica == 0 {
                    return;
                }
                tracing::info!(group = self.g, learner = %t, "adding a learner");
                // A new learner has not replied yet. Its silence counts from here.
                replica.contacts.saw(reply.replica);
                let add = raft.add_learner(reply.replica, Peer { node: t }, false);
                let _ = tokio::time::timeout(CHANGE_TIMEOUT, add).await;
                return;
            }
        };
        let _ = tokio::time::timeout(CHANGE_TIMEOUT, raft.change_membership(change, false)).await;
    }

    /// Every voter of the best known Raft regime is gone: the top target creates the group
    /// again, empty (spec section 10.2).
    pub(super) async fn rebuild_if_lost(&self, counters: &Arc<Counters>, view: &View, seen: u64) {
        let Some(known) = self.inner().known.clone() else {
            return;
        };
        if self.leader(view.me, &view.epoch).is_some() {
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
                *inner.leaderless.get_or_insert_with(Instant::now)
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
        let Ok(replica) = counters.start_replica(self.g, c.regime).await else {
            return refuse;
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

    fn set(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    const ALL: fn(&u64) -> bool = |_| true;

    #[test]
    fn a_single_voter_waits_for_every_target() {
        assert_eq!(voter_goal(set(&[1, 2]), &set(&[1]), false, 3, ALL), None);
        assert_eq!(
            voter_goal(set(&[1, 2, 3]), &set(&[1]), false, 3, ALL),
            Some(set(&[1, 2, 3]))
        );
    }

    #[test]
    fn the_voters_change_in_one_step_and_the_leader_can_leave() {
        assert_eq!(
            voter_goal(set(&[2, 3, 4]), &set(&[1, 2, 3]), false, 3, ALL),
            Some(set(&[2, 3, 4]))
        );
    }

    #[test]
    fn a_dead_voter_stays_until_its_replacement_is_ready() {
        let live = |id: &u64| *id != 3;
        assert_eq!(
            voter_goal(set(&[1, 2]), &set(&[1, 2, 3]), false, 3, live),
            None
        );
        assert_eq!(
            voter_goal(set(&[1, 2, 4]), &set(&[1, 2, 3]), false, 3, live),
            Some(set(&[1, 2, 4]))
        );
    }

    #[test]
    fn no_change_starts_without_a_live_majority_of_the_voters() {
        let live = |id: &u64| *id == 1;
        assert_eq!(
            voter_goal(set(&[1, 2, 4]), &set(&[1, 2, 3]), false, 3, live),
            None
        );
        assert_eq!(
            voter_goal(set(&[1, 2, 3]), &set(&[1, 2, 3]), true, 3, live),
            None
        );
    }

    #[test]
    fn a_joint_change_ends_even_when_a_target_is_lost() {
        assert_eq!(
            voter_goal(set(&[1, 2, 3]), &set(&[1, 2, 3]), true, 3, ALL),
            Some(set(&[1, 2, 3])),
            "the left-behind joint config ends in the set it aimed for"
        );
        let live = |id: &u64| *id != 2;
        assert_eq!(
            voter_goal(set(&[1, 3]), &set(&[1, 2, 3]), true, 3, live),
            Some(set(&[1, 2, 3])),
            "without a whole target set it still leaves the joint config"
        );
        assert_eq!(
            voter_goal(set(&[1, 3, 4]), &set(&[1, 2, 3]), true, 3, live),
            Some(set(&[1, 3, 4])),
            "a replacement that is ready wins"
        );
    }

    #[test]
    fn no_change_when_the_voters_are_the_targets() {
        let v = set(&[1, 2, 3]);
        assert_eq!(voter_goal(v.clone(), &v, false, 3, ALL), None);
    }

    #[test]
    fn the_normal_voters_can_elect_with_a_majority() {
        let ids: Vec<NodeId> = (1..=3u128).map(NodeId::from_u128).collect();
        let normal =
            |up: &'static [u128]| move |n: NodeId| up.iter().any(|u| NodeId::from_u128(*u) == n);
        assert!(normal_quorum(&ids, normal(&[1, 2, 3])));
        assert!(normal_quorum(&ids, normal(&[1, 2])));
        assert!(!normal_quorum(&ids, normal(&[1])));
        assert!(normal_quorum(&ids[..2], normal(&[1, 2])));
        assert!(!normal_quorum(&ids[..2], normal(&[1])));
    }
}
