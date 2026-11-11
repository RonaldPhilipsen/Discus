//! The controller of one counter group: which role this node has in it (spec sections 10.1 to
//! 10.3).
//!
//! Each step reads the epoch, the hints, and the lease, and moves the role one transition at a
//! time. Inbound syncs change the role too. A transition that waits (a lease call) checks the
//! role version afterwards and gives up when the role changed meanwhile.

use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_membership::{LeaseHolder, LeaseProvider, LeaseState};
use discus_placement::{Epoch, NodeId};
use discus_wire::{CounterAck, CounterMode, CounterSync, GroupHint, Regime};
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::lf::{Follower, LfLeader};
use super::raft_group::RaftRole;
use super::state::CounterState;
use super::{normal, Counters, View};

/// The role of this node in one group.
#[derive(Clone, Default)]
pub enum Role {
    #[default]
    Idle,
    Follower(Arc<Follower>),
    Leader(Arc<LfLeader>),
    Raft(Arc<RaftRole>),
}

impl Role {
    pub fn name(&self) -> &'static str {
        match self {
            Role::Idle => "idle",
            Role::Follower(_) => "follower",
            Role::Leader(_) => "leader",
            Role::Raft(r) => r.role_name(),
        }
    }

    pub(super) fn regime(&self) -> Option<Regime> {
        match self {
            Role::Idle => None,
            Role::Follower(f) => Some(f.regime),
            Role::Leader(l) => Some(l.regime),
            Role::Raft(r) => Some(r.regime()),
        }
    }

    /// The state that this role holds, if any. A new leader starts from it.
    pub(super) fn state(&self) -> Option<CounterState> {
        match self {
            Role::Idle => None,
            Role::Follower(f) => Some(f.state()),
            Role::Leader(l) => Some(l.state()),
            Role::Raft(r) => Some(r.replica.state()),
        }
    }
}

#[derive(Default)]
pub struct Inner {
    pub role: Role,
    /// Raised on every role change, so a transition that waited can see a newer role.
    pub version: u64,
    /// The best hint this node knows. It stays after its leader dies, so the next leader can
    /// follow the epoch rule.
    pub known: Option<GroupHint>,
    /// The largest regime generation seen. A new regime takes the next one.
    pub max_generation: u64,
    /// A leader that refused a command recently. Routing skips it until the time passes.
    pub distrusted: Option<(NodeId, Instant)>,
    /// The last `RAFT_LEADER` round for this group.
    pub asked: Option<Instant>,
    /// Since when this node knows no live leader.
    pub leaderless: Option<Instant>,
    /// The last lease call of this group.
    pub tried: Option<Instant>,
    /// The first role this node had in the current regime, for tests.
    pub first_role: Option<(Regime, &'static str)>,
}

/// How long routing skips a leader that refused a command.
const DISTRUST: Duration = Duration::from_millis(500);
/// The shortest time between two `RAFT_LEADER` rounds of one group.
pub const ASK_EVERY: Duration = Duration::from_millis(200);

/// Order two hints: the newer regime, then the higher Raft term, then a named leader over the
/// nil one. A term has at most one leader, so a hint that names it beats a hint of the same term
/// that lost its leader. Else the nil hint blocks the leader of its term until the next election.
/// It appears when an answer to `RAFT_LEADER` names a new leader before its gossip claim arrives.
pub fn newer(a: &GroupHint, b: &GroupHint) -> bool {
    let rank = |h: &GroupHint| (h.regime, h.term, h.leader != NodeId::NIL);
    rank(a) > rank(b)
}

pub struct Group {
    pub g: u16,
    inner: Mutex<Inner>,
    pub(super) lease: Option<Arc<dyn LeaseProvider>>,
    /// Woken when the role or the known leader changes, so waiting commands look again.
    pub changed: Notify,
}

impl Group {
    pub fn new(g: u16, lease: Option<Arc<dyn LeaseProvider>>) -> Group {
        Group {
            g,
            inner: Mutex::default(),
            lease,
            changed: Notify::new(),
        }
    }

    pub fn inner(&self) -> parking_lot::MutexGuard<'_, Inner> {
        self.inner.lock()
    }

    pub fn role(&self) -> Role {
        self.inner.lock().role.clone()
    }

    pub(super) fn version(&self) -> u64 {
        self.inner.lock().version
    }

    /// Install `role` unless the role changed after `seen`.
    pub(super) fn set_role_if(&self, seen: u64, role: Role) -> bool {
        let mut inner = self.inner.lock();
        if inner.version != seen {
            drop(inner);
            stop(&role);
            return false;
        }
        let old = std::mem::replace(&mut inner.role, role);
        inner.version += 1;
        if let Some(regime) = inner.role.regime() {
            inner.max_generation = inner.max_generation.max(regime.generation);
            if inner.first_role.is_none_or(|(r, _)| r != regime) {
                inner.first_role = Some((regime, inner.role.name()));
            }
        }
        drop(inner);
        stop(&old);
        self.changed.notify_waiters();
        true
    }

    fn set_role(&self, role: Role) {
        let seen = self.version();
        self.set_role_if(seen, role);
    }

    /// Drop every role and every state of this group.
    pub fn clear(&self) {
        self.set_role(Role::Idle);
        let mut inner = self.inner.lock();
        inner.known = None;
        inner.leaderless = None;
        inner.first_role = None;
    }

    pub(super) fn next_regime(&self, me: NodeId) -> Regime {
        let mut inner = self.inner.lock();
        inner.max_generation += 1;
        Regime {
            generation: inner.max_generation,
            origin: me,
        }
    }

    /// The node that leads this group, as far as this node knows.
    pub fn leader(&self, me: NodeId, epoch: &Epoch) -> Option<NodeId> {
        let inner = self.inner.lock();
        let distrusted = inner
            .distrusted
            .filter(|(_, at)| at.elapsed() < DISTRUST)
            .map(|(n, _)| n);
        let usable = |l: &NodeId| *l != me && normal(epoch, *l) && Some(*l) != distrusted;
        let stale = |r: Regime| inner.known.as_ref().is_some_and(|k| k.regime > r);
        let held = match &inner.role {
            Role::Leader(l) if l.serving() => return Some(me),
            Role::Raft(r) if r.leads() => return Some(me),
            Role::Follower(f) if !stale(f.regime) => f.leader.filter(usable),
            Role::Raft(r) => r.replica.leader_node().filter(usable),
            _ => None,
        };
        held.or_else(|| inner.known.as_ref().map(|k| k.leader).filter(usable))
    }

    pub fn distrust(&self, leader: NodeId) {
        self.inner.lock().distrusted = Some((leader, Instant::now()));
    }

    /// True when a `RAFT_LEADER` round may start now. It marks the round as started.
    pub fn may_ask(&self) -> bool {
        let mut inner = self.inner.lock();
        if inner.asked.is_some_and(|at| at.elapsed() < ASK_EVERY) {
            return false;
        }
        inner.asked = Some(Instant::now());
        true
    }

    /// Take the best hint of a round of answers. It replaces the known hint when it is newer, or
    /// when the leader of the known hint answered without claiming the group (`denied`). An older
    /// answer never replaces a hint that nobody denied, because a replica can answer from an old
    /// view.
    pub fn learn(&self, best: Option<GroupHint>, denied: &[NodeId]) {
        let mut inner = self.inner.lock();
        let Some(best) = best else {
            // Nobody claims the group. A known leader that denied it leads no more, but its
            // voters stay known for a rebuild.
            if let Some(k) = inner.known.as_mut().filter(|k| denied.contains(&k.leader)) {
                k.leader = NodeId::NIL;
            }
            return;
        };
        inner.max_generation = inner.max_generation.max(best.regime.generation);
        let take = inner
            .known
            .as_ref()
            .is_none_or(|k| newer(&best, k) || denied.contains(&k.leader));
        if take {
            inner.distrusted = None;
            inner.known = Some(best);
            drop(inner);
            self.changed.notify_waiters();
        }
    }

    /// Merge the current hints of the `NORMAL` members. A newer hint replaces the known one. So
    /// does the current hint of the known leader, when it changed. A known leader that is
    /// `NORMAL` but claims nothing leads no more: the best hint replaces it. Without one, a Raft
    /// hint loses its leader and keeps its voters. Only gossip shows what a member claims, so a
    /// node without gossip skips that rule. A dead leader leaves its hint in place, so the next
    /// leader can follow the epoch rule and see a total voter loss.
    pub fn observe(&self, view: &View, hints: &[GroupHint], gossip: bool) {
        let ours: Vec<&GroupHint> = hints.iter().filter(|h| h.group == self.g).collect();
        let best = ours
            .iter()
            .copied()
            .reduce(|a, b| if newer(b, a) { b } else { a });
        let mut inner = self.inner.lock();
        if let Some(best) = best {
            inner.max_generation = inner.max_generation.max(best.regime.generation);
        }
        let next = match (&inner.known, best) {
            (None, Some(best)) => Some(best.clone()),
            (None, None) => None,
            (Some(k), Some(best)) if newer(best, k) => Some(best.clone()),
            (Some(k), best) => match ours.iter().copied().find(|h| h.leader == k.leader) {
                Some(h) if h != k => Some(h.clone()),
                None if k.leader != NodeId::NIL && view.normal(k.leader) => {
                    best.cloned().or_else(|| {
                        (gossip && k.mode == CounterMode::Raft).then(|| GroupHint {
                            leader: NodeId::NIL,
                            ..k.clone()
                        })
                    })
                }
                _ => None,
            },
        };
        if let Some(next) = next.filter(|n| inner.known.as_ref() != Some(n)) {
            inner.known = Some(next);
            drop(inner);
            self.changed.notify_waiters();
        }
    }

    /// The hint of this node for the group, when it leads it.
    pub fn own_hint(&self, me: NodeId) -> Option<GroupHint> {
        let inner = self.inner.lock();
        match &inner.role {
            // A leader that removed itself is no voter, and it must not claim the group.
            Role::Raft(r) if r.leads() => r.hint(self.g),
            Role::Leader(l) if l.serving() => Some(GroupHint {
                group: self.g,
                mode: CounterMode::LeaderFollower,
                regime: l.regime,
                leader: me,
                follower: l.follower(),
                ..GroupHint::default()
            }),
            _ => None,
        }
    }

    /// The leader that the epoch names (spec section 10.1): the previous leader, else the
    /// previous follower, else the top member by HRW.
    fn designated(&self, view: &View) -> Option<NodeId> {
        let inner = self.inner.lock();
        if let Some(k) = &inner.known {
            if view.normal(k.leader) {
                return Some(k.leader);
            }
            if let Some(f) = k.follower.filter(|f| view.normal(*f)) {
                return Some(f);
            }
        }
        view.ranked(self.g).first().copied()
    }

    /// What this node answers to `RAFT_LEADER`: its own hint, or the Raft leader it follows.
    pub fn answer_hint(&self, me: NodeId) -> Option<GroupHint> {
        if let Some(h) = self.own_hint(me) {
            return Some(h);
        }
        match self.role() {
            Role::Raft(r) => Some(raft_answer(self.g, r.regime(), r.hint(self.g))),
            _ => None,
        }
    }

    /// Whether this node holds what group `g` needs from it (spec section 15.2). `None` while
    /// it does not know the group yet.
    pub fn settled_for(&self, view: &View) -> Option<bool> {
        let inner = self.inner.lock();
        let known = inner.known.as_ref()?;
        let raft = view.normal_count() >= 3 && known.mode == CounterMode::Raft;
        let target = view
            .ranked(self.g)
            .into_iter()
            .take(3)
            .any(|n| n == view.me);
        Some(match &inner.role {
            Role::Leader(_) => true,
            Role::Raft(r) => !raft || !target || r.is_voter(),
            Role::Follower(f) => (raft && !target) || f.synced(),
            Role::Idle if raft => !target,
            Role::Idle => known.follower != Some(view.me),
        })
    }

    /// The group runs in Raft mode as far as this node knows.
    fn raft_known(&self) -> bool {
        self.inner
            .lock()
            .known
            .as_ref()
            .is_some_and(|k| k.mode == CounterMode::Raft)
    }

    /// One controller step.
    pub async fn step(&self, counters: &Arc<Counters>, view: &View) {
        let seen = self.version();
        let role = self.role();
        if !view.me_normal {
            // A `LEAVING` voter stays until the leader moves its slot away (spec section 15.3).
            match role {
                Role::Raft(r) if view.leaving => self.raft_step(counters, view, seen, &r).await,
                Role::Idle => {}
                _ => self.set_role(Role::Idle),
            }
            return;
        }
        match role {
            Role::Leader(l) => self.lead(counters, view, seen, &l).await,
            Role::Raft(r) => self.raft_step(counters, view, seen, &r).await,
            role => self.wait_or_take(counters, view, seen, role).await,
        }
    }

    async fn lead(&self, counters: &Arc<Counters>, view: &View, seen: u64, l: &Arc<LfLeader>) {
        let settings = &counters.settings;
        if l.fenced() {
            let due = self
                .inner
                .lock()
                .tried
                .is_none_or(|t| t.elapsed() >= settings.lease_renew);
            if due {
                match self.acquire(counters).await {
                    Some(Ok(until)) => l.renewed(until),
                    Some(Err(())) => {
                        tracing::info!(group = self.g, "lost the counter lease");
                        self.step_down(counters, seen, l);
                        return;
                    }
                    None => {}
                }
            }
            if l.expired() {
                tracing::warn!(group = self.g, "the counter lease expired");
                self.step_down(counters, seen, l);
                return;
            }
        } else if self.designated(view) != Some(view.me) {
            self.step_down(counters, seen, l);
            return;
        }
        if counters.raft_ready(view) {
            // The up switch: the lease holder becomes the only voter of a new Raft group with its
            // state, and lets the lease expire (spec section 10.3).
            l.stop();
            if !self
                .create_instance(counters, view, seen, l.state(), "the up switch")
                .await
            {
                self.step_down(counters, seen, l);
            }
            return;
        }
        let current = l.follower().filter(|f| view.normal(*f));
        let follower = current.or_else(|| view.ranked(self.g).into_iter().find(|n| *n != view.me));
        l.set_follower(follower);
        let hint = self.own_hint(view.me);
        let mut inner = self.inner.lock();
        inner.leaderless = None;
        if hint.is_some() {
            inner.known = hint;
        }
    }

    fn step_down(&self, counters: &Arc<Counters>, seen: u64, l: &Arc<LfLeader>) {
        let follower = Follower::new(l.regime, None, l.state(), counters.budget());
        self.set_role_if(seen, Role::Follower(Arc::new(follower)));
    }

    async fn wait_or_take(&self, counters: &Arc<Counters>, view: &View, seen: u64, role: Role) {
        let settings = &counters.settings;
        // A follower of an older regime holds nothing that a newer leader needs.
        if let Role::Follower(f) = &role {
            let newer = self.inner.lock().known.as_ref().map(|k| k.regime);
            if newer.is_some_and(|r| r > f.regime) {
                self.set_role_if(seen, Role::Idle);
                return;
            }
        }
        if view.normal_count() >= 3 && self.raft_known() {
            self.rebuild_if_lost(counters, view, seen).await;
            return;
        }
        let live = self.leader(view.me, &view.epoch);
        let leaderless = {
            let mut inner = self.inner.lock();
            if live.is_some() {
                inner.leaderless = None;
            } else if inner.leaderless.is_none() {
                inner.leaderless = Some(Instant::now());
            }
            inner.leaderless.map_or(Duration::ZERO, |s| s.elapsed())
        };
        let designated = self.designated(view) == Some(view.me);
        let follower = match &role {
            Role::Follower(f) if f.leader.is_some() => Some(f),
            _ => None,
        };
        let candidate = if self.lease.is_none() {
            designated && live.is_none()
        } else if let Some(f) = follower {
            f.silence() >= settings.lease_duration
        } else if live.is_some() {
            false
        } else {
            designated || leaderless >= 2 * settings.lease_duration
        };
        if !candidate {
            return;
        }
        // Ask first, so a node never claims a group that a member it has no hint from leads,
        // for example a Raft leader right after the up switch.
        if let Some(best) = counters.ask_now(self.g).await {
            if role.regime().is_none_or(|r| best.regime > r) {
                return;
            }
        }
        let lease_until = if self.lease.is_some() {
            match self.acquire(counters).await {
                Some(Ok(until)) => Some(until),
                _ => return,
            }
        } else {
            None
        };
        let state = role.state().unwrap_or_default();
        let previous = self.inner.lock().known.as_ref().map(|k| k.leader);
        let Some(leader) =
            self.become_leader(counters, seen, state, lease_until, "leads a counter group")
        else {
            return;
        };
        if previous.is_some_and(|p| p != view.me) {
            discus_telemetry::global().counter_failover();
        }
        self.start_leader(counters, view, leader).await;
    }

    /// Lead a new regime in leader-follower mode from `state`. `None` when the role changed
    /// after `seen`. The caller starts the sender with [`Group::start_leader`].
    pub(super) fn become_leader(
        &self,
        counters: &Arc<Counters>,
        seen: u64,
        state: CounterState,
        lease_until: Option<Instant>,
        why: &str,
    ) -> Option<Arc<LfLeader>> {
        let regime = self.next_regime(counters.id);
        let leader = LfLeader::new(
            regime,
            state,
            counters.budget(),
            lease_until,
            counters.settings.lease_margin,
        );
        self.set_role_if(seen, Role::Leader(leader.clone()))
            .then(|| {
                tracing::info!(group = self.g, %regime, "{why}");
                leader
            })
    }

    /// Start the sender of a new leader-follower leader and run its first step.
    pub(super) async fn start_leader(
        &self,
        counters: &Arc<Counters>,
        view: &View,
        leader: Arc<LfLeader>,
    ) {
        tokio::spawn(leader.clone().sender(
            counters.clone(),
            self.g,
            counters.settings.lease_renew,
        ));
        self.lead(counters, view, self.version(), &leader).await;
    }

    /// One lease call: `Some(Ok(until))` when this node holds the lease, `Some(Err(()))` when
    /// another node holds it, and `None` when the call failed. Calls are at most one per renew
    /// interval.
    pub(super) async fn acquire(&self, counters: &Arc<Counters>) -> Option<Result<Instant, ()>> {
        let lease = self.lease.as_ref()?;
        {
            let mut inner = self.inner.lock();
            if inner
                .tried
                .is_some_and(|t| t.elapsed() < counters.settings.lease_renew / 2)
            {
                return None;
            }
            inner.tried = Some(Instant::now());
        }
        let holder: LeaseHolder = counters.lease_holder();
        let start = Instant::now();
        match lease.acquire_or_renew(&holder).await {
            Ok(LeaseState::Held(h)) if h.identity == holder.identity => {
                Some(Ok(start + counters.settings.lease_duration))
            }
            Ok(_) => Some(Err(())),
            Err(e) => {
                tracing::debug!(group = self.g, error = %e, "a counter lease call failed");
                None
            }
        }
    }

    /// Answer a `COUNTER_SYNC` from `from`.
    pub fn on_sync(
        &self,
        counters: &Arc<Counters>,
        view: &View,
        from: NodeId,
        s: &CounterSync,
    ) -> CounterAck {
        let refuse = |regime: Regime| CounterAck {
            group: s.group,
            regime,
            seq: 0,
            need_snapshot: true,
        };
        if !view.me_normal {
            return refuse(Regime::default());
        }
        let seen = self.version();
        let role = self.role();
        let follower = match &role {
            Role::Follower(f) if f.regime == s.regime => f.clone(),
            Role::Leader(l) if l.regime >= s.regime => return refuse(l.regime),
            Role::Raft(r) if r.regime() >= s.regime => return refuse(r.regime()),
            Role::Follower(f) if f.regime > s.regime => return refuse(f.regime),
            other => {
                let state = other.state().unwrap_or_default();
                let f = Arc::new(Follower::new(
                    s.regime,
                    Some(from),
                    state,
                    counters.budget(),
                ));
                if !self.set_role_if(seen, Role::Follower(f.clone())) {
                    return refuse(Regime::default());
                }
                f
            }
        };
        let hint = GroupHint {
            group: self.g,
            mode: CounterMode::LeaderFollower,
            regime: s.regime,
            leader: from,
            follower: Some(view.me),
            ..GroupHint::default()
        };
        {
            let mut inner = self.inner.lock();
            inner.max_generation = inner.max_generation.max(s.regime.generation);
            if inner.known.as_ref().is_none_or(|k| !newer(k, &hint)) {
                inner.known = Some(hint);
            }
            inner.leaderless = None;
        }
        follower.on_sync(s, &counters.settings.limits)
    }
}

/// The answer to `RAFT_LEADER` of a Raft replica that does not lead, from `known`, the hint of
/// the leader that its replica knows. It says that this node holds the group, so a rebuild never
/// skips a live voter. Only [`Group::own_hint`] names a leader: a named hint of a term beats the
/// nil hint of that term (see [`newer`]), so the answer of a lagging replica could bring back a
/// leader that left.
pub(super) fn raft_answer(g: u16, regime: Regime, known: Option<GroupHint>) -> GroupHint {
    let hint = known.unwrap_or(GroupHint {
        group: g,
        mode: CounterMode::Raft,
        regime,
        ..GroupHint::default()
    });
    GroupHint {
        leader: NodeId::NIL,
        ..hint
    }
}

/// The best hint of a round of `RAFT_LEADER` answers for group `g`, and the peers that answered
/// without claiming the group. A hint whose leader answered in the same round without a claim is
/// dropped: that leader knows best, and a replica that names it can lag behind.
pub(super) fn pick(
    g: u16,
    view: &View,
    answers: Vec<(NodeId, Vec<GroupHint>)>,
) -> (Option<GroupHint>, Vec<NodeId>) {
    let mut hints = Vec::new();
    let mut denied = Vec::new();
    for (peer, answer) in answers {
        let answer: Vec<GroupHint> = answer.into_iter().filter(|h| h.group == g).collect();
        if !answer.iter().any(|h| h.leader == peer) {
            denied.push(peer);
        }
        hints.extend(answer.into_iter().filter(|h| view.normal(h.leader)));
    }
    let best = hints
        .into_iter()
        .filter(|h| !denied.contains(&h.leader))
        .reduce(|a, b| if newer(&b, &a) { b } else { a });
    (best, denied)
}

fn stop(role: &Role) {
    match role {
        Role::Raft(r) => {
            let replica = r.replica.clone();
            tokio::spawn(async move { replica.shutdown().await });
        }
        Role::Leader(l) => l.stop(),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use discus_placement::{ClusterId, EpochId, Member, MemberState, Weight};

    fn view(ids: &[NodeId]) -> View {
        let members = ids
            .iter()
            .map(|id| Member {
                id: *id,
                addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
                rpc_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
                client_addr: None,
                client_host: None,
                state: MemberState::Normal,
                weight: Weight::default(),
            })
            .collect();
        let id = EpochId {
            generation: 1,
            origin: ids[0],
        };
        View {
            epoch: Arc::new(Epoch::new(id, ClusterId::NIL, members).unwrap()),
            me: ids[0],
            me_normal: true,
            leaving: false,
        }
    }

    #[test]
    fn group_voters_follow_hrw_by_group() {
        let ids: Vec<NodeId> = (1..=5).map(NodeId::from_u128).collect();
        let v = view(&ids);
        for g in 0..64 {
            assert_eq!(
                v.ranked(g),
                discus_placement::hrw::owners(
                    &super::super::group_key(g),
                    v.epoch.members(),
                    usize::MAX
                )
            );
        }
    }

    fn raft_hint(leader: NodeId, term: u64, voters: &[NodeId]) -> GroupHint {
        GroupHint {
            mode: CounterMode::Raft,
            regime: Regime {
                generation: 2,
                origin: NodeId::from_u128(1),
            },
            leader,
            term,
            voters: voters.to_vec(),
            ..GroupHint::default()
        }
    }

    /// An answer to `RAFT_LEADER` names the new leader before its gossip does. The next merge
    /// sees no claim and drops the leader. Once the claim arrives, it names the leader again.
    #[test]
    fn a_leader_hint_replaces_a_leaderless_one_of_its_term() {
        let ids: Vec<NodeId> = (1..=4).map(NodeId::from_u128).collect();
        let v = view(&ids);
        let group = Group::new(0, None);
        let elected = raft_hint(ids[1], 2, &ids[1..]);
        group.learn(Some(elected.clone()), &[ids[2]]);
        assert_eq!(group.leader(v.me, &v.epoch), Some(ids[1]));
        group.observe(&v, &[], true);
        assert_eq!(group.leader(v.me, &v.epoch), None);

        group.observe(&v, std::slice::from_ref(&elected), true);
        assert_eq!(
            group.leader(v.me, &v.epoch),
            Some(ids[1]),
            "the gossip claim"
        );

        group.observe(&v, &[], true);
        group.learn(Some(elected), &[ids[2]]);
        assert_eq!(group.leader(v.me, &v.epoch), Some(ids[1]), "the answer");
    }

    #[test]
    fn a_replica_that_does_not_lead_names_no_leader() {
        let ids: Vec<NodeId> = (1..=4).map(NodeId::from_u128).collect();
        let known = raft_hint(ids[1], 2, &ids[1..]);
        let answer = raft_answer(0, known.regime, Some(known.clone()));
        assert_eq!(answer.leader, NodeId::NIL);
        assert_eq!(answer.voters, known.voters);
        assert_eq!(answer.mode, CounterMode::Raft);
        let unknown = raft_answer(0, known.regime, None);
        assert_eq!(unknown.leader, NodeId::NIL);
        assert_eq!(unknown.regime, known.regime);
    }

    /// The answer of a new leader still names it before its gossip claim arrives.
    #[test]
    fn an_answer_from_the_leader_replaces_a_leaderless_hint() {
        let ids: Vec<NodeId> = (1..=4).map(NodeId::from_u128).collect();
        let v = view(&ids);
        let group = Group::new(0, None);
        let elected = raft_hint(ids[1], 2, &ids[1..]);
        group.learn(Some(elected.clone()), &[]);
        group.observe(&v, &[], true);
        assert_eq!(group.leader(v.me, &v.epoch), None);

        let regime = elected.regime;
        let answers = vec![
            (ids[1], vec![elected.clone()]),
            (ids[2], vec![raft_answer(0, regime, Some(elected))]),
        ];
        let (best, denied) = pick(0, &v, answers);
        group.learn(best, &denied);
        assert_eq!(group.leader(v.me, &v.epoch), Some(ids[1]));
    }
}
