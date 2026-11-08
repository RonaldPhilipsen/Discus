//! The controller of one counter group: which role this node has in it (spec sections 10.1 to
//! 10.3).
//!
//! Each step reads the epoch, the hints, and the lease, and moves the role one transition at a
//! time. Inbound syncs change the role too. A transition that waits (a lease call) checks the
//! role version afterwards and gives up when the role changed meanwhile.

use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_membership::{LeaseHolder, LeaseProvider, LeaseState};
use discus_placement::NodeId;
use discus_wire::{CounterAck, CounterMode, CounterSync, GroupHint, Regime};
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::lf::{Follower, LfLeader};
use super::state::CounterState;
use super::{Counters, View};

/// The role of this node in one group.
#[derive(Clone)]
pub enum Role {
    Idle,
    Follower(Arc<Follower>),
    Leader(Arc<LfLeader>),
}

impl Role {
    pub fn name(&self) -> &'static str {
        match self {
            Role::Idle => "idle",
            Role::Follower(_) => "follower",
            Role::Leader(_) => "leader",
        }
    }

    fn regime(&self) -> Option<Regime> {
        match self {
            Role::Idle => None,
            Role::Follower(f) => Some(f.regime),
            Role::Leader(l) => Some(l.regime),
        }
    }

    /// The state that this role holds, if any. A new leader starts from it.
    fn state(&self) -> Option<CounterState> {
        match self {
            Role::Idle => None,
            Role::Follower(f) => Some(f.state()),
            Role::Leader(l) => Some(l.state()),
        }
    }
}

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

/// Order two hints: the newer regime, then the higher Raft term.
pub fn newer(a: &GroupHint, b: &GroupHint) -> bool {
    (a.regime, a.term) > (b.regime, b.term)
}

pub struct Group {
    pub g: u16,
    inner: Mutex<Inner>,
    lease: Option<Arc<dyn LeaseProvider>>,
    /// Woken when the role or the known leader changes, so waiting commands look again.
    pub changed: Notify,
}

impl Group {
    pub fn new(g: u16, lease: Option<Arc<dyn LeaseProvider>>) -> Group {
        Group {
            g,
            inner: Mutex::new(Inner {
                role: Role::Idle,
                version: 0,
                known: None,
                max_generation: 0,
                distrusted: None,
                asked: None,
                leaderless: None,
                tried: None,
                first_role: None,
            }),
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

    fn version(&self) -> u64 {
        self.inner.lock().version
    }

    /// Install `role` unless the role changed after `seen`.
    fn set_role_if(&self, seen: u64, role: Role) -> bool {
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

    fn next_regime(&self, me: NodeId) -> Regime {
        let mut inner = self.inner.lock();
        inner.max_generation += 1;
        Regime {
            generation: inner.max_generation,
            origin: me,
        }
    }

    /// The node that leads this group, as far as this node knows.
    pub fn leader(&self, view: &View) -> Option<NodeId> {
        let inner = self.inner.lock();
        match &inner.role {
            Role::Leader(l) if l.serving() => return Some(view.me),
            _ => {}
        }
        let distrusted = inner
            .distrusted
            .filter(|(_, at)| at.elapsed() < DISTRUST)
            .map(|(n, _)| n);
        let usable = |l: &NodeId| *l != view.me && view.normal(*l) && Some(*l) != distrusted;
        if let Role::Follower(f) = &inner.role {
            if let Some(l) = f.leader.filter(usable) {
                return Some(l);
            }
        }
        inner.known.as_ref().map(|k| k.leader).filter(usable)
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

    /// Take the best hint of a fresh round of answers. It replaces what this node knew, also
    /// when it is older, because the answers describe the cluster now.
    pub fn learn(&self, best: Option<GroupHint>) {
        let Some(best) = best else {
            return;
        };
        let mut inner = self.inner.lock();
        inner.max_generation = inner.max_generation.max(best.regime.generation);
        inner.distrusted = None;
        inner.known = Some(best);
        drop(inner);
        self.changed.notify_waiters();
    }

    /// Merge the current hints of the `NORMAL` members. The best one replaces the known hint
    /// when it is newer, or when no current hint backs the known one any more.
    pub fn observe(&self, hints: &[GroupHint]) {
        let best =
            hints
                .iter()
                .filter(|h| h.group == self.g)
                .reduce(|a, b| if newer(b, a) { b } else { a });
        let Some(best) = best else {
            return;
        };
        let mut inner = self.inner.lock();
        inner.max_generation = inner.max_generation.max(best.regime.generation);
        let replace = match &inner.known {
            None => true,
            Some(k) => newer(best, k) || !hints.iter().any(|h| h.group == self.g && h == k),
        };
        if replace && inner.known.as_ref() != Some(best) {
            inner.known = Some(best.clone());
            drop(inner);
            self.changed.notify_waiters();
        }
    }

    /// The hint of this node for the group, when it leads it.
    pub fn own_hint(&self, me: NodeId) -> Option<GroupHint> {
        let inner = self.inner.lock();
        match &inner.role {
            Role::Leader(l) if l.serving() => Some(GroupHint {
                group: self.g,
                mode: CounterMode::LeaderFollower,
                regime: l.regime,
                leader: me,
                term: 0,
                follower: l.follower(),
                voters: Vec::new(),
                learners: Vec::new(),
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

    /// One controller step.
    pub async fn step(&self, counters: &Arc<Counters>, view: &View) {
        if !view.me_normal {
            if !matches!(self.role(), Role::Idle) {
                self.set_role(Role::Idle);
            }
            return;
        }
        let seen = self.version();
        match self.role() {
            Role::Leader(l) => self.lead(counters, view, seen, &l).await,
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
        let current = l.follower().filter(|f| view.normal(*f));
        let follower = current.or_else(|| view.ranked(self.g).into_iter().find(|n| *n != view.me));
        l.set_follower(follower);
        let hint = self.own_hint(view.me);
        let mut inner = self.inner.lock();
        inner.leaderless = None;
        if let Some(h) = hint {
            if inner.known.as_ref() != Some(&h) {
                inner.known = Some(h);
            }
        }
    }

    fn step_down(&self, counters: &Arc<Counters>, seen: u64, l: &Arc<LfLeader>) {
        let follower = Follower::new(l.regime, None, l.state(), counters.budget());
        self.set_role_if(seen, Role::Follower(Arc::new(follower)));
    }

    async fn wait_or_take(&self, counters: &Arc<Counters>, view: &View, seen: u64, role: Role) {
        let settings = &counters.settings;
        let live = self.leader(view);
        let leaderless = {
            let mut inner = self.inner.lock();
            if live.is_some() {
                inner.leaderless = None;
            }
            match inner.leaderless {
                Some(since) => since.elapsed(),
                None if live.is_none() => {
                    inner.leaderless = Some(Instant::now());
                    Duration::ZERO
                }
                None => Duration::ZERO,
            }
        };
        let designated = self.designated(view) == Some(view.me);
        let follower = match &role {
            Role::Follower(f) => f.leader.is_some(),
            _ => false,
        };
        let candidate = if self.lease.is_none() {
            designated && live.is_none()
        } else if follower {
            matches!(&role, Role::Follower(f) if f.silence() >= settings.lease_duration)
        } else if live.is_some() {
            false
        } else {
            designated || leaderless >= 2 * settings.lease_duration
        };
        if !candidate {
            return;
        }
        // Ask first, so a node never claims a group that a member it has no hint from leads.
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
        let regime = self.next_regime(view.me);
        let leader = LfLeader::new(
            regime,
            state,
            counters.budget(),
            lease_until,
            settings.lease_margin,
        );
        if self.set_role_if(seen, Role::Leader(leader.clone())) {
            tracing::info!(group = self.g, %regime, "leads a counter group");
            tokio::spawn(
                leader
                    .clone()
                    .sender(counters.clone(), self.g, settings.lease_renew),
            );
            self.lead(counters, view, self.version(), &leader).await;
        }
    }

    /// One lease call: `Some(Ok(until))` when this node holds the lease, `Some(Err(()))` when
    /// another node holds it, and `None` when the call failed. Calls are at most one per renew
    /// interval.
    async fn acquire(&self, counters: &Arc<Counters>) -> Option<Result<Instant, ()>> {
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
            term: 0,
            follower: Some(view.me),
            voters: Vec::new(),
            learners: Vec::new(),
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

fn stop(role: &Role) {
    if let Role::Leader(l) = role {
        l.stop();
    }
}
