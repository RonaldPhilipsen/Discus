//! The controller of one counter group: which role this node has in it (spec sections 10.1 to
//! 10.3).

use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_placement::NodeId;
use discus_wire::{CounterMode, GroupHint, Regime};
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::lf::LfLeader;
use super::state::CounterState;
use super::{Counters, View};

/// The role of this node in one group.
#[derive(Clone)]
pub enum Role {
    Idle,
    Leader(Arc<LfLeader>),
}

impl Role {
    pub fn name(&self) -> &'static str {
        match self {
            Role::Idle => "idle",
            Role::Leader(_) => "leader",
        }
    }
}

pub struct Inner {
    pub role: Role,
    /// The best hint this node knows. It stays after its leader dies, so the next leader can
    /// follow the epoch rule.
    pub known: Option<GroupHint>,
    /// The largest regime generation seen. A new regime takes the next one.
    pub max_generation: u64,
    /// A leader that refused a command recently. Routing skips it until the time passes.
    pub distrusted: Option<(NodeId, Instant)>,
    /// The last `RAFT_LEADER` round for this group.
    pub asked: Option<Instant>,
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
    /// Woken when the role or the known leader changes, so waiting commands look again.
    pub changed: Notify,
}

impl Group {
    pub fn new(g: u16) -> Group {
        Group {
            g,
            inner: Mutex::new(Inner {
                role: Role::Idle,
                known: None,
                max_generation: 0,
                distrusted: None,
                asked: None,
            }),
            changed: Notify::new(),
        }
    }

    pub fn role(&self) -> Role {
        self.inner.lock().role.clone()
    }

    fn set_role(&self, role: Role) {
        let old = std::mem::replace(&mut self.inner.lock().role, role);
        if let Role::Leader(l) = old {
            l.stop();
        }
        self.changed.notify_waiters();
    }

    /// Drop every role and every state of this group.
    pub fn clear(&self) {
        self.set_role(Role::Idle);
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
        inner
            .known
            .as_ref()
            .map(|k| k.leader)
            .filter(|l| *l != view.me && view.normal(*l) && Some(*l) != distrusted)
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
                follower: None,
                voters: Vec::new(),
                learners: Vec::new(),
            }),
            _ => None,
        }
    }

    /// The leader that the epoch names when no lease fences the group (spec section 10.1): the
    /// previous leader, else the previous follower, else the top member by HRW.
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
        let designated = self.designated(view);
        match self.role() {
            Role::Leader(l) => {
                if designated != Some(view.me) {
                    self.set_role(Role::Idle);
                } else {
                    self.publish(view, &l);
                }
            }
            Role::Idle => {
                if designated == Some(view.me) {
                    let regime = self.next_regime(view.me);
                    let leader = LfLeader::new(regime, CounterState::new(), counters.budget());
                    self.publish(view, &leader);
                    self.set_role(Role::Leader(leader));
                }
            }
        }
    }

    fn publish(&self, view: &View, l: &LfLeader) {
        let hint = GroupHint {
            group: self.g,
            mode: CounterMode::LeaderFollower,
            regime: l.regime,
            leader: view.me,
            term: 0,
            follower: None,
            voters: Vec::new(),
            learners: Vec::new(),
        };
        let mut inner = self.inner.lock();
        inner.max_generation = inner.max_generation.max(l.regime.generation);
        inner.known = Some(hint);
    }
}
