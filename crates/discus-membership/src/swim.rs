//! The sans-IO SWIM core (spec sections 5.1, 6.1 to 6.3).
//!
//! The caller supplies the time and the inbound messages, and sends the actions that the core
//! returns. Each action is one request with one reply. The caller reports the result with
//! [`Swim::outcome`], or with [`Swim::relayed`] for the ping that a `PING_REQ` asked for.
//!
//! Epoch changes are pending deltas, not a recomputed view: each observed transition of a member
//! records the presence that the member needs in the epoch. When the jitter timer fires, the node
//! drops the deltas that the current epoch already shows and creates the next epoch from the
//! current members with the rest applied. So a node never undoes a change that it has not seen,
//! and a losing creator applies its change again in the next epoch.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use discus_placement::{ClusterId, Epoch, EpochId, Member, MemberState, NodeId};
use discus_wire::{Capacity, Gossip, GossipKind, Liveness, MemberInfo, Rumor};

use crate::lease::{LeaseError, LeaseState};
use crate::merge::{winner, JoinLimiter};
use crate::view::{retransmit_limit, supersedes, Broadcasts, Record};
use crate::{Detector, Event, Host, Millis, SwimConfig};

/// A join reply takes the highest HLC heard from members within this window into account.
const HLC_WINDOW_MS: Millis = 30_000;

/// Join requests per period while seeking.
const JOIN_FANOUT: usize = 3;

/// Members that receive the final `LEAVE`.
const LEAVE_FANOUT: usize = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct Action {
    pub token: u64,
    pub to: SocketAddr,
    /// The node that the core expects at `to`, if it knows one.
    pub expected: Option<NodeId>,
    pub msg: Gossip,
    pub timeout_ms: Millis,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Reply(Box<Gossip>),
    /// The connection worked but no reply came in time. `in_cluster` is true when the peer
    /// reported an epoch generation of 1 or more in `HELLO`.
    Silent {
        in_cluster: bool,
    },
    Unreachable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Handled {
    pub reply: Gossip,
    /// A ping to send before the reply. Its result goes to [`Swim::relayed`], which gives the
    /// final reply.
    pub relay: Option<Action>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Looking for a cluster, or joining one again after a rejoin. The node is `JOINING`.
    Seeking,
    Member,
    Leaving,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    Direct { target: NodeId },
    Indirect { target: NodeId },
    Relay { target: NodeId },
    Sync,
    Join,
    Leave,
}

#[derive(Debug, Clone, Copy)]
struct Probe {
    target: NodeId,
    acked: bool,
    indirect_left: usize,
}

pub struct Swim {
    cfg: SwimConfig,
    me: MemberInfo,
    cluster_id: ClusterId,
    phase: Phase,
    members: BTreeMap<NodeId, Record>,
    broadcasts: Broadcasts,
    rng: fastrand::Rng,
    order: Vec<NodeId>,
    next_probe: Millis,
    probe: Option<Probe>,
    pending: HashMap<u64, Pending>,
    next_token: u64,
    next_sync: Millis,
    next_reconnect: Millis,
    events: Vec<Event>,
    seeds: Vec<SocketAddr>,
    seek_started: Option<Millis>,
    last_normal_seen: Option<Millis>,
    next_join: Millis,
    join_cursor: usize,
    next_lease: Millis,
    /// The presence each member needs in the next epoch: `Some` member (`NORMAL` or `LEAVING`)
    /// or `None` (absent).
    deltas: BTreeMap<NodeId, Option<Member>>,
    epoch_timer: Option<Millis>,
    /// The epoch that the core processed last. It notices epochs adopted elsewhere, for example
    /// on the forward path.
    last_epoch: Option<Arc<Epoch>>,
    limiter: JoinLimiter,
    /// The cluster size that the detectors expect. They are rebuilt when the size doubles or
    /// halves.
    detector_n: usize,
}

impl Swim {
    /// A node that is not in a cluster yet. It starts seeking at its first tick.
    pub fn new(cfg: SwimConfig, me: MemberInfo) -> Swim {
        let rng = fastrand::Rng::with_seed(cfg.seed);
        Swim {
            cfg,
            me,
            cluster_id: ClusterId::NIL,
            phase: Phase::Seeking,
            members: BTreeMap::new(),
            broadcasts: Broadcasts::default(),
            rng,
            order: Vec::new(),
            next_probe: 0,
            probe: None,
            pending: HashMap::new(),
            next_token: 1,
            next_sync: 0,
            next_reconnect: 0,
            events: Vec::new(),
            seeds: Vec::new(),
            seek_started: None,
            last_normal_seen: None,
            next_join: 0,
            join_cursor: 0,
            next_lease: 0,
            deltas: BTreeMap::new(),
            epoch_timer: None,
            last_epoch: None,
            limiter: JoinLimiter::default(),
            detector_n: 1,
        }
    }

    pub fn config(&self) -> &SwimConfig {
        &self.cfg
    }

    pub fn me(&self) -> &MemberInfo {
        &self.me
    }

    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Make this node a member of `cluster_id` in `state` without a join. The simulator and the
    /// static membership mode use it.
    pub fn set_static_member(&mut self, cluster_id: ClusterId, state: MemberState) {
        self.cluster_id = cluster_id;
        self.me.state = state;
        self.phase = Phase::Member;
    }

    pub fn members(&self) -> Vec<(MemberInfo, Liveness)> {
        self.members
            .values()
            .map(|r| (r.info.clone(), r.liveness))
            .collect()
    }

    pub fn liveness(&self, id: NodeId) -> Option<Liveness> {
        self.members.get(&id).map(|r| r.liveness)
    }

    pub fn member(&self, id: NodeId) -> Option<&MemberInfo> {
        self.members.get(&id).map(|r| &r.info)
    }

    pub fn score(&self, now: Millis, id: NodeId) -> f64 {
        self.members
            .get(&id)
            .map(|r| r.detector.score(now))
            .unwrap_or(0.0)
    }

    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    /// Discovery seeds: static seeds, headless Service addresses, and lease holders. The own
    /// address is dropped.
    pub fn set_seeds(&mut self, seeds: Vec<SocketAddr>) {
        self.seeds.clear();
        for s in seeds {
            self.add_seed(s);
        }
    }

    pub fn add_seed(&mut self, seed: SocketAddr) {
        if seed != self.me.addr && !self.seeds.contains(&seed) {
            self.seeds.push(seed);
        }
    }

    /// True when the node serves: it is `NORMAL`, and the current epoch lists it as `NORMAL`.
    pub fn is_normal(&self, host: &dyn Host) -> bool {
        self.phase == Phase::Member
            && self.me.state == MemberState::Normal
            && host.epoch().member(self.me.id).map(|m| m.state) == Some(MemberState::Normal)
    }

    /// Counter leader hints spread with a new incarnation.
    pub fn set_leader_hints(&mut self, hints: Bytes) {
        self.me.leader_hints = hints;
        self.bump();
    }

    /// Capacity hints travel in the sender part of every message, without a new incarnation.
    pub fn set_capacity(&mut self, capacity: Capacity) {
        self.me.capacity = capacity;
    }

    fn bump(&mut self) {
        self.me.incarnation += 1;
        self.broadcasts.push(self.me.id);
    }

    fn set_state(&mut self, now: Millis, state: MemberState, phase: Phase) {
        self.me.state = state;
        self.phase = phase;
        self.bump();
        self.events.push(Event::StateChanged {
            state,
            incarnation: self.me.incarnation,
        });
        if phase == Phase::Seeking {
            self.seek_started = Some(now);
            self.next_join = now;
        }
        self.on_change(now, self.me.id);
    }

    /// Data-path evidence against `peer`: a forward timeout, a reset, or a failed handshake.
    pub fn evidence(&mut self, now: Millis, peer: NodeId) {
        let weight = self.cfg.data_path_weight;
        if let Some(r) = self.members.get_mut(&peer) {
            r.detector.evidence(now, weight);
        }
    }

    fn token(&mut self, pending: Pending) -> u64 {
        let t = self.next_token;
        self.next_token += 1;
        self.pending.insert(t, pending);
        t
    }

    fn alive_count(&self) -> usize {
        self.members
            .values()
            .filter(|r| r.liveness != Liveness::Dead)
            .count()
    }

    fn rumor_of(&self, id: NodeId) -> Option<Rumor> {
        if id == self.me.id {
            return Some(Rumor {
                info: self.me.clone(),
                liveness: Liveness::Alive,
                accuser: None,
            });
        }
        self.members.get(&id).map(|r| Rumor {
            info: r.info.clone(),
            liveness: r.liveness,
            accuser: match r.liveness {
                Liveness::Suspect if r.accusers.contains(&self.me.id) => Some(self.me.id),
                Liveness::Suspect => r.accusers.iter().next().copied(),
                _ => None,
            },
        })
    }

    fn full_view(&self) -> Vec<Rumor> {
        std::iter::once(self.me.id)
            .chain(self.members.keys().copied())
            .filter_map(|id| self.rumor_of(id))
            .collect()
    }

    /// A message with only the sender part: for seekers and for foreign clusters, which never
    /// receive this node's view.
    fn bare(&mut self, kind: GossipKind, host: &dyn Host) -> Gossip {
        let epoch = host.epoch();
        self.me.epoch = epoch.id();
        Gossip {
            kind,
            from: self.me.clone(),
            cluster_id: self.cluster_id,
            cluster_size: normal_count(&epoch) as u32,
            epoch_body: None,
            hlc: host.hlc(),
            target: None,
            rumors: Vec::new(),
            retry_after_ms: 0,
        }
    }

    fn message(&mut self, kind: GossipKind, to: Option<NodeId>, host: &dyn Host) -> Gossip {
        let mut g = self.bare(kind, host);
        let epoch = host.epoch();
        let full = matches!(
            kind,
            GossipKind::Sync | GossipKind::SyncReply | GossipKind::Join | GossipKind::JoinReply
        );
        g.rumors = if full {
            self.full_view()
        } else {
            let limit = retransmit_limit(self.cfg.retransmit_mult, self.alive_count() + 1);
            let ids = self.broadcasts.take(self.cfg.max_rumors, limit);
            ids.into_iter().filter_map(|id| self.rumor_of(id)).collect()
        };
        let stale = to
            .and_then(|t| self.members.get(&t))
            .is_some_and(|r| r.info.epoch < epoch.id());
        let in_cluster = epoch.id().generation >= 1 && self.cluster_id != ClusterId::NIL;
        if in_cluster && (full || stale) {
            g.epoch_body = Some((*epoch).clone());
        }
        g
    }

    /// Merge one rumor into the view. `direct` marks the sender part of a message, which also
    /// counts as a heartbeat.
    pub fn learn(&mut self, now: Millis, rumor: Rumor, direct: bool) {
        let id = rumor.info.id;
        if id == self.me.id {
            if rumor.liveness != Liveness::Alive && rumor.info.incarnation >= self.me.incarnation {
                // Refute. Only this node raises its incarnation. A rumor never changes the own
                // state: only an adopted epoch makes the node rejoin (spec section 6.3).
                self.me.incarnation = rumor.info.incarnation;
                self.bump();
            }
            return;
        }
        let new = (rumor.info.incarnation, rumor.liveness);
        let accuser = rumor
            .accuser
            .filter(|_| rumor.liveness == Liveness::Suspect);
        let mut changed = None;
        match self.members.get_mut(&id) {
            None => {
                let mut record = Record {
                    info: rumor.info.clone(),
                    liveness: rumor.liveness,
                    since: now,
                    accusers: BTreeSet::new(),
                    detector: Detector::new(&self.cfg, now, self.detector_n),
                    hlc: None,
                };
                record.accusers.extend(accuser);
                changed = Some((rumor.info.state, rumor.liveness));
                self.members.insert(id, record);
                self.broadcasts.push(id);
            }
            Some(r) => {
                let old = (r.info.incarnation, r.liveness);
                if supersedes(new, old) {
                    if r.liveness != rumor.liveness || r.info.state != rumor.info.state {
                        changed = Some((rumor.info.state, rumor.liveness));
                    }
                    if r.liveness != rumor.liveness {
                        r.since = now;
                    }
                    r.accusers.clear();
                    r.accusers.extend(accuser);
                    r.info = rumor.info;
                    r.liveness = rumor.liveness;
                    self.broadcasts.push(id);
                } else if new == old && rumor.liveness == Liveness::Suspect {
                    // Lifeguard: independent accusers shorten the suspicion timeout. Only the
                    // first confirmations are worth spreading.
                    if let Some(a) = accuser {
                        if r.accusers.insert(a) && r.accusers.len() <= self.cfg.suspicion_k + 1 {
                            self.broadcasts.push(id);
                        }
                    }
                } else if direct && new.0 == old.0 {
                    r.info.epoch = rumor.info.epoch;
                    r.info.capacity = rumor.info.capacity;
                    r.info.addr = rumor.info.addr;
                }
            }
        }
        if direct {
            if let Some(r) = self.members.get_mut(&id) {
                r.detector.heartbeat(now);
            }
        }
        if let Some((state, liveness)) = changed {
            self.events.push(Event::MemberChanged {
                id,
                state,
                liveness,
            });
            self.on_change(now, id);
        }
    }

    /// The presence that member `id` needs in the epoch, from the view.
    fn wanted(&self, id: NodeId) -> Option<Member> {
        let state = if id == self.me.id {
            let listed = self
                .last_epoch
                .as_ref()
                .and_then(|e| e.member(id))
                .is_some();
            match self.phase {
                Phase::Member if self.me.state == MemberState::Normal => MemberState::Normal,
                // A drain during a join never puts the node into an epoch.
                Phase::Leaving if listed => MemberState::Leaving,
                _ => return None,
            }
        } else {
            let r = self.members.get(&id)?;
            match (r.liveness, r.info.state) {
                (Liveness::Dead, _) => return None,
                (_, MemberState::Normal) => MemberState::Normal,
                (_, MemberState::Leaving) => MemberState::Leaving,
                _ => return None,
            }
        };
        let info = if id == self.me.id {
            &self.me
        } else {
            &self.members.get(&id)?.info
        };
        Some(Member {
            id,
            addr: info.addr,
            state,
            weight: info.weight,
        })
    }

    fn on_change(&mut self, now: Millis, id: NodeId) {
        let want = self.wanted(id);
        let shown = self
            .last_epoch
            .as_ref()
            .and_then(|e| e.member(id))
            .map(|m| m.state);
        if want.as_ref().map(|m| m.state) == shown {
            self.deltas.remove(&id);
        } else {
            self.deltas.insert(id, want);
            self.arm(now);
        }
    }

    fn arm(&mut self, now: Millis) {
        if self.epoch_timer.is_none() {
            let jitter = self.rng.u64(0..=self.cfg.epoch_jitter_ms);
            self.epoch_timer = Some(now + jitter);
        }
    }

    fn drop_shown_deltas(&mut self, epoch: &Epoch) {
        self.deltas.retain(|id, want| {
            want.as_ref().map(|m| m.state) != epoch.member(*id).map(|m| m.state)
        });
    }

    /// Create the next epoch from the current one with the pending deltas applied (spec section
    /// 5.1). The jitter has already passed.
    fn create_epoch(&mut self, now: Millis, host: &mut dyn Host) {
        if !matches!(self.phase, Phase::Member | Phase::Leaving)
            || self.cluster_id == ClusterId::NIL
        {
            return;
        }
        let current = host.epoch();
        if current.id().generation == 0 {
            return;
        }
        self.drop_shown_deltas(&current);
        if self.deltas.is_empty() {
            return;
        }
        let mut members: Vec<Member> = current
            .members()
            .iter()
            .filter(|m| !self.deltas.contains_key(&m.id))
            .cloned()
            .collect();
        members.extend(self.deltas.values().flatten().cloned());
        let id = EpochId {
            generation: host.next_generation(),
            origin: self.me.id,
        };
        match Epoch::new(id, self.cluster_id, members) {
            Ok(epoch) => {
                if let Err(e) = host.adopt(epoch) {
                    tracing::warn!(error = %e, "could not adopt an own epoch");
                }
            }
            Err(e) => tracing::warn!(error = %e, "could not build an epoch"),
        }
        self.sync_epoch(now, host);
    }

    /// Process an epoch change, wherever it came from.
    fn sync_epoch(&mut self, now: Millis, host: &mut dyn Host) {
        let current = host.epoch();
        let same =
            |e: &Arc<Epoch>| e.id() == current.id() && e.cluster_id() == current.cluster_id();
        if self.last_epoch.as_ref().is_some_and(same) {
            return;
        }
        let old = self.last_epoch.replace(current.clone());
        if let Some(old) = old.filter(|o| o.cluster_id() == current.cluster_id()) {
            self.events.push(Event::EpochAdopted {
                old: old.id(),
                new: current.id(),
            });
            let gained = gained_owners(&old, &current, self.cfg.replication_factor);
            if !gained.is_empty() {
                self.events.push(Event::OwnershipGained {
                    old: old.id(),
                    new: current.id(),
                    gained,
                });
            }
            let me = self.me.id;
            let normal = |e: &Epoch| e.member(me).map(|m| m.state) == Some(MemberState::Normal);
            // The rejoin rule needs the node to have been placed. A joiner that adopts an epoch
            // made before its own change keeps its pending delta instead.
            if self.phase == Phase::Member && normal(&old) && !normal(&current) {
                self.set_state(now, MemberState::Joining, Phase::Seeking);
                self.events.push(Event::Rejoining {
                    incarnation: self.me.incarnation,
                });
            }
        }
        self.drop_shown_deltas(&current);
        if !self.deltas.is_empty() {
            self.arm(now);
        }
    }

    /// Merge a whole message: the sender, its rumors, and its epoch.
    fn absorb(&mut self, now: Millis, g: &Gossip, host: &mut dyn Host) {
        if g.from.id == self.me.id {
            return;
        }
        if g.cluster_id != ClusterId::NIL {
            self.last_normal_seen = Some(now);
        }
        if g.cluster_id != ClusterId::NIL
            && self.cluster_id != ClusterId::NIL
            && g.cluster_id != self.cluster_id
        {
            self.foreign(now, g, host);
            return;
        }
        if g.cluster_id == ClusterId::NIL || self.cluster_id == ClusterId::NIL {
            return;
        }
        self.learn(
            now,
            Rumor {
                info: g.from.clone(),
                liveness: Liveness::Alive,
                accuser: None,
            },
            true,
        );
        if let Some(r) = self.members.get_mut(&g.from.id) {
            r.hlc = Some((g.hlc, now));
        }
        for rumor in &g.rumors {
            self.learn(now, rumor.clone(), false);
        }
        if let Some(body) = &g.epoch_body {
            if body.cluster_id() == self.cluster_id && body.id() > host.epoch().id() {
                if let Err(e) = host.adopt(body.clone()) {
                    tracing::debug!(error = %e, "refused a gossiped epoch");
                }
            }
        }
        self.sync_epoch(now, host);
    }

    /// A message from another cluster (spec section 6.3). The loser discards its cluster and
    /// joins the winner through the sender.
    fn foreign(&mut self, now: Millis, g: &Gossip, host: &mut dyn Host) {
        if self.phase != Phase::Member {
            return;
        }
        let size = self
            .last_epoch
            .as_ref()
            .map(|e| normal_count(e))
            .unwrap_or(0) as u32;
        let won = winner(
            (self.cluster_id, size),
            (g.cluster_id, g.cluster_size),
            host.lease_cluster(),
        );
        if won != g.cluster_id {
            return;
        }
        tracing::warn!(ours = %self.cluster_id, winner = %won, "lost a merge, joining the winner");
        self.events.push(Event::ClusterLost {
            old: self.cluster_id,
            winner: won,
        });
        host.reset_cluster(provisional_epoch(self.me.id));
        self.cluster_id = ClusterId::NIL;
        self.members.clear();
        self.deltas.clear();
        self.epoch_timer = None;
        self.order.clear();
        self.pending.clear();
        self.probe = None;
        self.broadcasts = Broadcasts::default();
        self.last_epoch = Some(host.epoch());
        self.last_normal_seen = Some(now);
        self.add_seed(g.from.addr);
        self.set_state(now, MemberState::Joining, Phase::Seeking);
    }

    fn may_form(&self, now: Millis) -> bool {
        let deadline = self.cfg.bootstrap_deadline_ms;
        self.phase == Phase::Seeking
            && self.cluster_id == ClusterId::NIL
            && self
                .seek_started
                .is_some_and(|s| now.saturating_sub(s) >= deadline)
            && self
                .last_normal_seen
                .is_none_or(|t| now.saturating_sub(t) >= deadline)
    }

    /// Form a new cluster: epoch `(1, self)`, the HLC stays on the wall clock (spec section 6.2).
    pub fn form(&mut self, now: Millis, cluster_id: ClusterId, host: &mut dyn Host) {
        if self.phase != Phase::Seeking || self.cluster_id != ClusterId::NIL {
            return;
        }
        let me = Member {
            id: self.me.id,
            addr: self.me.addr,
            state: MemberState::Normal,
            weight: self.me.weight,
        };
        host.reset_cluster(Epoch::bootstrap(me, cluster_id));
        self.cluster_id = cluster_id;
        self.last_epoch = Some(host.epoch());
        tracing::info!(cluster = %cluster_id, "formed a new cluster");
        self.set_state(now, MemberState::Normal, Phase::Member);
    }

    pub fn lease_result(
        &mut self,
        now: Millis,
        result: Result<LeaseState, LeaseError>,
        host: &mut dyn Host,
    ) {
        match result {
            Ok(LeaseState::Held(h)) => {
                if h.identity == self.me.id.to_string() && self.may_form(now) {
                    self.form(now, h.cluster_id, host);
                }
            }
            Ok(LeaseState::HeldByOther(h)) => self.add_seed(h.address),
            Err(e) => tracing::debug!(error = %e, "the lease call failed"),
        }
    }

    /// Start the drain: `LEAVING` leaves placement in the next epoch, and the node keeps serving.
    pub fn leave(&mut self, now: Millis) {
        if self.phase == Phase::Leaving {
            return;
        }
        self.set_state(now, MemberState::Leaving, Phase::Leaving);
    }

    /// The final messages before the node leaves gossip: its own death, to a few members.
    pub fn depart(&mut self, host: &dyn Host) -> Vec<Action> {
        let mut targets: Vec<(NodeId, SocketAddr)> = self
            .members
            .iter()
            .filter(|(_, r)| r.liveness == Liveness::Alive)
            .map(|(id, r)| (*id, r.info.addr))
            .collect();
        self.rng.shuffle(&mut targets);
        targets.truncate(LEAVE_FANOUT);
        let mut out = Vec::new();
        for (id, addr) in targets {
            let mut msg = self.bare(GossipKind::Leave, host);
            msg.rumors = vec![Rumor {
                info: self.me.clone(),
                liveness: Liveness::Dead,
                accuser: None,
            }];
            let token = self.token(Pending::Leave);
            out.push(Action {
                token,
                to: addr,
                expected: Some(id),
                msg,
                timeout_ms: self.cfg.probe_timeout_ms,
            });
        }
        out
    }

    pub fn tick(&mut self, now: Millis, host: &mut dyn Host) -> Vec<Action> {
        let mut out = Vec::new();
        self.sync_epoch(now, host);
        if self.epoch_timer.is_some_and(|t| now >= t) {
            self.epoch_timer = None;
            self.create_epoch(now, host);
        }
        self.rescale_detectors(now);
        self.check_failures(now);
        if now >= self.next_probe {
            self.next_probe = now + self.cfg.period_ms;
            out.extend(self.start_probe(host));
        }
        if self.phase == Phase::Seeking {
            self.seek(now, host, &mut out);
        }
        if now >= self.next_sync {
            self.next_sync = now + self.cfg.sync_interval_ms;
            let alive: Vec<NodeId> = self
                .members
                .iter()
                .filter(|(_, r)| r.liveness != Liveness::Dead)
                .map(|(id, _)| *id)
                .collect();
            if let Some(&target) = pick(&mut self.rng, &alive) {
                out.extend(self.sync_to(target, host));
            }
        }
        if now >= self.next_reconnect && self.phase != Phase::Seeking {
            self.next_reconnect = now + self.cfg.reconnect_interval_ms;
            out.extend(self.reconnect(host));
        }
        self.prune(now);
        out
    }

    fn seek(&mut self, now: Millis, host: &dyn Host, out: &mut Vec<Action>) {
        self.seek_started.get_or_insert(now);
        if now >= self.next_join {
            self.next_join = now + self.cfg.period_ms;
            let targets = self.join_targets();
            for i in 0..targets.len().min(JOIN_FANOUT) {
                let to = targets[(self.join_cursor + i) % targets.len()];
                let msg = self.message(GossipKind::Join, None, host);
                let token = self.token(Pending::Join);
                out.push(Action {
                    token,
                    to,
                    expected: None,
                    msg,
                    timeout_ms: self.cfg.period_ms,
                });
            }
            self.join_cursor = self.join_cursor.wrapping_add(JOIN_FANOUT);
        }
        if self.may_form(now) && now >= self.next_lease {
            self.next_lease = now + self.cfg.period_ms;
            self.events.push(Event::LeaseWanted);
        }
    }

    /// Seeds, the members of the known epoch, and the live members of the view.
    fn join_targets(&self) -> Vec<SocketAddr> {
        let mut targets = self.seeds.clone();
        if let Some(e) = self.last_epoch.as_ref().filter(|e| e.id().generation >= 1) {
            targets.extend(e.members().iter().map(|m| m.addr));
        }
        targets.extend(
            self.members
                .values()
                .filter(|r| r.liveness != Liveness::Dead)
                .map(|r| r.info.addr),
        );
        let mut seen = BTreeSet::new();
        targets.retain(|a| *a != self.me.addr && seen.insert(*a));
        targets
    }

    /// A sync with a dead member or a seed that is not a live member. This is how a healed
    /// partition and a second cluster find each other.
    fn reconnect(&mut self, host: &dyn Host) -> Option<Action> {
        let live: BTreeSet<SocketAddr> = self
            .members
            .values()
            .filter(|r| r.liveness != Liveness::Dead)
            .map(|r| r.info.addr)
            .collect();
        let mut targets: Vec<(Option<NodeId>, SocketAddr)> = self
            .members
            .iter()
            .filter(|(_, r)| r.liveness == Liveness::Dead)
            .map(|(id, r)| (Some(*id), r.info.addr))
            .collect();
        targets.extend(
            self.seeds
                .iter()
                .filter(|s| !live.contains(s))
                .map(|s| (None, *s)),
        );
        let &(expected, to) = pick(&mut self.rng, &targets)?;
        let msg = self.message(GossipKind::Sync, expected, host);
        let token = self.token(Pending::Sync);
        Some(Action {
            token,
            to,
            expected,
            msg,
            timeout_ms: self.cfg.period_ms,
        })
    }

    fn sync_to(&mut self, target: NodeId, host: &dyn Host) -> Option<Action> {
        let addr = self.members.get(&target)?.info.addr;
        let msg = self.message(GossipKind::Sync, Some(target), host);
        let token = self.token(Pending::Sync);
        Some(Action {
            token,
            to: addr,
            expected: Some(target),
            msg,
            timeout_ms: self.cfg.period_ms,
        })
    }

    fn rescale_detectors(&mut self, now: Millis) {
        let n = self.alive_count() + 1;
        if n > self.detector_n * 2 || n * 2 < self.detector_n {
            self.detector_n = n;
            for r in self.members.values_mut() {
                r.detector = Detector::new(&self.cfg, now, n);
            }
        }
    }

    fn check_failures(&mut self, now: Millis) {
        let me = self.me.id;
        let mut changes = Vec::new();
        for (id, r) in self.members.iter_mut() {
            let score = r.detector.score(now);
            match r.liveness {
                Liveness::Alive if score >= self.cfg.phi_suspect => {
                    r.liveness = Liveness::Suspect;
                    r.since = now;
                    r.accusers = BTreeSet::from([me]);
                    changes.push(*id);
                }
                Liveness::Suspect => {
                    if score >= self.cfg.phi_suspect && r.accusers.insert(me) {
                        self.broadcasts.push(*id);
                    }
                    let others = r.accusers.iter().filter(|a| **a != me).count();
                    let timeout = suspicion_timeout(&self.cfg, others);
                    if now.saturating_sub(r.since) >= timeout || score >= self.cfg.phi_dead {
                        r.liveness = Liveness::Dead;
                        r.since = now;
                        changes.push(*id);
                    }
                }
                _ => {}
            }
        }
        for id in changes {
            self.broadcasts.push(id);
            if let Some(r) = self.members.get(&id) {
                self.events.push(Event::MemberChanged {
                    id,
                    state: r.info.state,
                    liveness: r.liveness,
                });
            }
            self.on_change(now, id);
        }
    }

    /// Drop dead records past the retention, and the oldest beyond `max_dead`.
    fn prune(&mut self, now: Millis) {
        let retention = self.cfg.dead_retention_ms;
        self.members
            .retain(|_, r| r.liveness != Liveness::Dead || now.saturating_sub(r.since) < retention);
        let mut dead: Vec<(Millis, NodeId)> = self
            .members
            .iter()
            .filter(|(_, r)| r.liveness == Liveness::Dead)
            .map(|(id, r)| (r.since, *id))
            .collect();
        if dead.len() > self.cfg.max_dead {
            dead.sort();
            let excess = dead.len() - self.cfg.max_dead;
            for (_, id) in &dead[..excess] {
                self.members.remove(id);
            }
        }
    }

    /// Shuffled round-robin over the members that are not dead.
    fn next_target(&mut self) -> Option<NodeId> {
        loop {
            if self.order.is_empty() {
                self.order = self
                    .members
                    .iter()
                    .filter(|(_, r)| r.liveness != Liveness::Dead)
                    .map(|(id, _)| *id)
                    .collect();
                self.rng.shuffle(&mut self.order);
                if self.order.is_empty() {
                    return None;
                }
            }
            let id = self.order.pop()?;
            if self
                .members
                .get(&id)
                .is_some_and(|r| r.liveness != Liveness::Dead)
            {
                return Some(id);
            }
        }
    }

    fn start_probe(&mut self, host: &dyn Host) -> Option<Action> {
        let target = self.next_target()?;
        let addr = self.members.get(&target)?.info.addr;
        self.probe = Some(Probe {
            target,
            acked: false,
            indirect_left: 0,
        });
        let msg = self.message(GossipKind::Ping, Some(target), host);
        let token = self.token(Pending::Direct { target });
        Some(Action {
            token,
            to: addr,
            expected: Some(target),
            msg,
            timeout_ms: self.cfg.probe_timeout_ms,
        })
    }

    fn missed(&mut self, now: Millis, target: NodeId) {
        let weight = self.cfg.missed_ping_weight;
        if let Some(r) = self.members.get_mut(&target) {
            r.detector.evidence(now, weight);
        }
    }

    fn probe_acked(&mut self, now: Millis, target: NodeId) {
        if let Some(r) = self.members.get_mut(&target) {
            r.detector.heartbeat(now);
        }
        if let Some(p) = self.probe.as_mut().filter(|p| p.target == target) {
            p.acked = true;
        }
    }

    pub fn outcome(
        &mut self,
        now: Millis,
        token: u64,
        outcome: Outcome,
        host: &mut dyn Host,
    ) -> Vec<Action> {
        let Some(pending) = self.pending.remove(&token) else {
            return Vec::new();
        };
        if let Outcome::Reply(g) = &outcome {
            self.absorb(now, g, host);
        }
        match pending {
            Pending::Direct { target } => match outcome {
                Outcome::Reply(_) => {
                    self.probe_acked(now, target);
                    Vec::new()
                }
                _ => self.start_indirect(now, target, host),
            },
            Pending::Indirect { target } => {
                if matches!(&outcome, Outcome::Reply(g) if g.kind == GossipKind::Ack) {
                    self.probe_acked(now, target);
                }
                let done = match self.probe.as_mut().filter(|p| p.target == target) {
                    Some(p) => {
                        p.indirect_left = p.indirect_left.saturating_sub(1);
                        (p.indirect_left == 0 && !p.acked).then_some(target)
                    }
                    None => None,
                };
                if let Some(t) = done {
                    self.missed(now, t);
                }
                Vec::new()
            }
            Pending::Join => {
                match outcome {
                    Outcome::Reply(g) if g.kind == GossipKind::JoinReply => {
                        self.join_reply(now, *g, host)
                    }
                    Outcome::Silent { in_cluster: true } => self.last_normal_seen = Some(now),
                    _ => {}
                }
                Vec::new()
            }
            Pending::Relay { .. } | Pending::Sync | Pending::Leave => Vec::new(),
        }
    }

    /// Take the epoch and the HLC maximum, then become `NORMAL` (spec sections 6.2 and 8.1).
    fn join_reply(&mut self, now: Millis, g: Gossip, host: &mut dyn Host) {
        if g.retry_after_ms > 0 {
            self.next_join = now + Millis::from(g.retry_after_ms);
            return;
        }
        if g.cluster_id == ClusterId::NIL || self.phase != Phase::Seeking {
            return;
        }
        let Some(body) = g
            .epoch_body
            .clone()
            .filter(|b| b.cluster_id() == g.cluster_id)
        else {
            return;
        };
        if self.cluster_id == ClusterId::NIL {
            host.reset_cluster(body);
            self.cluster_id = g.cluster_id;
            self.last_epoch = Some(host.epoch());
        } else if self.cluster_id == g.cluster_id {
            if body.id() > host.epoch().id() {
                if let Err(e) = host.adopt(body) {
                    tracing::debug!(error = %e, "refused a joined epoch");
                }
            }
        } else {
            return;
        }
        if !host.set_hlc_floor(g.hlc) {
            tracing::warn!("the cluster HLC is too far ahead of the wall clock, not joining yet");
            return;
        }
        self.absorb(now, &g, host);
        self.set_state(now, MemberState::Normal, Phase::Member);
    }

    fn start_indirect(&mut self, now: Millis, target: NodeId, host: &dyn Host) -> Vec<Action> {
        let Some(t) = self.members.get(&target) else {
            return Vec::new();
        };
        let target_addr = t.info.addr;
        let mut helpers: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(id, r)| **id != target && r.liveness == Liveness::Alive)
            .map(|(id, _)| *id)
            .collect();
        self.rng.shuffle(&mut helpers);
        helpers.truncate(self.cfg.indirect_probes);
        if helpers.is_empty() {
            self.missed(now, target);
            return Vec::new();
        }
        if let Some(p) = self.probe.as_mut().filter(|p| p.target == target) {
            p.indirect_left = helpers.len();
        }
        let timeout = self
            .cfg
            .period_ms
            .saturating_sub(self.cfg.probe_timeout_ms)
            .max(1);
        let mut out = Vec::new();
        for helper in helpers {
            let Some(addr) = self.members.get(&helper).map(|r| r.info.addr) else {
                continue;
            };
            let mut msg = self.message(GossipKind::PingReq, Some(helper), host);
            msg.target = Some((target, target_addr));
            let token = self.token(Pending::Indirect { target });
            out.push(Action {
                token,
                to: addr,
                expected: Some(helper),
                msg,
                timeout_ms: timeout,
            });
        }
        out
    }

    /// Answer an inbound message from `src`.
    pub fn handle(
        &mut self,
        now: Millis,
        src: SocketAddr,
        msg: Gossip,
        host: &mut dyn Host,
    ) -> Handled {
        self.sync_epoch(now, host);
        self.absorb(now, &msg, host);
        if msg.kind == GossipKind::Join {
            return Handled {
                reply: self.answer_join(now, src, &msg, host),
                relay: None,
            };
        }
        // Seekers and foreign clusters get only the sender part. A merge loser has already
        // switched to the provisional cluster in `absorb`.
        if msg.cluster_id == ClusterId::NIL || msg.cluster_id != self.cluster_id {
            return Handled {
                reply: self.bare(GossipKind::Ack, host),
                relay: None,
            };
        }
        let sender = Some(msg.from.id);
        match msg.kind {
            GossipKind::PingReq => {
                let Some((target, addr)) = msg.target else {
                    return self.reply(GossipKind::Nack, sender, host);
                };
                let ping = self.message(GossipKind::Ping, Some(target), host);
                let token = self.token(Pending::Relay { target });
                let timeout = self.cfg.probe_timeout_ms;
                Handled {
                    reply: self.message(GossipKind::Nack, sender, host),
                    relay: Some(Action {
                        token,
                        to: addr,
                        expected: Some(target),
                        msg: ping,
                        timeout_ms: timeout,
                    }),
                }
            }
            GossipKind::Sync => self.reply(GossipKind::SyncReply, sender, host),
            _ => self.reply(GossipKind::Ack, sender, host),
        }
    }

    fn answer_join(
        &mut self,
        now: Millis,
        src: SocketAddr,
        msg: &Gossip,
        host: &dyn Host,
    ) -> Gossip {
        if self.phase != Phase::Member || self.cluster_id == ClusterId::NIL {
            let mut g = self.bare(GossipKind::JoinReply, host);
            g.cluster_id = ClusterId::NIL;
            return g;
        }
        if msg.cluster_id != ClusterId::NIL && msg.cluster_id != self.cluster_id {
            return self.bare(GossipKind::JoinReply, host);
        }
        if let Err(wait) = self.limiter.admit(src.ip(), now) {
            let mut g = self.bare(GossipKind::JoinReply, host);
            g.retry_after_ms = wait.max(1);
            return g;
        }
        self.learn(
            now,
            Rumor {
                info: msg.from.clone(),
                liveness: Liveness::Alive,
                accuser: None,
            },
            true,
        );
        let mut g = self.message(GossipKind::JoinReply, Some(msg.from.id), host);
        g.hlc = self.cluster_max_hlc(now, host);
        g
    }

    /// The highest HLC that this node issued, observed, or heard from a member recently.
    fn cluster_max_hlc(&self, now: Millis, host: &dyn Host) -> u64 {
        self.members
            .values()
            .filter_map(|r| r.hlc)
            .filter(|(_, at)| now.saturating_sub(*at) < HLC_WINDOW_MS)
            .map(|(h, _)| h)
            .fold(host.hlc(), u64::max)
    }

    fn reply(&mut self, kind: GossipKind, to: Option<NodeId>, host: &dyn Host) -> Handled {
        Handled {
            reply: self.message(kind, to, host),
            relay: None,
        }
    }

    /// The result of the ping that a `PING_REQ` asked for. Returns the reply to the requester.
    pub fn relayed(
        &mut self,
        now: Millis,
        token: u64,
        outcome: Outcome,
        host: &mut dyn Host,
    ) -> Gossip {
        let target = match self.pending.remove(&token) {
            Some(Pending::Relay { target }) => Some(target),
            _ => None,
        };
        match (target, outcome) {
            (Some(target), Outcome::Reply(g)) => {
                self.absorb(now, &g, host);
                if let Some(r) = self.members.get_mut(&target) {
                    r.detector.heartbeat(now);
                }
                self.message(GossipKind::Ack, None, host)
            }
            _ => self.message(GossipKind::Nack, None, host),
        }
    }
}

/// Lifeguard suspicion timeout: `max - (max - min) * ln(c + 1) / ln(K + 1)` with `c` other
/// accusers, capped at `K`.
pub fn suspicion_timeout(cfg: &SwimConfig, others: usize) -> Millis {
    let k = cfg.suspicion_k.max(1) as f64;
    let c = others.min(cfg.suspicion_k) as f64;
    let (min, max) = (cfg.suspicion_min_ms as f64, cfg.suspicion_max_ms as f64);
    let t = max - (max - min) * (c + 1.0).ln() / (k + 1.0).ln();
    t.max(min) as Millis
}

pub fn normal_count(epoch: &Epoch) -> usize {
    epoch
        .members()
        .iter()
        .filter(|m| m.state == MemberState::Normal)
        .count()
}

/// The epoch of a node that is not in a cluster: generation 0, `ClusterId::NIL`, no members. Its
/// owner sets are empty, and `HELLO` reports generation 0.
pub fn provisional_epoch(id: NodeId) -> Epoch {
    let id = EpochId {
        generation: 0,
        origin: id,
    };
    Epoch::new(id, ClusterId::NIL, Vec::new()).expect("an empty member list has no duplicates")
}

/// The members `NORMAL` in `new` that gain an owner slot for some key. An added member gains.
/// When a `NORMAL` member left and the old epoch had more than `rf` of them, every remaining
/// member gains, because the keys of the departed member move to them. With `rf` or fewer, every
/// member already owned every key.
pub fn gained_owners(old: &Epoch, new: &Epoch, rf: usize) -> Vec<NodeId> {
    let normal = |e: &Epoch| -> BTreeSet<NodeId> {
        e.members()
            .iter()
            .filter(|m| m.state == MemberState::Normal)
            .map(|m| m.id)
            .collect()
    };
    let (o, n) = (normal(old), normal(new));
    let mut gained: BTreeSet<NodeId> = n.difference(&o).copied().collect();
    if o.difference(&n).next().is_some() && o.len() > rf {
        gained.extend(n.intersection(&o).copied());
    }
    gained.into_iter().collect()
}

fn pick<'a, T>(rng: &mut fastrand::Rng, items: &'a [T]) -> Option<&'a T> {
    if items.is_empty() {
        None
    } else {
        items.get(rng.usize(..items.len()))
    }
}
