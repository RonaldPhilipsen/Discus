//! The sans-IO SWIM core (spec section 6.1).
//!
//! The caller supplies the time and the inbound messages, and sends the actions that the core
//! returns. Each action is one request with one reply. The caller reports the result with
//! [`Swim::outcome`], or with [`Swim::relayed`] for the ping that a `PING_REQ` asked for.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;

use bytes::Bytes;
use discus_placement::{ClusterId, Epoch, MemberState, NodeId};
use discus_wire::{Capacity, Gossip, GossipKind, Liveness, MemberInfo, Rumor};

use crate::view::{retransmit_limit, supersedes, Broadcasts, Record};
use crate::{Detector, Event, Host, Millis, SwimConfig};

/// One request to send.
#[derive(Debug, Clone, PartialEq)]
pub struct Action {
    /// Report the result under this token.
    pub token: u64,
    pub to: SocketAddr,
    /// The node that the core expects at `to`, if it knows one.
    pub expected: Option<NodeId>,
    pub msg: Gossip,
    pub timeout_ms: Millis,
}

/// The result of one action.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Reply(Box<Gossip>),
    /// The connection worked but no reply came in time. `in_cluster` is true when the peer
    /// reported an epoch generation of 1 or more in `HELLO`.
    Silent {
        in_cluster: bool,
    },
    /// No connection.
    Unreachable,
}

/// The answer to an inbound message.
#[derive(Debug, Clone, PartialEq)]
pub struct Handled {
    pub reply: Gossip,
    /// A ping to send before the reply. Report its result with [`Swim::relayed`], which gives
    /// the final reply.
    pub relay: Option<Action>,
}

/// What a token waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    Direct { target: NodeId },
    Indirect { target: NodeId },
    Relay { target: NodeId },
    Sync,
}

/// The probe of the current period.
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
}

impl Swim {
    pub fn new(cfg: SwimConfig, me: MemberInfo) -> Swim {
        let rng = fastrand::Rng::with_seed(cfg.seed);
        Swim {
            cfg,
            me,
            cluster_id: ClusterId::NIL,
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
        }
    }

    pub fn config(&self) -> &SwimConfig {
        &self.cfg
    }

    /// The local member.
    pub fn me(&self) -> &MemberInfo {
        &self.me
    }

    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// Make this node a member of `cluster_id` in `state` without a join. The simulator uses it
    /// to start from a formed cluster.
    pub fn set_static_member(&mut self, cluster_id: ClusterId, state: MemberState) {
        self.cluster_id = cluster_id;
        self.me.state = state;
    }

    /// Every other member that the view holds, with its liveness.
    pub fn members(&self) -> Vec<(MemberInfo, Liveness)> {
        self.members
            .values()
            .map(|r| (r.info.clone(), r.liveness))
            .collect()
    }

    /// The liveness of `id` in the view.
    pub fn liveness(&self, id: NodeId) -> Option<Liveness> {
        self.members.get(&id).map(|r| r.liveness)
    }

    /// The record of `id` in the view.
    pub fn member(&self, id: NodeId) -> Option<&MemberInfo> {
        self.members.get(&id).map(|r| &r.info)
    }

    /// The detector score of `id` at `now`.
    pub fn score(&self, now: Millis, id: NodeId) -> f64 {
        self.members
            .get(&id)
            .map(|r| r.detector.score(now))
            .unwrap_or(0.0)
    }

    /// The events since the last call.
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    /// Publish new counter leader hints. They spread with a new incarnation.
    pub fn set_leader_hints(&mut self, hints: Bytes) {
        self.me.leader_hints = hints;
        self.bump();
    }

    /// Update the capacity hints. They travel in the sender part of every message.
    pub fn set_capacity(&mut self, capacity: Capacity) {
        self.me.capacity = capacity;
    }

    /// Raise the own incarnation and queue the own record.
    fn bump(&mut self) {
        self.me.incarnation += 1;
        self.broadcasts.push(self.me.id);
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

    /// The rumor of `id` as this node would send it.
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

    /// Every record, for a full state exchange.
    fn full_view(&self) -> Vec<Rumor> {
        std::iter::once(self.me.id)
            .chain(self.members.keys().copied())
            .filter_map(|id| self.rumor_of(id))
            .collect()
    }

    /// Build a message of `kind` for `to`.
    pub(crate) fn message(
        &mut self,
        kind: GossipKind,
        to: Option<NodeId>,
        host: &dyn Host,
    ) -> Gossip {
        let epoch = host.epoch();
        self.me.epoch = epoch.id();
        let full = matches!(
            kind,
            GossipKind::Sync | GossipKind::SyncReply | GossipKind::Join | GossipKind::JoinReply
        );
        let rumors = if full {
            self.full_view()
        } else {
            let limit = retransmit_limit(self.cfg.retransmit_mult, self.alive_count() + 1);
            let ids = self.broadcasts.take(self.cfg.max_rumors, limit);
            ids.into_iter().filter_map(|id| self.rumor_of(id)).collect()
        };
        let stale = to
            .and_then(|t| self.members.get(&t))
            .is_some_and(|r| r.info.epoch < epoch.id());
        let in_cluster = epoch.id().generation >= 1 && !self.cluster_id_is_nil();
        Gossip {
            kind,
            from: self.me.clone(),
            cluster_id: self.cluster_id,
            cluster_size: normal_count(&epoch) as u32,
            epoch_body: (in_cluster && (full || stale)).then(|| (*epoch).clone()),
            hlc: host.hlc(),
            target: None,
            rumors,
            retry_after_ms: 0,
        }
    }

    fn cluster_id_is_nil(&self) -> bool {
        self.cluster_id == ClusterId::NIL
    }

    /// Merge one rumor into the view. `direct` is true for the sender part of a message, which
    /// also counts as a heartbeat.
    pub fn learn(&mut self, now: Millis, rumor: Rumor, direct: bool) {
        let id = rumor.info.id;
        if id == self.me.id {
            if rumor.liveness != Liveness::Alive && rumor.info.incarnation >= self.me.incarnation {
                // Refute: only this node raises its incarnation.
                self.me.incarnation = rumor.info.incarnation;
                self.bump();
            }
            return;
        }
        let new = (rumor.info.incarnation, rumor.liveness);
        let mut changed = None;
        match self.members.get_mut(&id) {
            None => {
                let mut record = Record {
                    info: rumor.info.clone(),
                    liveness: rumor.liveness,
                    since: now,
                    accusers: BTreeSet::new(),
                    detector: Detector::new(&self.cfg, now),
                    hlc: None,
                };
                if let Some(a) = rumor
                    .accuser
                    .filter(|_| rumor.liveness == Liveness::Suspect)
                {
                    record.accusers.insert(a);
                }
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
                    if let Some(a) = rumor
                        .accuser
                        .filter(|_| rumor.liveness == Liveness::Suspect)
                    {
                        r.accusers.insert(a);
                    }
                    r.info = rumor.info;
                    r.liveness = rumor.liveness;
                    self.broadcasts.push(id);
                } else if new == old && rumor.liveness == Liveness::Suspect {
                    if let Some(a) = rumor.accuser {
                        if r.accusers.insert(a) && r.accusers.len() <= self.cfg.suspicion_k + 1 {
                            self.broadcasts.push(id);
                        }
                    }
                } else if direct && new.0 == old.0 {
                    // The member speaks for itself: refresh the fields that change without a new
                    // incarnation.
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

    /// A member changed its state or liveness. Task 4 hooks the epoch deltas here.
    fn on_change(&mut self, _now: Millis, _id: NodeId) {}

    /// Merge a whole message: the sender, its rumors, and its epoch.
    fn absorb(&mut self, now: Millis, g: &Gossip, host: &mut dyn Host) {
        if g.from.id == self.me.id {
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
    }

    /// Advance the timers. Returns the requests to send.
    pub fn tick(&mut self, now: Millis, host: &mut dyn Host) -> Vec<Action> {
        let mut out = Vec::new();
        self.check_failures(now);
        if now >= self.next_probe {
            self.next_probe = now + self.cfg.period_ms;
            if let Some(a) = self.start_probe(host) {
                out.push(a);
            }
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
        if now >= self.next_reconnect {
            self.next_reconnect = now + self.cfg.reconnect_interval_ms;
            let dead: Vec<NodeId> = self
                .members
                .iter()
                .filter(|(_, r)| r.liveness == Liveness::Dead)
                .map(|(id, _)| *id)
                .collect();
            if let Some(&target) = pick(&mut self.rng, &dead) {
                out.extend(self.sync_to(target, host));
            }
        }
        self.prune(now);
        out
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

    /// Suspect members whose score passed the threshold, and declare dead the suspects whose
    /// timeout ended.
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

    /// The next probe target in a shuffled round-robin over the members that are not dead.
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

    /// The probe of `target` failed directly and through every helper.
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

    /// Report the result of an action.
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
            Pending::Relay { .. } | Pending::Sync => Vec::new(),
        }
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

    /// Answer an inbound message.
    pub fn handle(
        &mut self,
        now: Millis,
        _src: SocketAddr,
        msg: Gossip,
        host: &mut dyn Host,
    ) -> Handled {
        self.absorb(now, &msg, host);
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

/// The `NORMAL` members of an epoch.
pub fn normal_count(epoch: &Epoch) -> usize {
    epoch
        .members()
        .iter()
        .filter(|m| m.state == MemberState::Normal)
        .count()
}

fn pick<'a, T>(rng: &mut fastrand::Rng, items: &'a [T]) -> Option<&'a T> {
    if items.is_empty() {
        None
    } else {
        items.get(rng.usize(..items.len()))
    }
}
