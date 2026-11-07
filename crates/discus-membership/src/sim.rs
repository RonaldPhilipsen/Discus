//! A deterministic network simulator for the SWIM core. Tests only: never enable the `testing`
//! feature in a release build.
//!
//! Time is virtual and moves in 10 ms steps. Every node runs the real [`Swim`] core with a
//! [`SimHost`]. A message takes 1 to 5 ms. A request that gets no reply resolves at its timeout,
//! as `Silent` when the target is up and reachable but silenced, else as `Unreachable`.
//! Partitions, isolation, and kills drop messages.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use discus_placement::{
    Adopt, ClusterId, Epoch, EpochError, EpochHistory, EpochId, Member, MemberState, NodeId, Weight,
};
use discus_wire::{Capacity, Gossip, Liveness, MemberInfo, Rumor};

use crate::{Action, Host, Millis, Outcome, Swim, SwimConfig};

/// The time step of the simulator.
pub const STEP_MS: Millis = 10;

/// The host side of one simulated node.
pub struct SimHost {
    pub history: EpochHistory,
    /// The HLC as a plain number. It follows the virtual time plus `clock_offset`.
    pub hlc: u64,
    pub clock_offset: u64,
    pub lease_cluster: Option<ClusterId>,
    now: Millis,
}

impl SimHost {
    fn new(id: NodeId) -> SimHost {
        SimHost {
            history: EpochHistory::new(provisional(id)),
            hlc: 0,
            clock_offset: 0,
            lease_cluster: None,
            now: 0,
        }
    }

    fn wall(&self) -> u64 {
        self.now + self.clock_offset
    }
}

/// The epoch of a node that is not in a cluster: generation 0, no members.
pub fn provisional(id: NodeId) -> Epoch {
    Epoch::new(
        EpochId {
            generation: 0,
            origin: id,
        },
        ClusterId::NIL,
        Vec::new(),
    )
    .unwrap()
}

impl Host for SimHost {
    fn epoch(&self) -> Arc<Epoch> {
        self.history.current().clone()
    }

    fn adopt(&mut self, epoch: Epoch) -> Result<Adopt, EpochError> {
        self.history.adopt(epoch)
    }

    fn next_generation(&self) -> u64 {
        self.history.next_generation()
    }

    fn reset_cluster(&mut self, epoch: Epoch) {
        self.history = EpochHistory::new(epoch);
    }

    fn hlc(&self) -> u64 {
        self.hlc.max(self.wall())
    }

    fn set_hlc_floor(&mut self, floor: u64) -> bool {
        if floor > self.wall() + 1000 {
            return false;
        }
        self.hlc = self.hlc.max(floor);
        true
    }

    fn lease_cluster(&self) -> Option<ClusterId> {
        self.lease_cluster
    }
}

pub struct SimNode {
    pub swim: Swim,
    pub host: SimHost,
    pub alive: bool,
    /// The node answers nothing, but its connections work.
    pub silenced: bool,
    isolated_until: Millis,
    sent: u64,
    /// Tokens that wait for a reply or a timeout.
    waiting: BTreeSet<u64>,
    /// Relay tokens: the requester and its token.
    relays: HashMap<u64, (NodeId, u64)>,
}

enum Delivery {
    Request {
        from: NodeId,
        to: SocketAddr,
        token: u64,
        msg: Gossip,
    },
    Reply {
        from: NodeId,
        to: NodeId,
        token: u64,
        msg: Gossip,
    },
    Timeout {
        to: NodeId,
        token: u64,
        target: SocketAddr,
    },
}

pub struct Sim {
    now: Millis,
    rng: fastrand::Rng,
    cfg: SwimConfig,
    nodes: BTreeMap<NodeId, SimNode>,
    by_addr: HashMap<SocketAddr, NodeId>,
    queue: BTreeMap<(Millis, u64), Delivery>,
    seq: u64,
    groups: Option<Vec<BTreeSet<NodeId>>>,
}

impl Sim {
    pub fn new(seed: u64, cfg: SwimConfig) -> Sim {
        Sim {
            now: 0,
            rng: fastrand::Rng::with_seed(seed),
            cfg,
            nodes: BTreeMap::new(),
            by_addr: HashMap::new(),
            queue: BTreeMap::new(),
            seq: 0,
            groups: None,
        }
    }

    pub fn now(&self) -> Millis {
        self.now
    }

    /// Add a node that is not in a cluster yet. It starts `JOINING`.
    pub fn add_node(&mut self) -> NodeId {
        let id = NodeId::from_u128(self.rng.u128(1..));
        let n = self.by_addr.len() as u32 + 1;
        let addr = SocketAddr::from(([10, 0, (n >> 8) as u8, n as u8], 7379));
        let info = MemberInfo {
            id,
            addr,
            state: MemberState::Joining,
            incarnation: 0,
            weight: Weight::default(),
            epoch: provisional(id).id(),
            capabilities: discus_wire::caps::KNOWN,
            capacity: Capacity::default(),
            leader_hints: Bytes::new(),
        };
        let cfg = SwimConfig {
            seed: self.rng.u64(..),
            ..self.cfg.clone()
        };
        let node = SimNode {
            swim: Swim::new(cfg, info),
            host: SimHost::new(id),
            alive: true,
            silenced: false,
            isolated_until: 0,
            sent: 0,
            waiting: BTreeSet::new(),
            relays: HashMap::new(),
        };
        self.by_addr.insert(addr, id);
        self.nodes.insert(id, node);
        id
    }

    /// `n` nodes that already form one cluster: one epoch of generation 1 with every node
    /// `NORMAL`, and a full view on every node.
    pub fn cluster(&mut self, n: usize) -> Vec<NodeId> {
        let ids: Vec<NodeId> = (0..n).map(|_| self.add_node()).collect();
        let cluster = ClusterId::from_u128(self.rng.u128(1..));
        let members: Vec<Member> = ids
            .iter()
            .map(|id| Member {
                id: *id,
                addr: self.nodes[id].swim.me().addr,
                state: MemberState::Normal,
                weight: Weight::default(),
            })
            .collect();
        let origin = *ids.iter().min().unwrap();
        let epoch = Epoch::new(
            EpochId {
                generation: discus_placement::FIRST_GENERATION,
                origin,
            },
            cluster,
            members,
        )
        .unwrap();
        let infos: Vec<MemberInfo> = ids
            .iter()
            .map(|id| {
                let mut info = self.nodes[id].swim.me().clone();
                info.state = MemberState::Normal;
                info.epoch = epoch.id();
                info
            })
            .collect();
        let now = self.now;
        for id in &ids {
            let node = self.nodes.get_mut(id).unwrap();
            node.host.reset_cluster(epoch.clone());
            node.swim.set_static_member(cluster, MemberState::Normal);
            for info in infos.iter().filter(|i| i.id != *id) {
                node.swim.learn(
                    now,
                    Rumor {
                        info: info.clone(),
                        liveness: Liveness::Alive,
                        accuser: None,
                    },
                    true,
                );
            }
            node.swim.take_events();
        }
        ids
    }

    pub fn node(&self, id: NodeId) -> &SimNode {
        &self.nodes[&id]
    }

    pub fn node_mut(&mut self, id: NodeId) -> &mut SimNode {
        self.nodes.get_mut(&id).unwrap()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn alive_ids(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.alive)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn kill(&mut self, id: NodeId) {
        self.node_mut(id).alive = false;
    }

    /// Drop every message to and from `id` for `ms`.
    pub fn isolate(&mut self, id: NodeId, ms: Millis) {
        let until = self.now + ms;
        self.node_mut(id).isolated_until = until;
    }

    /// The node keeps its connections but answers nothing.
    pub fn silence(&mut self, id: NodeId) {
        self.node_mut(id).silenced = true;
    }

    /// Split the network: nodes in different groups cannot reach each other. Nodes in no group
    /// reach nobody.
    pub fn partition(&mut self, groups: &[&[NodeId]]) {
        self.groups = Some(groups.iter().map(|g| g.iter().copied().collect()).collect());
    }

    pub fn heal(&mut self) {
        self.groups = None;
    }

    pub fn liveness(&self, observer: NodeId, subject: NodeId) -> Option<Liveness> {
        self.nodes[&observer].swim.liveness(subject)
    }

    pub fn incarnation(&self, id: NodeId) -> u64 {
        self.nodes[&id].swim.me().incarnation
    }

    pub fn known_incarnation(&self, observer: NodeId, subject: NodeId) -> Option<u64> {
        self.nodes[&observer]
            .swim
            .member(subject)
            .map(|m| m.incarnation)
    }

    pub fn messages_sent(&self, id: NodeId) -> u64 {
        self.nodes[&id].sent
    }

    pub fn data_path_failure(&mut self, observer: NodeId, subject: NodeId) {
        let now = self.now;
        self.node_mut(observer).swim.evidence(now, subject);
    }

    pub fn set_leader_hints(&mut self, id: NodeId, hints: Bytes) {
        self.node_mut(id).swim.set_leader_hints(hints);
    }

    pub fn epoch(&self, id: NodeId) -> Arc<Epoch> {
        self.nodes[&id].host.epoch()
    }

    pub fn cluster_id(&self, id: NodeId) -> ClusterId {
        self.nodes[&id].host.history.cluster_id()
    }

    pub fn hlc(&self, id: NodeId) -> u64 {
        self.nodes[&id].host.hlc()
    }

    fn up(&self, id: NodeId) -> bool {
        self.nodes
            .get(&id)
            .is_some_and(|n| n.alive && n.isolated_until <= self.now)
    }

    /// True when a message from `a` reaches `b` now.
    pub fn reachable(&self, a: NodeId, b: NodeId) -> bool {
        if !self.up(a) || !self.up(b) {
            return false;
        }
        match &self.groups {
            None => true,
            Some(groups) => groups.iter().any(|g| g.contains(&a) && g.contains(&b)),
        }
    }

    fn schedule(&mut self, at: Millis, d: Delivery) {
        self.seq += 1;
        self.queue.insert((at, self.seq), d);
    }

    fn latency(&mut self) -> Millis {
        self.rng.u64(1..=5)
    }

    /// Send the actions of `from`.
    pub fn send(&mut self, from: NodeId, actions: Vec<Action>) {
        for a in actions {
            let node = self.node_mut(from);
            node.sent += 1;
            node.waiting.insert(a.token);
            let lat = self.latency();
            let now = self.now;
            self.schedule(
                now + lat,
                Delivery::Request {
                    from,
                    to: a.to,
                    token: a.token,
                    msg: a.msg,
                },
            );
            self.schedule(
                now + a.timeout_ms.max(1),
                Delivery::Timeout {
                    to: from,
                    token: a.token,
                    target: a.to,
                },
            );
        }
    }

    /// Advance the time by `ms` in steps of [`STEP_MS`].
    pub fn run_ms(&mut self, ms: Millis) {
        let end = self.now + ms;
        while self.now < end {
            let step_end = self.now + STEP_MS;
            self.deliver_until(step_end);
            self.now = step_end;
            for id in self.alive_ids() {
                let now = self.now;
                let node = self.node_mut(id);
                node.host.now = now;
                let actions = node.swim.tick(now, &mut node.host);
                self.send(id, actions);
            }
            self.after_tick();
        }
    }

    /// A hook for later parts of the simulator, such as the lease.
    fn after_tick(&mut self) {}

    fn deliver_until(&mut self, end: Millis) {
        while let Some(entry) = self.queue.first_entry() {
            let (at, _) = *entry.key();
            if at > end {
                break;
            }
            let d = entry.remove();
            self.now = self.now.max(at);
            self.deliver(d);
        }
    }

    fn deliver(&mut self, d: Delivery) {
        let now = self.now;
        match d {
            Delivery::Request {
                from,
                to,
                token,
                msg,
            } => {
                let Some(&target) = self.by_addr.get(&to) else {
                    return;
                };
                if !self.reachable(from, target) || self.nodes[&target].silenced {
                    return;
                }
                let node = self.node_mut(target);
                node.host.now = now;
                let handled = node.swim.handle(now, to, msg, &mut node.host);
                match handled.relay {
                    Some(relay) => {
                        self.node_mut(target)
                            .relays
                            .insert(relay.token, (from, token));
                        self.send(target, vec![relay]);
                    }
                    None => {
                        let lat = self.latency();
                        self.schedule(
                            now + lat,
                            Delivery::Reply {
                                from: target,
                                to: from,
                                token,
                                msg: handled.reply,
                            },
                        );
                    }
                }
            }
            Delivery::Reply {
                from,
                to,
                token,
                msg,
            } => {
                if !self.reachable(from, to) {
                    return;
                }
                self.resolve(to, token, Outcome::Reply(Box::new(msg)));
            }
            Delivery::Timeout { to, token, target } => {
                if !self.up(to) {
                    return;
                }
                let outcome = match self.by_addr.get(&target).copied() {
                    Some(t) if self.reachable(to, t) && self.nodes[&t].silenced => {
                        Outcome::Silent {
                            in_cluster: self.nodes[&t].host.epoch().id().generation >= 1,
                        }
                    }
                    Some(t) if self.reachable(to, t) => Outcome::Silent { in_cluster: false },
                    _ => Outcome::Unreachable,
                };
                self.resolve(to, token, outcome);
            }
        }
    }

    fn resolve(&mut self, at: NodeId, token: u64, outcome: Outcome) {
        let now = self.now;
        if !self.up(at) {
            return;
        }
        let node = self.node_mut(at);
        if !node.waiting.remove(&token) {
            return;
        }
        node.host.now = now;
        if let Some((requester, their_token)) = node.relays.remove(&token) {
            let reply = node.swim.relayed(now, token, outcome, &mut node.host);
            let lat = self.latency();
            self.schedule(
                now + lat,
                Delivery::Reply {
                    from: at,
                    to: requester,
                    token: their_token,
                    msg: reply,
                },
            );
        } else {
            let actions = node.swim.outcome(now, token, outcome, &mut node.host);
            self.send(at, actions);
        }
    }
}
