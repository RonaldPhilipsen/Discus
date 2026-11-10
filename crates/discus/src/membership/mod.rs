//! SWIM membership over QUIC on the `discus-gossip` ALPN (spec section 6).
//!
//! The sans-IO core in `discus-membership` decides everything. This driver feeds it the time,
//! the inbound gossip, and the data-path evidence of the transport, sends its requests, runs
//! discovery and the lease renewal, and publishes its events.

pub mod discovery;
mod host;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_membership::{
    normal_count, Action, Event as CoreEvent, LeaseHolder, LeaseProvider, LeaseState, Millis,
    Outcome, Phase, Swim,
};
use discus_placement::{ClusterId, EpochId, MemberState, NodeId, Weight};
use discus_store::every;
use discus_transport::{GossipError, Transport};
use discus_wire::{Capacity, Gossip, Liveness, MemberInfo, Message};
use parking_lot::Mutex;
use tokio::sync::{broadcast, oneshot};
use tokio_util::sync::CancellationToken;

use crate::node::EpochPin;
use crate::Node;
use host::NodeHost;

/// Events that subscribers see.
#[derive(Debug, Clone)]
pub enum Event {
    Core(CoreEvent),
    /// The new epoch gives owner slots to `gained`. The pin keeps `old` in the history until
    /// every holder drops it, so a subscriber can compute the moved keys.
    OwnershipGained(OwnershipGained),
}

#[derive(Debug, Clone)]
pub struct OwnershipGained {
    pub old: EpochId,
    pub new: EpochId,
    pub gained: Vec<NodeId>,
    pub pin: Arc<EpochPin>,
}

const EVENT_CAPACITY: usize = 256;
const TICK: Duration = Duration::from_millis(10);
/// Time for each final `LEAVE` message.
const LEAVE_TIMEOUT: Duration = Duration::from_millis(200);

struct Inner {
    swim: Swim,
    host: NodeHost,
}

pub struct Membership {
    node: Weak<Node>,
    transport: Transport,
    lease: Arc<dyn LeaseProvider>,
    inner: Mutex<Inner>,
    start: Instant,
    events: broadcast::Sender<Event>,
    relays: Mutex<HashMap<u64, oneshot::Sender<Outcome>>>,
    reached_normal: AtomicBool,
    normal: Mutex<(usize, Instant)>,
    /// The cluster ID that this node mints if it wins the lease.
    candidate: ClusterId,
    advertise: SocketAddr,
    static_seeds: Vec<SocketAddr>,
    dns: Option<(String, u16)>,
    lease_renew: Duration,
    stop: CancellationToken,
}

impl Membership {
    pub fn new(
        node: &Arc<Node>,
        transport: Transport,
        lease: Arc<dyn LeaseProvider>,
        advertise: SocketAddr,
        rpc_advertise: SocketAddr,
    ) -> Arc<Membership> {
        let config = &node.config;
        let me = MemberInfo {
            id: node.id,
            addr: advertise,
            rpc_addr: rpc_advertise,
            state: MemberState::Joining,
            incarnation: 0,
            weight: Weight::new(config.weight).unwrap_or_default(),
            epoch: node.epoch_id(),
            #[cfg(feature = "testing")]
            capabilities: config.cluster_capabilities,
            #[cfg(not(feature = "testing"))]
            capabilities: discus_wire::caps::KNOWN,
            capacity: Capacity::default(),
            leader_hints: Bytes::new(),
        };
        let static_seeds = discovery::static_seeds(config);
        let mut swim = Swim::new(config.swim(), me);
        swim.set_seeds(static_seeds.clone());
        let host = NodeHost {
            node: Arc::downgrade(node),
            id: node.id,
            transport: transport.clone(),
            lease_cluster: None,
        };
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        Arc::new(Membership {
            node: Arc::downgrade(node),
            transport,
            lease,
            inner: Mutex::new(Inner { swim, host }),
            start: Instant::now(),
            events,
            relays: Mutex::new(HashMap::new()),
            reached_normal: AtomicBool::new(false),
            normal: Mutex::new((0, Instant::now())),
            candidate: config.cluster_id.unwrap_or_else(ClusterId::mint),
            advertise,
            static_seeds,
            dns: discovery::dns_target(config),
            lease_renew: Duration::from_millis(config.lease_renew_ms.max(1)),
            stop: CancellationToken::new(),
        })
    }

    fn now(&self) -> Millis {
        self.start.elapsed().as_millis() as Millis
    }

    /// Run the gossip loop, the evidence reader, the lease renewal, and discovery until `stop`
    /// or [`Membership::depart`].
    pub fn spawn(self: &Arc<Self>, stop: CancellationToken) {
        let stop = {
            let mine = self.stop.clone();
            tokio::spawn(async move {
                stop.cancelled().await;
                mine.cancel();
            });
            self.stop.clone()
        };
        tracing::info!(
            lease = self.lease.name(),
            advertise = %self.advertise,
            "gossip membership starting"
        );
        let me = self.clone();
        every(TICK, stop.clone(), move || {
            me.tick();
            std::future::ready(())
        });
        let me = self.clone();
        let s = stop.clone();
        tokio::spawn(async move { me.read_evidence(s).await });
        let me = self.clone();
        every(self.lease_renew, stop.clone(), move || {
            let me = me.clone();
            async move {
                if me.phase() != Phase::Seeking {
                    me.lease_call().await;
                }
            }
        });
        if let Some(target) = self.dns.clone() {
            let me = self.clone();
            let period = Duration::from_millis(self.inner.lock().swim.config().period_ms);
            every(period.max(Duration::from_secs(1)), stop, move || {
                let (me, target) = (me.clone(), target.clone());
                async move {
                    let mut seeds = me.static_seeds.clone();
                    seeds.extend(discovery::resolve(&target).await);
                    me.inner.lock().swim.set_seeds(seeds);
                }
            });
        }
    }

    fn tick(self: &Arc<Self>) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        let capacity = Capacity {
            used: node.store.used_memory(),
            max: node.config.max_memory,
        };
        let now = self.now();
        let actions = self.drive(|swim, host| {
            swim.set_capacity(capacity);
            swim.tick(now, host)
        });
        self.dispatch(actions);
        let count = normal_count(node.epochs().current());
        let mut normal = self.normal.lock();
        if normal.0 != count {
            *normal = (count, Instant::now());
        }
    }

    fn dispatch(self: &Arc<Self>, actions: Vec<Action>) {
        for action in actions {
            tokio::spawn(self.clone().send(action));
        }
    }

    async fn exchange(&self, action: Action) -> Outcome {
        let started = Instant::now();
        let timeout = Duration::from_millis(action.timeout_ms);
        let message = Message::Gossip(action.msg);
        match self
            .transport
            .gossip(action.to, action.expected, message, timeout)
            .await
        {
            Ok((info, Message::Gossip(reply))) => {
                if let Some(label) = crate::telemetry::node_label(info.node_id) {
                    discus_telemetry::global().gossip_rtt(&label, started.elapsed());
                }
                Outcome::Reply(Box::new(reply))
            }
            Ok(_) => Outcome::Unreachable,
            Err(GossipError::Silent(info)) => Outcome::Silent {
                in_cluster: info.epoch.generation >= 1,
            },
            Err(GossipError::Unreachable(_) | GossipError::Refused(_)) => Outcome::Unreachable,
        }
    }

    async fn send(self: Arc<Self>, action: Action) {
        let token = action.token;
        let outcome = self.exchange(action).await;
        if let Some(tx) = self.relays.lock().remove(&token) {
            let _ = tx.send(outcome);
            return;
        }
        let now = self.now();
        let actions = self.drive(|swim, host| swim.outcome(now, token, outcome, host));
        self.dispatch(actions);
    }

    /// Answer one inbound gossip message from `src`.
    pub async fn handle(self: &Arc<Self>, src: SocketAddr, msg: Gossip) -> Gossip {
        let now = self.now();
        let handled = self.drive(|swim, host| swim.handle(now, src, msg, host));
        let Some(relay) = handled.relay else {
            return handled.reply;
        };
        let (tx, rx) = oneshot::channel();
        let token = relay.token;
        let wait = Duration::from_millis(relay.timeout_ms) + Duration::from_millis(100);
        self.relays.lock().insert(token, tx);
        tokio::spawn(self.clone().send(relay));
        let outcome = match tokio::time::timeout(wait, rx).await {
            Ok(Ok(outcome)) => outcome,
            _ => {
                self.relays.lock().remove(&token);
                Outcome::Unreachable
            }
        };
        let now = self.now();
        let mut guard = self.inner.lock();
        let Inner { swim, host } = &mut *guard;
        swim.relayed(now, token, outcome, host)
    }

    async fn read_evidence(self: Arc<Self>, stop: CancellationToken) {
        use tokio::sync::broadcast::error::RecvError;
        let mut events = self.transport.events();
        loop {
            let event = tokio::select! {
                _ = stop.cancelled() => return,
                event = events.recv() => event,
            };
            match event {
                Ok(event) => {
                    if let Some(peer) = event.peer {
                        let now = self.now();
                        self.inner.lock().swim.evidence(now, peer);
                    }
                }
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return,
            }
        }
    }

    fn holder(&self) -> LeaseHolder {
        let cluster = self.inner.lock().swim.cluster_id();
        LeaseHolder {
            identity: self
                .node
                .upgrade()
                .map_or_else(String::new, |n| n.id.to_string()),
            address: self.advertise,
            cluster_id: if cluster == ClusterId::NIL {
                self.candidate
            } else {
                cluster
            },
        }
    }

    async fn lease_call(self: Arc<Self>) {
        let holder = self.holder();
        let result = self.lease.acquire_or_renew(&holder).await;
        if let Err(e) = &result {
            tracing::warn!(error = %e, "the cluster lease call failed");
        }
        let now = self.now();
        self.drive(|swim, host| {
            if let Ok(LeaseState::Held(h) | LeaseState::HeldByOther(h)) = &result {
                if self.lease.authoritative() {
                    host.lease_cluster = Some(h.cluster_id);
                }
            }
            swim.lease_result(now, result, host);
        });
    }

    /// Run `f` on the SWIM state under the lock, then publish the events that it raised.
    fn drive<R>(self: &Arc<Self>, f: impl FnOnce(&mut Swim, &mut NodeHost) -> R) -> R {
        let (r, events) = {
            let mut guard = self.inner.lock();
            let Inner { swim, host } = &mut *guard;
            let r = f(swim, host);
            (r, swim.take_events())
        };
        self.publish(events);
        r
    }

    fn publish(self: &Arc<Self>, events: Vec<CoreEvent>) {
        let node = self.node.upgrade();
        for event in events {
            match &event {
                CoreEvent::OwnershipGained { old, new, gained } => {
                    let pin = node.as_ref().and_then(|n| n.pin_epoch(*old).ok());
                    if let Some(pin) = pin {
                        let _ = self.events.send(Event::OwnershipGained(OwnershipGained {
                            old: *old,
                            new: *new,
                            gained: gained.clone(),
                            pin: Arc::new(pin),
                        }));
                    }
                    continue;
                }
                CoreEvent::LeaseWanted => {
                    tokio::spawn(self.clone().lease_call());
                }
                CoreEvent::ClusterLost { old, winner } => {
                    tracing::warn!(%old, %winner, "lost a cluster merge, discarding the store");
                    if let Some(n) = &node {
                        n.store.clear();
                    }
                }
                CoreEvent::StateChanged { state, incarnation } => {
                    tracing::info!(state = state.name(), incarnation, "member state changed");
                }
                CoreEvent::Rejoining { incarnation } => {
                    tracing::warn!(incarnation, "dropped from the epoch, joining again");
                }
                CoreEvent::MemberChanged {
                    id,
                    state,
                    liveness,
                } => {
                    tracing::info!(peer = %id, state = state.name(), ?liveness, "member changed");
                    if matches!(liveness, Liveness::Suspect | Liveness::Dead) {
                        if let Some(label) = crate::telemetry::node_label(*id) {
                            discus_telemetry::global()
                                .peer_error(&label, discus_telemetry::attrs::Status::Timeout);
                        }
                    }
                }
                CoreEvent::EpochAdopted { .. } => {}
            }
            let _ = self.events.send(Event::Core(event));
        }
        if self.is_normal() {
            self.reached_normal.store(true, Ordering::Release);
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// `NORMAL` in the current epoch, and not `LEAVING`.
    pub fn is_normal(&self) -> bool {
        let guard = self.inner.lock();
        guard.swim.is_normal(&guard.host)
    }

    /// The node was `NORMAL` at least once. The startup probe latches on it.
    pub fn reached_normal(&self) -> bool {
        self.reached_normal.load(Ordering::Acquire) || self.is_normal()
    }

    pub fn phase(&self) -> Phase {
        self.inner.lock().swim.phase()
    }

    pub fn local(&self) -> MemberInfo {
        self.inner.lock().swim.me().clone()
    }

    pub fn members(&self) -> Vec<(MemberInfo, Liveness)> {
        self.inner.lock().swim.members()
    }

    /// Publish the counter leader hints of this node (spec section 6.1). They spread with a new
    /// incarnation.
    pub fn set_leader_hints(&self, hints: Vec<u8>) {
        self.inner.lock().swim.set_leader_hints(Bytes::from(hints));
    }

    /// The `NORMAL` count of the current epoch, and when it last changed. The counter tier
    /// measures the dwell time of the up switch from it (spec section 10.3).
    pub fn normal_members(&self) -> (usize, Instant) {
        *self.normal.lock()
    }

    pub fn add_seed(&self, seed: SocketAddr) {
        self.inner.lock().swim.add_seed(seed);
    }

    /// Mark the node `LEAVING`. It leaves placement in the next epoch and keeps serving.
    pub fn drain(self: &Arc<Self>) {
        let now = self.now();
        self.drive(|swim, _| swim.leave(now));
    }

    /// Tell a few members that this node is gone, then stop gossip.
    pub async fn depart(self: &Arc<Self>) {
        let actions = {
            let mut guard = self.inner.lock();
            let Inner { swim, host } = &mut *guard;
            swim.depart(host)
        };
        let sends = actions.into_iter().map(|a| {
            let transport = self.transport.clone();
            async move {
                let _ = transport
                    .gossip(a.to, a.expected, Message::Gossip(a.msg), LEAVE_TIMEOUT)
                    .await;
            }
        });
        futures::future::join_all(sends).await;
        self.stop.cancel();
        tracing::info!("left gossip");
    }
}
