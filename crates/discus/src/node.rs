//! State shared by every connection on a node.

use std::cell::{Cell, RefCell};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arc_swap::ArcSwap;
use bytes::Bytes;
use discus_placement::{
    hlc, Adopt, Clock, ClusterId, Epoch, EpochError, EpochHistory, EpochId, Hlc, Member,
    MemberState, NodeId, Owners, Version, Weight, NTP64,
};
use discus_resp::{Protocol, Value};
use discus_store::{Slot, Store, StoreConfig, SystemClock};
use discus_telemetry::attrs::TopologyCommand;
use discus_transport::TransportError;
use parking_lot::{RwLock, RwLockReadGuard};

use crate::cluster::topology;
use crate::cluster::writeset::{WriteSet, WriteSetBudget, WriteSetLimits};
use crate::cluster::ClusterLink;
use crate::command::CommandError;
use crate::config::{client_host, default_client_addr};
use crate::counter::Counters;
use crate::lag::LagProbe;
use crate::membership::Membership;
use crate::replication::Replicator;
use crate::security::{ConnAuth, Security};
use crate::Config;

thread_local! {
    /// The last stamp that `Node::stamp` issued on this thread. Local execution reads it back to
    /// report the applied HLC of a command.
    static APPLIED: Cell<Option<Version>> = const { Cell::new(None) };
    /// The current epoch of a node as this thread last read it, with its generation.
    static EPOCH_CACHE: RefCell<Option<(u64, Arc<Epoch>)>> = const { RefCell::new(None) };
}

/// Where the node is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Serving,
    Leaving,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Serving => "serving",
            Phase::Leaving => "leaving",
        }
    }
}

/// The node. Shared as `Arc<Node>`.
pub struct Node {
    pub config: Config,
    /// This process's identity. A restart is a new node.
    pub id: NodeId,
    /// The clock that stamps every write.
    pub hlc: Hlc,
    epochs: Arc<RwLock<EpochHistory>>,
    /// The current epoch of `epochs`, for reads on the command path without the lock. It changes
    /// only under the write lock, after the work that a reader must see with the new epoch.
    current: ArcSwap<Epoch>,
    /// The generation of `current`, unique across all nodes of the process. A thread keeps its
    /// last read of `current` while the generation stays, so a lookup costs one atomic load.
    current_generation: AtomicU64,
    pub started_at: Instant,
    pub store: Arc<Store>,
    pub security: Arc<Security>,
    next_conn_id: AtomicU64,
    connected: AtomicUsize,
    phase: AtomicU8,
    link: OnceLock<ClusterLink>,
    membership: OnceLock<Arc<Membership>>,
    /// The client address that this node advertises. Set at construction when the flags and the
    /// static member list settle it, else by `Membership::new` once the cluster address is known.
    client_addr: OnceLock<SocketAddr>,
    lag: LagProbe,
    data: OnceLock<tokio::runtime::Handle>,
    /// The ordered consumer of [`Node::apply_epoch`] work on the data runtime.
    epoch_work: OnceLock<tokio::sync::mpsc::UnboundedSender<(Arc<Epoch>, Arc<Epoch>)>>,
    replicator: OnceLock<Arc<Replicator>>,
    counters: OnceLock<Arc<Counters>>,
    epoch_tx: tokio::sync::watch::Sender<EpochId>,
    write_budget: Arc<WriteSetBudget>,
    drain: tokio_util::sync::CancellationToken,
    topology: parking_lot::Mutex<TopologyCache>,
    /// Faults that tests inject.
    #[cfg(feature = "testing")]
    pub faults: Faults,
}

/// Faults that tests inject into one node.
#[cfg(feature = "testing")]
#[derive(Debug, Default)]
pub struct Faults {
    /// Delay inbound `REPLICATE`, `HINT_REPLAY`, and `HANDOFF` by this many milliseconds.
    pub stall_bulk_ms: AtomicU64,
    /// Never answer a forwarded command.
    pub stall_forwards: std::sync::atomic::AtomicBool,
    /// Delay inbound counter messages by this many milliseconds.
    pub stall_counter_ms: AtomicU64,
    /// The names of the threads that ran [`Node::apply_epoch`].
    pub epoch_work_threads: parking_lot::Mutex<Vec<String>>,
    /// Never answer an inbound message of any kind from these peers. Blocking both sides makes
    /// a partition.
    pub blocked: parking_lot::Mutex<std::collections::HashSet<NodeId>>,
    /// Commands that this node forwarded to another node.
    pub forwards: AtomicU64,
    /// `-MOVED` replies that this node returned.
    pub redirects: AtomicU64,
    /// The handoff scan waits before each page while this is set.
    pub hold_scan: std::sync::atomic::AtomicBool,
    /// A handoff scan waited on `hold_scan`.
    pub scan_held: std::sync::atomic::AtomicBool,
    /// The drop of moved keys waits before each page while this is set.
    pub hold_drop: std::sync::atomic::AtomicBool,
    /// A drop of moved keys waited on `hold_drop`.
    pub drop_held: std::sync::atomic::AtomicBool,
    /// The replication senders keep their queued keys while this is set.
    pub hold_replication: std::sync::atomic::AtomicBool,
    /// The refill after a hint replay waits before each page while this is set.
    pub hold_refill: std::sync::atomic::AtomicBool,
    /// A refill waited on `hold_refill`.
    pub refill_held: std::sync::atomic::AtomicBool,
    /// Bulk acknowledgements carry this epoch in place of the current one, as from a node that
    /// lags or runs ahead.
    pub ack_epoch: parking_lot::Mutex<Option<discus_placement::EpochId>>,
    /// Inbound replicated and handed-off entries are refused, as with clock drift.
    pub refuse_apply: std::sync::atomic::AtomicBool,
    /// Why the last drop of moved keys stopped early.
    pub drop_stopped: parking_lot::Mutex<Option<&'static str>>,
    /// The sweep of expired entries skips its runs while this is set.
    pub hold_sweep: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "testing")]
impl Faults {
    /// Wait while `hold` is set, and note in `held` that the wait happened.
    pub async fn wait(hold: &std::sync::atomic::AtomicBool, held: &std::sync::atomic::AtomicBool) {
        while hold.load(Ordering::Relaxed) {
            held.store(true, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// How long to hold an inbound message from `peer` before it runs. `Duration::MAX` means
    /// that it never gets an answer.
    pub fn delay(&self, peer: NodeId, m: &discus_wire::Message) -> Option<std::time::Duration> {
        use discus_wire::Message as M;
        let ms = |a: &AtomicU64| {
            Some(a.load(Ordering::Relaxed))
                .filter(|&n| n > 0)
                .map(std::time::Duration::from_millis)
        };
        if self.blocked.lock().contains(&peer) {
            return Some(std::time::Duration::MAX);
        }
        match m {
            M::Replicate(_) | M::HintReplay(_) | M::Handoff(_) => ms(&self.stall_bulk_ms),
            M::Forward(_) if self.stall_forwards.load(Ordering::Relaxed) => {
                Some(std::time::Duration::MAX)
            }
            M::RaftLeader(_)
            | M::RaftConfig(_)
            | M::RaftVote(_)
            | M::RaftAppend(_)
            | M::RaftSnapshot(_)
            | M::CounterSync(_)
            | M::CounterAck(_) => ms(&self.stall_counter_ms),
            _ => None,
        }
    }
}

/// Keeps one epoch in the history until it is dropped (spec section 5.1). Handoff holds one for
/// the old epoch of each ownership change.
pub struct EpochPin {
    epochs: std::sync::Weak<RwLock<EpochHistory>>,
    id: EpochId,
}

impl Drop for EpochPin {
    fn drop(&mut self) {
        if let Some(epochs) = self.epochs.upgrade() {
            epochs.write().unpin(self.id);
        }
    }
}

impl Node {
    pub fn new(config: Config, security: Arc<Security>) -> Node {
        Node::with_clocks(config, security, Arc::new(SystemClock), hlc::system_clock)
    }

    /// A node on the given clocks: `wall` for TTL and eviction, `hlc_clock` for the HLC. Tests
    /// pass both halves of one mock clock slot, so the two agree.
    pub fn with_clocks(
        config: Config,
        security: Arc<Security>,
        wall: Arc<dyn Clock>,
        hlc_clock: fn() -> NTP64,
    ) -> Node {
        let store = Store::new(
            StoreConfig {
                shards: config.shards,
                max_memory: config.max_memory,
                tombstone_ttl_ms: config.tombstone_ttl_ms,
                tombstone_reserve: config.max_memory / 100 * config.tombstone_reserve_percent,
            },
            wall,
        );
        let id = config.node_id.unwrap_or_else(NodeId::random);
        let cluster_id = config.cluster_id.unwrap_or_else(ClusterId::mint);
        // A static node takes its client address only from the flag, because its peers list the
        // same address and every node must build the same member records. Gossip nodes default
        // it in `Membership::new`.
        let client_addr = config.client_advertise;
        let me = Member {
            id,
            addr: config.cluster_listen,
            rpc_addr: config.cluster_listen,
            client_addr,
            client_host: host_of(&config, client_addr),
            state: MemberState::Normal,
            weight: Weight::new(config.weight).unwrap_or_default(),
        };
        // A gossip node starts outside any cluster and joins or forms one (spec section 6.2).
        let first = if config.gossip_enabled() {
            discus_membership::provisional_epoch(id)
        } else {
            static_epoch(me, &config, cluster_id)
        };
        first.table(config.replication_factor);
        let epochs = EpochHistory::new(first);
        crate::telemetry::record_epoch(discus_telemetry::global(), epochs.current());
        let (epoch_tx, _) = tokio::sync::watch::channel(epochs.current().id());
        let store = Arc::new(store);
        let write_budget = Arc::new(WriteSetBudget::new(
            config.max_memory / 100 * config.write_set_memory_percent,
            Some(store.clone()),
        ));
        Node {
            config,
            id,
            hlc: Hlc::new(id, hlc_clock),
            current: ArcSwap::new(epochs.current().clone()),
            current_generation: AtomicU64::new(next_generation()),
            epochs: Arc::new(RwLock::new(epochs)),
            started_at: Instant::now(),
            store,
            write_budget,
            security,
            next_conn_id: AtomicU64::new(1),
            connected: AtomicUsize::new(0),
            phase: AtomicU8::new(Phase::Starting as u8),
            link: OnceLock::new(),
            membership: OnceLock::new(),
            client_addr: client_addr.map(OnceLock::from).unwrap_or_default(),
            lag: LagProbe::new(),
            data: OnceLock::new(),
            epoch_work: OnceLock::new(),
            replicator: OnceLock::new(),
            counters: OnceLock::new(),
            epoch_tx,
            drain: tokio_util::sync::CancellationToken::new(),
            topology: parking_lot::Mutex::default(),
            #[cfg(feature = "testing")]
            faults: Faults::default(),
        }
    }

    /// Cancelled by `POST /admin/drain`. `Server::run` then drains as on SIGTERM (spec §15.3).
    pub fn drain_token(&self) -> tokio_util::sync::CancellationToken {
        self.drain.clone()
    }

    /// The data runtime. Work on the control lane that touches the store, or spawns tasks that
    /// do, runs here.
    ///
    /// # Panics
    ///
    /// Before `Server::bind_with` set it.
    pub fn data_handle(&self) -> &tokio::runtime::Handle {
        self.data
            .get()
            .expect("Server::bind_with sets the data runtime")
    }

    pub(crate) fn set_data_handle(&self, handle: tokio::runtime::Handle) {
        let _ = self.data.set(handle);
    }

    /// The lag probe of the data runtime.
    pub fn lag(&self) -> &LagProbe {
        &self.lag
    }

    pub fn membership(&self) -> Option<&Arc<Membership>> {
        self.membership.get()
    }

    /// The client address that this node advertises, when it is known.
    pub fn client_addr(&self) -> Option<SocketAddr> {
        self.client_addr.get().copied()
    }

    /// The host name that this node advertises: `--client-hostname` for the client address.
    pub fn client_host(&self) -> Option<String> {
        host_of(&self.config, self.client_addr())
    }

    /// The `--client-hostname` name for `addr`.
    pub(crate) fn host_for(&self, addr: SocketAddr) -> Option<String> {
        host_of(&self.config, Some(addr))
    }

    /// A topology reply from the current epoch. The node builds the reply value once per epoch,
    /// kind, and protocol and caches it. Each reply still encodes the cached value (spec "Epoch
    /// changes"). A node that is not `NORMAL` returns `-TRYAGAIN`, except for
    /// `CLUSTER INFO`, which then reports `cluster_state:fail` and is not cached.
    pub fn topology(
        &self,
        kind: TopologyCommand,
        protocol: Protocol,
    ) -> Result<Arc<Value>, CommandError> {
        let normal = self.is_normal();
        if !normal && kind != TopologyCommand::Info {
            return Err(CommandError::ClusterTryAgain);
        }
        let epoch = self.epochs().current().clone();
        let at = kind as usize * 2 + usize::from(protocol == Protocol::Resp3);
        if normal {
            let cache = self.topology.lock();
            if cache.epoch == Some(epoch.id()) {
                if let Some(reply) = &cache.replies[at] {
                    return Ok(reply.clone());
                }
            }
        }
        let rf = self.config.replication_factor;
        let reply = Arc::new(topology::build(
            kind,
            &epoch,
            rf,
            protocol,
            self.topology_responder(normal),
        )?);
        // A reply of an epoch that a newer one replaced in the meantime is not stored.
        if normal && self.epoch_id() == epoch.id() {
            let mut cache = self.topology.lock();
            if cache.epoch != Some(epoch.id()) {
                *cache = TopologyCache {
                    epoch: Some(epoch.id()),
                    ..TopologyCache::default()
                };
            }
            cache.replies[at] = Some(reply.clone());
        }
        Ok(reply)
    }

    pub(crate) fn topology_responder(&self, normal: bool) -> topology::Responder {
        topology::Responder {
            me: self.id,
            normal,
            tls: self.config.security.tls_mode == crate::security::TlsMode::Node,
        }
    }

    /// Settle the client address from the resolved cluster IP, unless a flag already did.
    pub(crate) fn resolve_client_addr(&self, cluster_ip: IpAddr) -> SocketAddr {
        *self.client_addr.get_or_init(|| {
            default_client_addr(cluster_ip, self.config.redirect_listen, self.config.listen)
        })
    }

    /// The target health of `id` (spec "Target health"): SWIM sees it `Alive`, or the node has no
    /// failure detector, and neither the last forward nor the replication queue to it failed.
    pub fn target_up(&self, id: NodeId) -> bool {
        let alive = self
            .membership()
            .is_none_or(|m| m.liveness(id) == Some(discus_wire::Liveness::Alive));
        alive
            && !self.link().is_some_and(|l| l.forward_down(id))
            && !self.replicator().is_some_and(|r| r.peer_down(id))
    }

    /// Attach the gossip membership. Only the first call has an effect.
    pub fn set_membership(&self, membership: Arc<Membership>) {
        let _ = self.membership.set(membership);
    }

    /// True when the node is `NORMAL` in its current epoch. Without gossip, always.
    pub fn is_normal(&self) -> bool {
        self.membership().is_none_or(|m| m.is_normal())
    }

    /// The node was `NORMAL` at least once. The startup probe latches on it.
    pub fn reached_normal(&self) -> bool {
        self.membership().is_none_or(|m| m.reached_normal())
    }

    /// The store holds its handoff: the node reached `NORMAL` with no incoming handoff left.
    /// A node without a transport needs no handoff.
    pub fn store_ready(&self) -> bool {
        self.replicator().is_none_or(|r| r.store_ready())
    }

    /// The counter tier is settled (spec section 15.2). A node without a tier has nothing to
    /// wait for.
    pub fn counters_settled(&self) -> bool {
        self.counters().is_none_or(|c| c.settled())
    }

    /// Replace the history with `epoch`: after a join, or after a lost merge (spec section 6.3).
    /// Pins of the old history are dropped with it.
    pub fn reset_cluster(&self, epoch: Epoch) {
        let id = epoch.id();
        epoch.table(self.config.replication_factor);
        let mut epochs = self.epochs.write();
        *epochs = EpochHistory::new(epoch);
        tracing::info!(epoch = %id, cluster = %epochs.cluster_id(), "took a new cluster history");
        crate::telemetry::record_epoch(discus_telemetry::global(), epochs.current());
        self.publish(epochs.current().clone());
        drop(epochs);
        if let Some(link) = self.link() {
            link.reset();
        }
        if let Some(r) = self.replicator() {
            r.reset_handoffs();
        }
        self.epoch_tx.send_replace(id);
    }

    /// The replicator, when the transport runs.
    pub fn replicator(&self) -> Option<&Arc<Replicator>> {
        self.replicator.get()
    }

    /// Attach the replicator. Only the first call has an effect.
    pub fn set_replicator(&self, replicator: Arc<Replicator>) {
        let _ = self.replicator.set(replicator);
    }

    /// The counter tier. Every node that `Server` binds has one, also without a transport.
    pub fn counters(&self) -> Option<&Arc<Counters>> {
        self.counters.get()
    }

    /// Attach the counter tier. Only the first call has an effect.
    pub fn set_counters(&self, counters: Arc<Counters>) {
        let _ = self.counters.set(counters);
    }

    /// A new write set for a client connection (spec section 7.4).
    pub fn new_write_set(&self) -> WriteSet {
        let c = &self.config;
        WriteSet::new(
            WriteSetLimits {
                keys: c.write_set_keys.max(1),
                bytes: c.write_set_bytes,
                max_age: std::time::Duration::from_millis(c.write_set_ttl_ms),
            },
            self.write_budget.clone(),
        )
    }

    /// Bytes that every write set of this node holds.
    pub fn write_set_bytes(&self) -> u64 {
        self.write_budget.used()
    }

    /// A strong counter key, by `--counter-prefix` (spec section 10). The prefix decides the
    /// path before the value is read.
    pub fn is_counter_key(&self, key: &[u8]) -> bool {
        key.starts_with(self.config.counter_prefix.as_bytes())
    }

    /// A key that reads from the primary only, by `--primary-read-prefix`. A counter key never
    /// matches, because the counter path takes it first.
    pub fn is_primary_read_key(&self, key: &[u8]) -> bool {
        !self.is_counter_key(key)
            && self
                .config
                .primary_read_prefix
                .iter()
                .any(|p| key.starts_with(p.as_bytes()))
    }

    /// Changes of the current epoch: every adoption and every reset of the history.
    pub fn subscribe_epoch(&self) -> tokio::sync::watch::Receiver<EpochId> {
        self.epoch_tx.subscribe()
    }

    /// The node-to-node link, when the transport runs.
    pub fn link(&self) -> Option<&ClusterLink> {
        self.link.get()
    }

    /// The QUIC address of `--cluster-listen`, when the transport runs. With gossip membership
    /// it serves gossip only.
    pub fn cluster_addr(&self) -> Option<SocketAddr> {
        match self.membership() {
            Some(m) => Some(m.gossip_transport().local_addr()),
            None => self.rpc_addr(),
        }
    }

    /// The QUIC address that serves node-to-node RPC, when the transport runs.
    pub fn rpc_addr(&self) -> Option<SocketAddr> {
        self.link().map(|l| l.transport.local_addr())
    }

    /// The link, the address, and the current epoch for a request to member `to`.
    pub(crate) fn peer(
        &self,
        to: NodeId,
    ) -> Result<(&ClusterLink, SocketAddr, Arc<Epoch>), String> {
        let link = self.link().ok_or("this node has no transport")?;
        let epoch = self.current_epoch();
        let addr = epoch
            .member(to)
            .map(|m| m.rpc_addr)
            .ok_or_else(|| format!("node {to} is not a member"))?;
        Ok((link, addr, epoch))
    }

    /// Send one request to member `to` on `lane` and return the reply. `build` makes the request
    /// from the `PeerInfo` of the connection. See [`discus_transport::Transport::request_with`].
    pub(crate) async fn request(
        &self,
        to: NodeId,
        lane: discus_transport::Lane,
        build: impl FnOnce(&discus_transport::PeerInfo) -> Result<discus_wire::Message, TransportError>,
        timeout: std::time::Duration,
        write: bool,
    ) -> Result<discus_wire::Message, TransportError> {
        let (link, addr, _) = self.peer(to).map_err(TransportError::Connect)?;
        let frame = link
            .transport
            .request_with(lane, to, addr, build, Default::default(), timeout, write)
            .await?;
        Ok(frame.message)
    }

    /// Attach the node-to-node link. Only the first call has an effect.
    pub fn set_link(&self, link: ClusterLink) {
        let _ = self.link.set(link);
    }

    /// Adopt a newer epoch of this cluster. A stale epoch changes nothing.
    pub fn adopt_epoch(&self, epoch: Epoch) -> Result<Adopt, EpochError> {
        let id = epoch.id();
        epoch.table(self.config.replication_factor);
        let mut epochs = self.epochs.write();
        let old = epochs.current().clone();
        let result = epochs.adopt(epoch);
        if !matches!(result, Ok(Adopt::Newer)) {
            return result;
        }
        tracing::info!(epoch = %id, "adopted a newer epoch");
        let new = epochs.current().clone();
        crate::telemetry::record_epoch(discus_telemetry::global(), &new);
        // The handoff registers under the lock, so no reader sees the new epoch without it.
        if let Some(r) = self.replicator() {
            let weak = Arc::downgrade(&self.epochs);
            r.register(&old, &new, || {
                epochs.pin(old.id()).ok()?;
                Some(EpochPin {
                    epochs: weak.clone(),
                    id: old.id(),
                })
            });
        }
        // Adoptions run on the control lane and on the data runtime, so the send happens under
        // the lock to keep the work in adoption order.
        self.publish(new.clone());
        let unsent = match self.epoch_work.get() {
            Some(tx) => tx.send((old, new)).err().map(|e| e.0),
            None => Some((old, new)),
        };
        drop(epochs);
        if let Some((old, new)) = unsent {
            self.apply_epoch(&old, &new);
        }
        self.epoch_tx.send_replace(id);
        result
    }

    /// The work of an adoption that may wait: it grows with the replication backlog.
    fn apply_epoch(&self, old: &Arc<Epoch>, new: &Arc<Epoch>) {
        #[cfg(feature = "testing")]
        self.faults.epoch_work_threads.lock().push(
            std::thread::current()
                .name()
                .unwrap_or_default()
                .to_string(),
        );
        // The work can run after later adoptions. Pruning against `new` would then drop the
        // queues and hints of a peer that joined in a later epoch, so it prunes against the
        // current epoch. Work from a cluster that a reset replaced has nothing left to do.
        let current = self.epochs().current().clone();
        if current.cluster_id() != new.cluster_id() {
            return;
        }
        if let Some(r) = self.replicator() {
            r.on_epoch(old, new, &current);
        }
        if let Some(link) = self.link() {
            link.forget_non_members(&current);
        }
    }

    /// Run the work of later adoptions on the current runtime, in adoption order, until `stop`.
    /// Adoptions on the control lane would otherwise run it on the lane. Without the consumer,
    /// or after `stop`, adoptions run it inline.
    pub fn spawn_epoch_work(self: &Arc<Self>, stop: tokio_util::sync::CancellationToken) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(Arc<Epoch>, Arc<Epoch>)>();
        if self.epoch_work.set(tx).is_err() {
            return;
        }
        let node = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let work = tokio::select! {
                    _ = stop.cancelled() => break,
                    work = rx.recv() => work,
                };
                let (Some((old, new)), Some(node)) = (work, node.upgrade()) else {
                    return;
                };
                node.apply_epoch(&old, &new);
            }
            // Later sends fail and run inline. Work queued before the close still runs here.
            rx.close();
            while let Ok((old, new)) = rx.try_recv() {
                if let Some(node) = node.upgrade() {
                    node.apply_epoch(&old, &new);
                }
            }
        });
    }

    pub fn epoch_id(&self) -> EpochId {
        self.with_current(Epoch::id)
    }

    /// The owner set of `key` in the previous epoch, if the history holds one.
    pub fn previous_owners(&self, key: &[u8]) -> Option<Owners> {
        self.epochs
            .read()
            .previous()
            .map(|e| e.owners(key, self.config.replication_factor))
    }

    /// The stamp that the last command on this thread took, and clear it.
    pub fn take_applied() -> Option<Version> {
        APPLIED.with(Cell::take)
    }

    /// Count a new client. The returned guard decrements the count when dropped.
    pub fn track_connection(self: &Arc<Self>) -> ConnectionGuard {
        self.connected.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard(self.clone())
    }

    pub fn connected_clients(&self) -> usize {
        self.connected.load(Ordering::Relaxed)
    }

    /// Connections accepted since start.
    pub fn total_connections(&self) -> u64 {
        self.next_conn_id.load(Ordering::Relaxed) - 1
    }

    pub fn phase(&self) -> Phase {
        match self.phase.load(Ordering::Acquire) {
            0 => Phase::Starting,
            1 => Phase::Serving,
            _ => Phase::Leaving,
        }
    }

    pub fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    /// The epoch history. The current epoch decides placement.
    pub fn epochs(&self) -> RwLockReadGuard<'_, EpochHistory> {
        self.epochs.read()
    }

    pub fn cluster_id(&self) -> ClusterId {
        self.epochs.read().cluster_id()
    }

    /// The owner set of `key` in the current epoch with the configured replication factor.
    pub fn owners(&self, key: &[u8]) -> Owners {
        self.with_current(|e| e.owners(key, self.config.replication_factor))
    }

    /// Run `f` on the current epoch. `f` must not call back into this function.
    fn with_current<R>(&self, f: impl FnOnce(&Epoch) -> R) -> R {
        let generation = self.current_generation.load(Ordering::Acquire);
        EPOCH_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            let cached = cache.as_ref().filter(|(g, _)| *g == generation);
            let epoch = match cached {
                Some((_, epoch)) => epoch,
                // A newer epoch than `generation` can arrive here. It is cached under the older
                // generation, so the next call reads it again.
                None => &cache.insert((generation, self.current.load_full())).1,
            };
            f(epoch)
        })
    }

    /// Make `epoch` the current epoch for readers without the lock. The caller holds the write
    /// lock of `epochs`.
    fn publish(&self, epoch: Arc<Epoch>) {
        self.current.store(epoch);
        self.current_generation
            .store(next_generation(), Ordering::Release);
    }

    /// The current epoch, without the lock of the history.
    pub fn current_epoch(&self) -> Arc<Epoch> {
        self.current.load_full()
    }

    /// A fresh HLC version for one write command. A clock pinned at its drift bound refuses, and
    /// the command returns `-TRYAGAIN`, also when the write would change nothing.
    pub fn stamp(&self) -> Result<Version, CommandError> {
        self.hlc.tick().map_err(|_| CommandError::TryAgain)
    }

    /// Run a store write with `stamp`. A write that changed its key records `stamp` as the
    /// applied stamp of the command and queues the key for the other owners (spec section 8.2).
    /// A write that changed nothing leaves no trace.
    pub fn write<R>(&self, key: &Bytes, stamp: Version, f: impl FnOnce(&mut Slot) -> R) -> R {
        let (result, changed) = self.store.write_changed(key, stamp, f);
        if changed {
            APPLIED.with(|a| a.set(Some(stamp)));
            if let Some(replicator) = self.replicator() {
                replicator.written(self, key);
            }
        }
        result
    }

    pub fn next_conn_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// The topology replies of one epoch, indexed by kind and protocol.
#[derive(Default)]
struct TopologyCache {
    epoch: Option<EpochId>,
    replies: [Option<Arc<Value>>; 8],
}

fn host_of(config: &Config, client_addr: Option<SocketAddr>) -> Option<String> {
    let template = config
        .client_hostname
        .as_deref()
        .filter(|t| !t.is_empty())?;
    Some(client_host(template, client_addr?.ip()))
}

/// The first epoch. With static peers, every member is `NORMAL` with weight 1 and the epoch is
/// `(1, lowest NodeId)`, so every node forms the same epoch. Alone, the node bootstraps `(1, self)`.
fn static_epoch(me: Member, config: &Config, cluster_id: ClusterId) -> Epoch {
    if config.peers.is_empty() {
        return Epoch::bootstrap(me, cluster_id);
    }
    let my_id = me.id;
    let mut members = vec![me];
    members.extend(config.peers.iter().map(|p| Member {
        // Config::validate requires the ID with static membership.
        id: p.id.unwrap_or(NodeId::NIL),
        addr: p.addr,
        rpc_addr: p.addr,
        client_addr: p.client_addr,
        client_host: host_of(config, p.client_addr),
        state: MemberState::Normal,
        weight: Weight::default(),
    }));
    let origin = members.iter().map(|m| m.id).min().unwrap_or(my_id);
    let id = EpochId {
        generation: discus_placement::FIRST_GENERATION,
        origin,
    };
    // Config::validate refuses duplicate IDs, so this only fails for a config that skipped it.
    Epoch::new(id, cluster_id, members.clone())
        .unwrap_or_else(|_| Epoch::bootstrap(members[0].clone(), cluster_id))
}

#[derive(Debug)]
pub struct ConnState {
    pub protocol: Protocol,
    pub auth: ConnAuth,
    pub name: Option<Bytes>,
    pub id: u64,
    /// Set by `QUIT`: close the connection after the reply is flushed.
    pub closing: bool,
    /// The round-robin position for reads over replicas.
    rr: u64,
    /// The recent writes of a client connection in a cluster. Internal execution has none.
    pub writes: Option<Arc<parking_lot::Mutex<WriteSet>>>,
    /// A connection of the redirect listener: keys that another node serves get `-MOVED`.
    pub redirect: bool,
    /// Set by `READONLY`, cleared by `READWRITE`. Only a redirect connection reads it.
    pub readonly: bool,
    /// The local address of the client socket.
    pub local_addr: Option<SocketAddr>,
}

impl ConnState {
    pub fn new(id: u64, auth: ConnAuth) -> ConnState {
        ConnState {
            protocol: Protocol::Resp2,
            auth,
            name: None,
            id,
            closing: false,
            rr: id,
            writes: None,
            redirect: false,
            readonly: false,
            local_addr: None,
        }
    }

    /// The next round-robin position. Each data command takes one.
    pub fn next_rr(&mut self) -> u64 {
        self.rr = self.rr.wrapping_add(1);
        self.rr
    }
}

/// Keeps the connected-clients count accurate for the life of one connection.
pub struct ConnectionGuard(Arc<Node>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connected.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A new epoch generation for `Node::current_generation`.
fn next_generation() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_client_hostname_means_none() {
        let addr = Some("10.1.2.3:6380".parse().unwrap());
        let empty = Config::parse_from(["discus", "--client-hostname", ""]).unwrap();
        assert_eq!(host_of(&empty, addr), None);
        let set = Config::parse_from(["discus", "--client-hostname", "{ip}.peers"]).unwrap();
        assert_eq!(host_of(&set, addr).as_deref(), Some("10-1-2-3.peers"));
    }
}
