//! State shared by every connection on a node.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use discus_placement::{
    hlc, Adopt, Clock, ClusterId, Epoch, EpochError, EpochHistory, EpochId, Hlc, Member,
    MemberState, NodeId, Version, Weight, NTP64,
};
use discus_resp::Protocol;
use discus_store::{Store, StoreConfig, SystemClock, HASH_SEED};
use parking_lot::{RwLock, RwLockReadGuard};

use crate::cluster::ClusterLink;
use crate::command::CommandError;
use crate::membership::Membership;
use crate::security::{ConnAuth, Security};
use crate::Config;

thread_local! {
    /// The last stamp that `Node::stamp` issued on this thread. Local execution reads it back to
    /// report the applied HLC of a command.
    static APPLIED: Cell<Option<Version>> = const { Cell::new(None) };
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
    pub started_at: Instant,
    pub store: Arc<Store>,
    pub security: Arc<Security>,
    next_conn_id: AtomicU64,
    connected: AtomicUsize,
    phase: AtomicU8,
    link: OnceLock<ClusterLink>,
    membership: OnceLock<Arc<Membership>>,
    drain: tokio_util::sync::CancellationToken,
}

/// Keeps one epoch in the history until it is dropped (spec section 5.1). Handoff (plan 4)
/// holds one for the old epoch of each ownership change.
pub struct EpochPin {
    epochs: std::sync::Weak<RwLock<EpochHistory>>,
    id: EpochId,
}

impl EpochPin {
    pub fn id(&self) -> EpochId {
        self.id
    }
}

impl Drop for EpochPin {
    fn drop(&mut self) {
        if let Some(epochs) = self.epochs.upgrade() {
            epochs.write().unpin(self.id);
        }
    }
}

impl std::fmt::Debug for EpochPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EpochPin({})", self.id)
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
                seed: HASH_SEED,
                tombstone_ttl_ms: config.tombstone_ttl_ms,
                tombstone_reserve: config.max_memory / 100 * config.tombstone_reserve_percent,
            },
            wall,
        );
        let id = config.node_id.unwrap_or_else(NodeId::random);
        let cluster_id = config.cluster_id.unwrap_or_else(ClusterId::mint);
        let me = Member {
            id,
            addr: config.cluster_listen,
            state: MemberState::Normal,
            weight: Weight::new(config.weight).unwrap_or_default(),
        };
        // A gossip node starts outside any cluster and joins or forms one (spec section 6.2).
        let first = if config.gossip_enabled() {
            discus_membership::provisional_epoch(id)
        } else {
            static_epoch(me, &config, cluster_id)
        };
        let epochs = EpochHistory::new(first);
        crate::telemetry::record_epoch(discus_telemetry::global(), epochs.current());
        Node {
            config,
            id,
            hlc: Hlc::new(id, hlc_clock),
            epochs: Arc::new(RwLock::new(epochs)),
            started_at: Instant::now(),
            store: Arc::new(store),
            security,
            next_conn_id: AtomicU64::new(1),
            connected: AtomicUsize::new(0),
            phase: AtomicU8::new(Phase::Starting as u8),
            link: OnceLock::new(),
            membership: OnceLock::new(),
            drain: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// Cancelled by `POST /admin/drain`. `Server::run` then drains as on SIGTERM (spec §15.3).
    pub fn drain_token(&self) -> tokio_util::sync::CancellationToken {
        self.drain.clone()
    }

    pub fn membership(&self) -> Option<&Arc<Membership>> {
        self.membership.get()
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

    /// The store needs no warm-up yet. Plan 4 adds a condition here.
    pub fn store_ready(&self) -> bool {
        true
    }

    /// The counter tier is settled (spec section 15.2). Plan 5 replaces this. Until then the tier
    /// does not exist, so it is always settled.
    pub fn counters_settled(&self) -> bool {
        true
    }

    /// Replace the history with `epoch`: after a join, or after a lost merge (spec section 6.3).
    /// Pins of the old history are dropped with it.
    pub fn reset_cluster(&self, epoch: Epoch) {
        let id = epoch.id();
        let mut epochs = self.epochs.write();
        *epochs = EpochHistory::new(epoch);
        tracing::info!(epoch = %id, cluster = %epochs.cluster_id(), "took a new cluster history");
        crate::telemetry::record_epoch(discus_telemetry::global(), epochs.current());
    }

    /// Keep epoch `id` in the history until the pin is dropped.
    pub fn pin_epoch(&self, id: EpochId) -> Result<EpochPin, EpochError> {
        self.epochs.write().pin(id)?;
        Ok(EpochPin {
            epochs: Arc::downgrade(&self.epochs),
            id,
        })
    }

    /// The node-to-node link, when the transport runs.
    pub fn link(&self) -> Option<&ClusterLink> {
        self.link.get()
    }

    /// Attach the node-to-node link. Only the first call has an effect.
    pub fn set_link(&self, link: ClusterLink) {
        let _ = self.link.set(link);
    }

    /// Adopt a newer epoch of this cluster. A stale epoch changes nothing.
    pub fn adopt_epoch(&self, epoch: Epoch) -> Result<Adopt, EpochError> {
        let id = epoch.id();
        let mut epochs = self.epochs.write();
        let result = epochs.adopt(epoch);
        if matches!(result, Ok(Adopt::Newer)) {
            tracing::info!(epoch = %id, "adopted a newer epoch");
            crate::telemetry::record_epoch(discus_telemetry::global(), epochs.current());
        }
        result
    }

    pub fn epoch_id(&self) -> EpochId {
        self.epochs.read().current().id()
    }

    /// The owner set of `key` in the previous epoch, if the history holds one.
    pub fn previous_owners(&self, key: &[u8]) -> Option<Vec<NodeId>> {
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
    pub fn owners(&self, key: &[u8]) -> Vec<NodeId> {
        self.epochs
            .read()
            .current()
            .owners(key, self.config.replication_factor)
    }

    /// A fresh HLC version for one write command. A clock pinned at its drift bound refuses, and
    /// the command returns `-TRYAGAIN`.
    pub fn stamp(&self) -> Result<Version, CommandError> {
        let stamp = self.hlc.tick().map_err(|_| CommandError::TryAgain)?;
        APPLIED.with(|a| a.set(Some(stamp)));
        Ok(stamp)
    }

    pub fn next_conn_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }
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
