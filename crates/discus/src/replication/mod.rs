//! Replication, read repair, hinted handoff, handoff, and promotion reconciliation (spec
//! sections 5.2, 8.2, and 9).
//!
//! The primary acknowledges a write once it applied it locally (`W = 1`). Then
//! [`Replicator::written`] queues the keys for every other current owner. One sender task per
//! peer drains its queue in batches on the bulk lane. A replica applies an entry only if its
//! version is newer, so replication is idempotent and order-tolerant.
//!
//! A write that cannot reach a peer becomes a hint: after a failed batch, while the peer is
//! down, and when its queue overflows. The sender replays the hints in HLC order when a probe
//! succeeds and the epoch lists the peer as `NORMAL`, or when a lagging queue drains.

pub mod digest;
mod handoff;
mod hints;
mod queue;
pub mod repair;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::{ClusterId, Epoch, NodeId, Version};
use discus_store::{refill_bucket, Entry, Store};
use discus_transport::{Lane, PeerInfo, TransportError};
use discus_wire::{caps, Batch, BatchAck, Message, TraceContext};
use parking_lot::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::{Config, Node};
use hints::{HintLimits, HintStore};
use queue::{PeerQueue, Pushed};

/// The most bytes of entries in one replication batch.
const BATCH_BYTES: u64 = 4 << 20;
/// Hints in one `HINT_REPLAY` batch.
const REPLAY_BATCH: usize = 256;
/// An incoming handoff this old asks its sources whether they are idle.
const PROBE_AFTER: Duration = Duration::from_secs(2);
/// How often the replicator reports its gauges and ages out hints.
const TICK: Duration = Duration::from_secs(1);

/// The replication settings from the flags.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub queue_keys: usize,
    pub batch_keys: usize,
    pub timeout: Duration,
    pub retry: Duration,
    pub rf: usize,
    pub tombstone_ttl: Duration,
    pub hints: HintLimits,
    pub handoff: bool,
    pub handoff_rate: u64,
    pub handoff_timeout: Duration,
}

impl Settings {
    pub fn from_config(c: &Config) -> Settings {
        Settings {
            queue_keys: c.replication_queue_keys.max(1),
            batch_keys: c.replication_batch_keys.max(1),
            timeout: Duration::from_millis(c.replication_timeout_ms),
            retry: Duration::from_millis(c.replication_retry_ms.max(1)),
            rf: c.replication_factor,
            tombstone_ttl: Duration::from_millis(c.tombstone_ttl_ms),
            hints: HintLimits {
                max_entries: c.hint_max_entries.max(1),
                max_bytes: c.hint_max_bytes,
                global_bytes: c.max_memory / 100 * c.hint_memory_percent,
                max_age: Duration::from_millis(c.tombstone_ttl_ms),
            },
            #[cfg(feature = "testing")]
            handoff: c.handoff,
            #[cfg(not(feature = "testing"))]
            handoff: true,
            handoff_rate: c.handoff_rate_bytes,
            handoff_timeout: Duration::from_millis(c.handoff_timeout_ms),
        }
    }
}

pub struct Replicator {
    me: Weak<Replicator>,
    node: Weak<Node>,
    id: NodeId,
    store: Arc<Store>,
    settings: Settings,
    peers: RwLock<HashMap<NodeId, Arc<PeerQueue>>>,
    hints: Mutex<HintStore>,
    /// The hint bytes charged to the store budget.
    charged: AtomicU64,
    /// The last message from each peer, to measure an absence on rejoin.
    contacts: Mutex<HashMap<NodeId, Instant>>,
    started: Instant,
    stop: CancellationToken,
    /// Lock order: the epoch lock comes first. Code that holds this lock never reads the
    /// epochs and never drops an `EpochPin`, because a pin takes the epoch lock on drop.
    handoff: Mutex<handoff::State>,
    scan_wake: tokio::sync::Notify,
    scans: AtomicU64,
    ready: AtomicBool,
    probing: AtomicBool,
    /// The newest epoch generation of a batch that a peer sent, recorded before its entries
    /// apply. `handoff::drop_lost` keeps every key once it passes the current generation.
    pub(crate) peer_generation: AtomicU64,
    /// Adoptions since start, counted under the epoch lock in `register`. The drop of moved
    /// keys keeps every key once this changes during its pass.
    pub(crate) adoptions: AtomicU64,
}

impl Replicator {
    pub fn new(node: &Arc<Node>) -> Arc<Replicator> {
        let settings = Settings::from_config(&node.config);
        let replicator = Arc::new_cyclic(|me| Replicator {
            me: me.clone(),
            node: Arc::downgrade(node),
            id: node.id,
            store: node.store.clone(),
            settings,
            peers: RwLock::new(HashMap::new()),
            hints: Mutex::new(HintStore::new(settings.hints)),
            charged: AtomicU64::new(0),
            contacts: Mutex::new(HashMap::new()),
            started: Instant::now(),
            stop: CancellationToken::new(),
            handoff: Mutex::new(handoff::State::default()),
            scan_wake: tokio::sync::Notify::new(),
            scans: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            probing: AtomicBool::new(false),
            peer_generation: AtomicU64::new(0),
            adoptions: AtomicU64::new(0),
        });
        // Under memory pressure the store frees the oldest hints after its normal entries
        // (spec section 11).
        let weak = Arc::downgrade(&replicator);
        node.store.set_pressure(Box::new(move |want| {
            weak.upgrade().map_or(0, |r| r.free_hints(want))
        }));
        replicator
    }

    /// Run the gauge reporter and the rejoin listener until `stop`. Sender tasks start with
    /// their first key.
    pub fn spawn(self: &Arc<Self>, stop: CancellationToken) {
        let me = self.clone();
        let s = stop.clone();
        tokio::spawn(async move { me.scan_loop(s).await });
        let me = self.clone();
        discus_store::every(TICK, stop.clone(), move || {
            me.tick();
            std::future::ready(())
        });
        let (mine, s) = (self.stop.clone(), stop.clone());
        tokio::spawn(async move {
            s.cancelled().await;
            mine.cancel();
        });
        let Some(membership) = self.node.upgrade().and_then(|n| n.membership().cloned()) else {
            return;
        };
        let me = self.clone();
        let mut events = membership.subscribe();
        tokio::spawn(async move {
            use crate::membership::Event;
            use tokio::sync::broadcast::error::RecvError;
            loop {
                let event = tokio::select! {
                    _ = stop.cancelled() => return,
                    e = events.recv() => e,
                };
                match event {
                    Ok(Event::Core(discus_membership::Event::Rejoining { .. })) => {
                        me.rejoining(me.absence());
                    }
                    Ok(Event::Core(discus_membership::Event::ClusterLost { .. })) => me.flush(),
                    Ok(_) | Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => return,
                }
            }
        });
    }

    fn tick(&self) {
        let metrics = discus_telemetry::global();
        metrics.tombstones(self.store.tombstones() as u64);
        for q in self.peers.read().values() {
            if let Some(label) = &q.label {
                metrics.replication_depth(label, q.len() as u64);
            }
        }
        let evicted = self.hints.lock().expire(Instant::now());
        self.count_evicted(&evicted);
        self.sync_charge();
        if let Some(node) = self.node.upgrade() {
            let current = node.epochs().current().clone();
            self.prune_handoffs(&current);
            self.probe_sources(&node);
        }
    }

    /// Ask the sources of incoming handoffs older than [`PROBE_AFTER`] whether they are idle. A
    /// source that adopted the epoch of the change and has no scan left will never send a done
    /// batch, for example after it gave up on this node, so it no longer counts.
    fn probe_sources(&self, node: &Arc<Node>) {
        let waiting: Vec<(NodeId, u64)> = {
            let st = self.handoff.lock();
            st.incoming
                .iter()
                .filter(|inc| inc.started.elapsed() >= PROBE_AFTER)
                .flat_map(|inc| inc.sources.iter().map(|s| (*s, inc.new.generation)))
                .collect()
        };
        if waiting.is_empty() || self.probing.swap(true, Ordering::AcqRel) {
            return;
        }
        let (Some(me), node) = (self.me.upgrade(), node.clone()) else {
            return;
        };
        tokio::spawn(async move {
            for (source, generation) in waiting {
                let probe = Message::Handoff(Batch::new(node.epoch_id()));
                if let Ok(Message::BatchAck(a)) = me.send(&node, source, probe).await {
                    if a.idle && a.epoch.generation >= generation {
                        let finished = me.handoff.lock().done(source, a.epoch);
                        drop(finished);
                    }
                }
            }
            me.probing.store(false, Ordering::Release);
        });
    }

    /// Register the handoffs of an adoption from `old` to `new`. `Node::adopt_epoch` calls
    /// this under the epoch lock, and `pin` pins `old` without taking the lock again.
    pub fn register(
        &self,
        old: &Arc<Epoch>,
        new: &Arc<Epoch>,
        mut pin: impl FnMut() -> Option<crate::node::EpochPin>,
    ) {
        self.adoptions.fetch_add(1, Ordering::SeqCst);
        if !self.settings.handoff {
            return;
        }
        let (outgoing, incoming) = handoff::changes(old, new, self.id, self.settings.rf);
        let mut st = self.handoff.lock();
        if let Some(gained) = outgoing {
            if let Some(pin) = pin() {
                st.pending.push(handoff::Outgoing {
                    old: old.clone(),
                    gained,
                    _pin: pin,
                });
                self.scan_wake.notify_one();
            }
        }
        if let Some(sources) = incoming {
            if let Some(pin) = pin() {
                st.expect(handoff::Incoming {
                    old: old.clone(),
                    new: new.id(),
                    sources,
                    started: Instant::now(),
                    _pin: pin,
                });
            }
        }
    }

    /// Forget every handoff, after the history was replaced.
    pub fn reset_handoffs(&self) {
        let dropped = std::mem::take(&mut *self.handoff.lock());
        drop(dropped);
    }

    fn prune_handoffs(&self, current: &Epoch) {
        let finished = self
            .handoff
            .lock()
            .prune(current, self.settings.handoff_timeout);
        drop(finished);
    }

    /// An incoming handoff is incomplete, so previous-epoch fallback is on.
    pub fn handoff_pending(&self) -> bool {
        !self.handoff.lock().incoming.is_empty()
    }

    /// A handoff scan runs or waits.
    pub fn scanning(&self) -> bool {
        let st = self.handoff.lock();
        st.scanning || !st.pending.is_empty()
    }

    /// Handoff scans finished since start.
    pub fn scans(&self) -> u64 {
        self.scans.load(Ordering::Relaxed)
    }

    /// The node reached `NORMAL` once with no incoming handoff left (spec section 15.2). It
    /// latches, so a later join of another node never makes this one unready.
    pub fn store_ready(&self) -> bool {
        if self.ready.load(Ordering::Acquire) {
            return true;
        }
        let Some(node) = self.node.upgrade() else {
            return false;
        };
        if node.reached_normal() && !self.handoff_pending() {
            self.ready.store(true, Ordering::Release);
            return true;
        }
        false
    }

    /// The previous owners of `key` to ask on a miss while a handoff toward this node is
    /// incomplete: the old owners that are not current owners, that the current epoch still
    /// lists, and that have not finished their part. `owners` are the current owners of `key`.
    pub fn fallback_sources(&self, node: &Node, key: &[u8], owners: &[NodeId]) -> Vec<NodeId> {
        if self.handoff.lock().incoming.is_empty() {
            return Vec::new();
        }
        // The epoch lock comes before the handoff lock, as in `register`.
        let current = node.epochs().current().clone();
        let st = self.handoff.lock();
        let mut out = Vec::new();
        for inc in &st.incoming {
            for o in inc.old.owners(key, self.settings.rf) {
                if !owners.contains(&o)
                    && inc.sources.contains(&o)
                    && current.member(o).is_some()
                    && !out.contains(&o)
                {
                    out.push(o);
                }
            }
        }
        out
    }

    async fn scan_loop(self: Arc<Self>, stop: CancellationToken) {
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = self.scan_wake.notified() => {}
            }
            let changes = {
                let mut st = self.handoff.lock();
                if st.pending.is_empty() {
                    continue;
                }
                st.scanning = true;
                std::mem::take(&mut st.pending)
            };
            let Some(node) = self.node.upgrade() else {
                return;
            };
            let to = node.epochs().current().clone();
            let limits = handoff::Limits {
                rate_bytes: self.settings.handoff_rate,
                rf: self.settings.rf,
            };
            let watermark = node.hlc.current();
            if handoff::scan(&node, &changes, to.clone(), limits).await {
                let dropped = handoff::drop_lost(&node, &to, watermark, limits).await;
                tracing::info!(dropped, "dropped the keys that moved to other owners");
            }
            self.scans.fetch_add(1, Ordering::Relaxed);
            drop(changes);
            let more = {
                let mut st = self.handoff.lock();
                st.scanning = false;
                !st.pending.is_empty()
            };
            if more {
                self.scan_wake.notify_one();
            }
        }
    }

    /// Wait until the node is out of placement, its scans are done, and its queues are empty.
    /// A draining node calls this within its shutdown deadline (spec section 15.3).
    pub async fn idle(&self) {
        loop {
            let Some(node) = self.node.upgrade() else {
                return;
            };
            let placed = node.membership().is_some() && node.epochs().current().is_normal(self.id);
            let queued = self
                .peers
                .read()
                .values()
                .any(|q| !q.is_empty() && !q.is_down());
            if !placed && !self.scanning() && !queued {
                return;
            }
            drop(node);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Keep the store charge equal to the hint bytes.
    fn sync_charge(&self) {
        let now = self.hints.lock().bytes();
        let before = self.charged.swap(now, Ordering::Relaxed);
        if now > before {
            self.store.charge(now - before);
        } else {
            self.store.release(before - now);
        }
    }

    fn free_hints(&self, want: u64) -> u64 {
        let (freed, evicted) = match self.hints.try_lock() {
            Some(mut h) => h.free(want),
            None => return 0,
        };
        self.count_evicted(&evicted);
        self.sync_charge();
        freed
    }

    fn count_evicted(&self, evicted: &[(NodeId, u64)]) {
        for (peer, n) in evicted {
            if let Some(label) = self.label(*peer) {
                discus_telemetry::global().hint_evicted(&label, *n);
            }
        }
    }

    /// The metric label of `peer`, from its queue when it has one.
    fn label(&self, peer: NodeId) -> Option<discus_telemetry::attrs::NodeLabel> {
        match self.peers.read().get(&peer) {
            Some(q) => q.label.clone(),
            None => crate::telemetry::node_label(peer),
        }
    }

    /// Record a hint for the peer of `q` with the current entry of `key`.
    fn hint(&self, q: &PeerQueue, key: Bytes) {
        let Some(entry) = self.store.entry(&key) else {
            return;
        };
        let evicted = self.hints.lock().record(q.peer, key, entry, Instant::now());
        if let Some(label) = &q.label {
            discus_telemetry::global().hint_recorded(label);
        }
        self.count_evicted(&evicted);
        self.sync_charge();
    }

    /// Hints held for `target`.
    pub fn hints(&self, target: NodeId) -> usize {
        self.hints.lock().count(target)
    }

    /// Queue `key`, which a local write just changed, for every other current owner.
    pub fn written(&self, node: &Node, key: &Bytes) {
        self.queue_for(node.owners(key), key);
    }

    /// Queue `key` for every owner in `owners` but this node.
    fn queue_for(&self, owners: Vec<NodeId>, key: &Bytes) {
        for owner in owners {
            if owner == self.id {
                continue;
            }
            let q = self.queue(owner);
            match q.push(key) {
                Pushed::Queued => {}
                Pushed::Overflow(old) => self.hint(&q, old),
                Pushed::Down => self.hint(&q, key.clone()),
            }
        }
    }

    fn queue(&self, peer: NodeId) -> Arc<PeerQueue> {
        if let Some(q) = self.peers.read().get(&peer) {
            return q.clone();
        }
        let mut peers = self.peers.write();
        let q = peers.entry(peer).or_insert_with(|| {
            let q = Arc::new(PeerQueue::new(peer, self.settings.queue_keys));
            if let Some(me) = self.me.upgrade() {
                tokio::spawn(me.send_loop(q.clone()));
            }
            q
        });
        q.clone()
    }

    /// The queue to `peer` is down. False without a queue.
    pub fn peer_down(&self, peer: NodeId) -> bool {
        self.peers.read().get(&peer).is_some_and(|q| q.is_down())
    }

    /// Keys waiting for `peer`.
    pub fn depth(&self, peer: NodeId) -> usize {
        self.peers.read().get(&peer).map_or(0, |q| q.len())
    }

    /// No queue, batch in flight, or hint holds `key`: every replica acknowledged its latest
    /// write.
    pub fn settled(&self, key: &[u8]) -> bool {
        self.peers.read().values().all(|q| !q.holds(key)) && !self.hints.lock().holds(key)
    }

    /// The node adopted `new` after `old`, and `current` is its epoch now. Queues and hints stop
    /// carrying keys that the target no longer owns in `current`, and those keys go to the queues
    /// of their current owners instead (spec sections 5.1 and 9). A write that this node
    /// acknowledged while the epoch changed can be in no handoff scan, so it must not stay
    /// behind on this node, also when this node no longer owns the key. A peer that left
    /// `current` is `DEAD` or departed, so it loses its queue and hints. Only the promotion
    /// reconciliation uses the `old` to `new` step.
    pub fn on_epoch(&self, old: &Arc<Epoch>, new: &Arc<Epoch>, current: &Epoch) {
        let rf = self.settings.rf;
        let mut moved: Vec<Bytes> = Vec::new();
        self.peers.write().retain(|peer, q| {
            let member = current.member(*peer).is_some();
            q.retain(|k| {
                let keep = member && current.owners(k, rf).contains(peer);
                if !keep {
                    moved.push(k.clone());
                }
                keep
            });
            if !member {
                q.close();
            }
            member
        });
        {
            let mut hints = self.hints.lock();
            for target in hints.targets() {
                if current.member(target).is_none() {
                    moved.extend(hints.drop_target(target));
                }
            }
            hints.retain(|target, k| {
                let keep = current.owners(k, rf).contains(&target);
                if !keep {
                    moved.push(k.clone());
                }
                keep
            });
        }
        moved.sort_unstable();
        moved.dedup();
        for k in &moved {
            self.queue_for(current.owners(k, rf), k);
        }
        self.sync_charge();
        self.prune_handoffs(current);
        self.contacts
            .lock()
            .retain(|p, _| current.member(*p).is_some());
        if !digest::removed(old, new).is_empty() && new.member(self.id).is_some() {
            if let Some(node) = self.node.upgrade() {
                tokio::spawn(digest::reconcile(node, old.clone(), new.clone()));
            }
        }
    }

    /// Drop every queue and hint: after a flush or a lost merge.
    pub fn flush(&self) {
        for q in self.peers.read().values() {
            q.clear();
        }
        self.hints.lock().clear();
        self.sync_charge();
    }

    /// The time since the last message from a member of the current epoch.
    fn absence(&self) -> Duration {
        let Some(node) = self.node.upgrade() else {
            return Duration::ZERO;
        };
        let epoch = node.epochs().current().clone();
        let contacts = self.contacts.lock();
        let last = epoch
            .members()
            .iter()
            .filter(|m| m.id != self.id)
            .filter_map(|m| contacts.get(&m.id))
            .max()
            .copied()
            .unwrap_or(self.started);
        last.elapsed()
    }

    /// Note a message from `peer`.
    pub fn touch(&self, peer: NodeId) {
        self.contacts.lock().insert(peer, Instant::now());
    }

    /// The node rejoins after `away` without its cluster (spec sections 6.3 and 8.2). After at
    /// least the tombstone TTL, deletes it missed can be gone everywhere, so the store is
    /// cleared. After a shorter absence, only the shards marked `needs_refill` are cleared.
    pub fn rejoining(&self, away: Duration) {
        self.flush();
        if away >= self.settings.tombstone_ttl {
            tracing::warn!(
                ?away,
                "rejoining after the tombstone TTL, discarding the store"
            );
            self.store.clear();
            return;
        }
        let shards = self.store.needs_refill();
        tracing::info!(?away, shards = shards.len(), "rejoining, keeping the store");
        for shard in shards {
            self.store.clear_shard(shard);
        }
    }

    async fn send_loop(self: Arc<Self>, q: Arc<PeerQueue>) {
        loop {
            if q.is_closed() || self.stop.is_cancelled() {
                return;
            }
            let Some(node) = self.node.upgrade() else {
                return;
            };
            if q.is_down() {
                drop(node);
                tokio::select! {
                    _ = self.stop.cancelled() => return,
                    _ = tokio::time::sleep(self.settings.retry) => {}
                }
                let Some(node) = self.node.upgrade() else {
                    return;
                };
                if self.probe(&node, &q).await && self.replay(&node, q.peer).await {
                    q.set_up();
                }
                continue;
            }
            #[cfg(feature = "testing")]
            if node.faults.hold_replication.load(Ordering::Relaxed) {
                drop(node);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let keys = q.take(self.settings.batch_keys);
            if keys.is_empty() {
                drop(node);
                tokio::select! {
                    _ = self.stop.cancelled() => return,
                    _ = q.notify.notified() => {}
                }
                continue;
            }
            let mut batch = Batch::new(node.epoch_id());
            batch.entries = fill(&self.store, &q, keys);
            let started = Instant::now();
            match self.send(&node, q.peer, Message::Replicate(batch)).await {
                Ok(_) => {
                    q.acked();
                    self.touch(q.peer);
                    if let Some(label) = &q.label {
                        discus_telemetry::global().replication_lag(label, started.elapsed());
                    }
                    if q.take_drained() && !self.replay(&node, q.peer).await {
                        for k in q.failed() {
                            self.hint(&q, k);
                        }
                    }
                }
                Err(TransportError::PeerUnsupported(_)) => q.acked(),
                Err(e) => {
                    tracing::debug!(peer = %q.peer, error = %e, "a replication batch failed");
                    for k in q.failed() {
                        self.hint(&q, k);
                    }
                }
            }
        }
    }

    /// An empty batch tells whether the peer takes replication again.
    async fn probe(&self, node: &Arc<Node>, q: &PeerQueue) -> bool {
        let batch = Batch::new(node.epoch_id());
        self.send(node, q.peer, Message::Replicate(batch))
            .await
            .is_ok()
    }

    /// Replay the hints of `peer` in HLC order, one acknowledged batch at a time (spec section
    /// 9). The first batch names the refill buckets of lost hints. Only a peer that the epoch
    /// lists as `NORMAL` gets a replay. Returns false when a batch failed.
    async fn replay(&self, node: &Arc<Node>, peer: NodeId) -> bool {
        if !node.epochs().current().is_normal(peer) {
            return false;
        }
        let mut first = true;
        loop {
            let (entries, lost) = {
                let h = self.hints.lock();
                (h.batch(peer, REPLAY_BATCH), h.lost(peer))
            };
            if entries.is_empty() && (lost.buckets.is_empty() || !first) {
                return true;
            }
            let sent: Vec<(Bytes, Version)> = entries
                .iter()
                .map(|(k, e)| (k.clone(), e.version))
                .collect();
            let mut batch = Batch::new(node.epoch_id());
            batch.entries = entries;
            if first {
                batch.discard = lost.buckets.clone();
            }
            match self.send(node, peer, Message::HintReplay(batch)).await {
                Ok(_) => {
                    {
                        let mut h = self.hints.lock();
                        h.confirm(peer, &sent);
                        if first {
                            h.clear_lost(peer, &lost);
                        }
                    }
                    self.sync_charge();
                    if let Some(label) = crate::telemetry::node_label(peer) {
                        discus_telemetry::global().hint_replayed(&label, sent.len() as u64);
                    }
                    first = false;
                }
                Err(e) => {
                    tracing::debug!(%peer, error = %e, "a hint replay batch failed");
                    return false;
                }
            }
        }
    }

    /// Send one bulk message to `peer` and return its reply. A peer without the replication
    /// capability gets nothing.
    pub(crate) async fn send(
        &self,
        node: &Arc<Node>,
        peer: NodeId,
        message: Message,
    ) -> Result<Message, TransportError> {
        let link = node
            .link()
            .ok_or_else(|| TransportError::Connect("no transport".into()))?;
        let addr = node
            .epochs()
            .current()
            .member(peer)
            .map(|m| m.rpc_addr)
            .ok_or_else(|| TransportError::Connect(format!("node {peer} is not a member")))?;
        let code = message.type_code();
        let mut cluster = ClusterId::NIL;
        let build = |info: &PeerInfo| {
            cluster = info.cluster_id;
            link.saw_peer_epoch(peer, info.cluster_id, info.epoch);
            match info.capabilities & caps::REPLICATION {
                0 => Err(TransportError::PeerUnsupported(code)),
                _ => Ok(message),
            }
        };
        let frame = link
            .transport
            .request_with(
                Lane::Bulk,
                peer,
                addr,
                build,
                TraceContext::default(),
                self.settings.timeout,
                true,
            )
            .await?;
        // An answered bulk message shows the peer is up (spec "Target health").
        link.note_forward(peer, true);
        // The ack carries the epoch that the peer adopted, so redirects need no forward first.
        if let Message::BatchAck(a) = &frame.message {
            link.saw_peer_epoch(peer, cluster, a.epoch);
        }
        Ok(frame.message)
    }

    /// Answer an inbound bulk message from `from`.
    pub async fn handle(&self, from: NodeId, message: Message) -> Message {
        let Some(node) = self.node.upgrade() else {
            return Message::Unsupported {
                message_type: message.type_code(),
            };
        };
        match message {
            Message::Replicate(b) => {
                self.saw_generation(&b);
                let applied = self.apply_entries(&node, b.entries);
                ack(&node, applied, Vec::new())
            }
            Message::HintReplay(b) => {
                self.saw_generation(&b);
                if !b.discard.is_empty() {
                    self.discard(&node, from, b.discard).await;
                }
                let applied = self.apply_entries(&node, b.entries);
                ack(&node, applied, Vec::new())
            }
            Message::Handoff(b)
                if b.entries.is_empty() && b.offers.is_empty() && b.done.is_none() =>
            {
                Message::BatchAck(BatchAck {
                    applied: 0,
                    want: Vec::new(),
                    epoch: node.epoch_id(),
                    idle: !self.scanning(),
                })
            }
            Message::Handoff(b) => {
                self.saw_generation(&b);
                // The entries apply first, so an offer of a key that the same batch carries
                // confirms that this node stored it. An expired entry still counts: it was newer
                // than the offer, so the offered copy must not come back.
                let applied = self.apply_entries(&node, b.entries);
                let want: Vec<Bytes> = b
                    .offers
                    .into_iter()
                    .filter(|(k, v)| self.store.stored_version(k).is_none_or(|local| local < *v))
                    .map(|(k, _)| k)
                    .collect();
                if let Some((_, to)) = b.done {
                    let finished = self.handoff.lock().done(from, to);
                    if !finished.is_empty() {
                        tracing::info!(source = %from, "an incoming handoff completed");
                    }
                    drop(finished);
                }
                ack(&node, applied, want)
            }
            Message::Fetch { keys } => {
                let mut out = Batch::new(node.epoch_id());
                for k in keys {
                    if let Some(e) = self.store.entry(&k) {
                        out.entries.push((k, e));
                    }
                }
                Message::Entries(out)
            }
            Message::Digest {
                from: old,
                to,
                sums,
            } => {
                // A node that skipped the sender's new epoch uses its own current one, which
                // has the same members once both nodes saw the same change.
                let (old, new) = {
                    let epochs = node.epochs();
                    let current = epochs.current();
                    let new = epochs
                        .get(to)
                        .or_else(|| (current.id() > to).then_some(current))
                        .cloned();
                    (epochs.get(old).cloned(), new)
                };
                let (Some(old), Some(new)) = (old, new) else {
                    // The epochs are not here yet. The sender tries again.
                    return ack(&node, 0, Vec::new());
                };
                let (store, rf, me) = (self.store.clone(), self.settings.rf, self.id);
                let entries = tokio::task::spawn_blocking(move || {
                    digest::answer(&store, &old, &new, rf, from, me, &sums)
                })
                .await
                .unwrap_or_default();
                let mut out = Batch::new(node.epoch_id());
                out.entries = entries;
                Message::Entries(out)
            }
            other => Message::Unsupported {
                message_type: other.type_code(),
            },
        }
    }

    fn saw_generation(&self, b: &Batch) {
        self.peer_generation
            .fetch_max(b.epoch.generation, Ordering::SeqCst);
    }

    /// Drop the keys of the lost refill buckets whose primary is `from`, before a replay
    /// refills what it can (spec section 9). The walk of the store runs off the async workers.
    async fn discard(&self, node: &Node, from: NodeId, buckets: Vec<u8>) {
        let (epoch, store) = (node.epochs().current().clone(), self.store.clone());
        let walk = move || {
            store.retain(|k| {
                !(buckets.contains(&refill_bucket(k)) && epoch.primary(k) == Some(from))
            })
        };
        if let Err(e) = tokio::task::spawn_blocking(walk).await {
            tracing::warn!(error = %e, "the discard of lost hint buckets failed");
        }
    }

    /// Apply entries that are newer than the local ones. An entry with a version too far ahead
    /// of the local clock is dropped (spec section 8.1).
    pub fn apply_entries(&self, node: &Node, entries: Vec<(Bytes, Entry)>) -> u32 {
        #[cfg(feature = "testing")]
        if node.faults.refuse_apply.load(Ordering::Relaxed) {
            return 0;
        }
        let mut applied = 0;
        for (k, e) in entries {
            if node.hlc.observe(e.version).is_err() {
                tracing::warn!("dropped a replicated entry too far ahead of the local clock");
                continue;
            }
            if self.store.apply(&k, e) {
                applied += 1;
            }
        }
        applied
    }
}

/// The current entries of `keys` for one replication batch, up to about [`BATCH_BYTES`]. The
/// keys past the limit go back to `q`, so acknowledging the batch does not drop them.
fn fill(store: &Store, q: &PeerQueue, keys: Vec<Bytes>) -> Vec<(Bytes, Entry)> {
    let mut entries = Vec::new();
    let mut bytes = 0;
    let mut keys = keys.into_iter();
    for k in keys.by_ref() {
        if let Some(e) = store.entry(&k) {
            bytes += discus_store::estimate_size(&k, &e);
            entries.push((k, e));
        }
        if bytes > BATCH_BYTES {
            break;
        }
    }
    q.untake(keys);
    entries
}

fn ack(node: &Node, applied: u32, want: Vec<Bytes>) -> Message {
    #[cfg(feature = "testing")]
    let epoch = node
        .faults
        .ack_epoch
        .lock()
        .unwrap_or_else(|| node.epoch_id());
    #[cfg(not(feature = "testing"))]
    let epoch = node.epoch_id();
    Message::BatchAck(BatchAck {
        applied,
        want,
        epoch,
        idle: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use discus_placement::ManualClock;
    use discus_store::{StoreConfig, Value};

    #[test]
    fn keys_past_the_batch_size_stay_queued() {
        let store = Store::new(StoreConfig::default(), Arc::new(ManualClock::new(1)));
        let q = PeerQueue::new(NodeId::from_u128(1), 1024);
        let big = Bytes::from(vec![b'x'; 100 << 10]);
        let keys: Vec<Bytes> = (0..64).map(|i| Bytes::from(format!("k{i}"))).collect();
        for k in &keys {
            store.write(k, Version::zero(), |slot| {
                slot.set(Some(Entry::new(Value::Str(big.clone()), None)));
            });
            q.push(k);
        }
        let entries = fill(&store, &q, q.take(keys.len()));
        assert!(entries.len() < keys.len());
        q.acked();
        let mut sent: Vec<Bytes> = entries.into_iter().map(|(k, _)| k).collect();
        while !q.is_empty() {
            sent.extend(
                fill(&store, &q, q.take(keys.len()))
                    .into_iter()
                    .map(|(k, _)| k),
            );
            q.acked();
        }
        assert_eq!(sent, keys);
    }
}
