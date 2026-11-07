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
use discus_placement::{Epoch, MemberState, NodeId, Version};
use discus_store::{refill_bucket, Entry, Store};
use discus_transport::{Lane, TransportError};
use discus_wire::{caps, Batch, BatchAck, Message, TraceContext};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{Config, Node};
use hints::{HintLimits, HintStore};
use queue::{PeerQueue, Pushed};

/// The most bytes of entries in one replication batch.
const BATCH_BYTES: u64 = 4 << 20;
/// Hints in one `HINT_REPLAY` batch.
const REPLAY_BATCH: usize = 256;
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
            handoff: c.handoff,
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
    peers: Mutex<HashMap<NodeId, Arc<PeerQueue>>>,
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
            peers: Mutex::new(HashMap::new()),
            hints: Mutex::new(HintStore::new(settings.hints)),
            charged: AtomicU64::new(0),
            contacts: Mutex::new(HashMap::new()),
            started: Instant::now(),
            stop: CancellationToken::new(),
            handoff: Mutex::new(handoff::State::default()),
            scan_wake: tokio::sync::Notify::new(),
            scans: AtomicU64::new(0),
            ready: AtomicBool::new(false),
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
        let s = stop.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(TICK);
            loop {
                tokio::select! {
                    _ = s.cancelled() => {
                        me.stop.cancel();
                        return;
                    }
                    _ = every.tick() => me.tick(),
                }
            }
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
        for (peer, q) in self.peers.lock().iter() {
            if let Some(label) = crate::telemetry::node_label(*peer) {
                metrics.replication_depth(&label, q.len() as u64);
            }
        }
        let evicted = self.hints.lock().expire(Instant::now());
        self.count_evicted(&evicted);
        self.sync_charge();
        if let Some(node) = self.node.upgrade() {
            let current = node.epochs().current().clone();
            self.prune_handoffs(&current);
        }
    }

    /// Register the handoffs of an adoption from `old` to `new`. `Node::adopt_epoch` calls
    /// this under the epoch lock, and `pin` pins `old` without taking the lock again.
    pub fn register(
        &self,
        old: &Arc<Epoch>,
        new: &Arc<Epoch>,
        mut pin: impl FnMut() -> Option<crate::node::EpochPin>,
    ) {
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
    /// lists, and that have not finished their part.
    pub fn fallback_sources(&self, key: &[u8], current: &Epoch) -> Vec<NodeId> {
        let owners = current.owners(key, self.settings.rf);
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
            handoff::scan(&node, &changes, to, limits).await;
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
            let placed = node.membership().is_some()
                && node
                    .epochs()
                    .current()
                    .member(self.id)
                    .is_some_and(|m| m.state == MemberState::Normal);
            let queued = self
                .peers
                .lock()
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
            if let Some(label) = crate::telemetry::node_label(*peer) {
                discus_telemetry::global().hint_evicted(&label, *n);
            }
        }
    }

    /// Record a hint for `peer` with the current entry of `key`.
    fn hint(&self, peer: NodeId, key: Bytes) {
        let Some(entry) = self.store.entry(&key) else {
            return;
        };
        let evicted = self.hints.lock().record(peer, key, entry, Instant::now());
        if let Some(label) = crate::telemetry::node_label(peer) {
            discus_telemetry::global().hint_recorded(&label);
        }
        self.count_evicted(&evicted);
        self.sync_charge();
    }

    /// Hints held for `target`.
    pub fn hints(&self, target: NodeId) -> usize {
        self.hints.lock().count(target)
    }

    /// Queue `keys`, which a local write just stamped, for every other current owner.
    pub fn written(&self, keys: &[Bytes]) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        let epoch = node.epochs().current().clone();
        for key in keys {
            for owner in epoch.owners(key, self.settings.rf) {
                if owner == self.id {
                    continue;
                }
                match self.queue(owner).push(key) {
                    Pushed::Queued => {}
                    Pushed::Overflow(old) => self.hint(owner, old),
                    Pushed::Down => self.hint(owner, key.clone()),
                }
            }
        }
    }

    fn queue(&self, peer: NodeId) -> Arc<PeerQueue> {
        let mut peers = self.peers.lock();
        if let Some(q) = peers.get(&peer) {
            return q.clone();
        }
        let q = Arc::new(PeerQueue::new(peer, self.settings.queue_keys));
        peers.insert(peer, q.clone());
        if let Some(me) = self.me.upgrade() {
            tokio::spawn(me.send_loop(q.clone()));
        }
        q
    }

    /// Keys waiting for `peer`.
    pub fn depth(&self, peer: NodeId) -> usize {
        self.peers.lock().get(&peer).map_or(0, |q| q.len())
    }

    /// No queue, batch in flight, or hint holds `key`: every replica acknowledged its latest
    /// write.
    pub fn settled(&self, key: &[u8]) -> bool {
        self.peers.lock().values().all(|q| !q.holds(key)) && !self.hints.lock().holds(key)
    }

    /// A newer epoch is current. Queues and hints stop carrying keys that the target no longer
    /// owns, and queues stop carrying keys that this node no longer owns (spec sections 5.1 and
    /// 9). A peer that left the epoch is `DEAD` or departed, so it loses its queue and hints.
    pub fn on_epoch(&self, old: &Arc<Epoch>, new: &Arc<Epoch>) {
        let rf = self.settings.rf;
        self.peers.lock().retain(|peer, q| {
            if new.member(*peer).is_none() {
                q.close();
                return false;
            }
            q.retain(|k| {
                let owners = new.owners(k, rf);
                owners.contains(peer) && owners.contains(&self.id)
            });
            true
        });
        {
            let mut hints = self.hints.lock();
            for target in hints.targets() {
                if new.member(target).is_none() {
                    hints.drop_target(target);
                }
            }
            hints.retain(|target, k| new.owners(k, rf).contains(&target));
        }
        self.sync_charge();
        self.prune_handoffs(new);
        if !digest::removed(old, new).is_empty() && new.member(self.id).is_some() {
            if let Some(node) = self.node.upgrade() {
                tokio::spawn(digest::reconcile(node, old.clone(), new.clone()));
            }
        }
    }

    /// Drop every queue and hint: after a flush or a lost merge.
    pub fn flush(&self) {
        for q in self.peers.lock().values() {
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
            let mut bytes = 0;
            for k in keys {
                if let Some(e) = self.store.entry(&k) {
                    bytes += discus_store::estimate_size(&k, &e);
                    batch.entries.push((k, e));
                }
                if bytes > BATCH_BYTES {
                    break;
                }
            }
            let started = Instant::now();
            match self.send(&node, q.peer, Message::Replicate(batch)).await {
                Ok(_) => {
                    q.acked();
                    self.touch(q.peer);
                    if let Some(label) = crate::telemetry::node_label(q.peer) {
                        discus_telemetry::global().replication_lag(&label, started.elapsed());
                    }
                    if q.take_drained() && !self.replay(&node, q.peer).await {
                        for k in q.failed() {
                            self.hint(q.peer, k);
                        }
                    }
                }
                Err(TransportError::PeerUnsupported(_)) => q.acked(),
                Err(e) => {
                    tracing::debug!(peer = %q.peer, error = %e, "a replication batch failed");
                    for k in q.failed() {
                        self.hint(q.peer, k);
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
        let normal = node
            .epochs()
            .current()
            .member(peer)
            .is_some_and(|m| m.state == MemberState::Normal);
        if !normal {
            return false;
        }
        let mut first = true;
        loop {
            let (entries, lost) = {
                let h = self.hints.lock();
                (h.batch(peer, REPLAY_BATCH), h.lost(peer))
            };
            if entries.is_empty() && (lost.is_empty() || !first) {
                return true;
            }
            let sent: Vec<(Bytes, Version)> = entries
                .iter()
                .map(|(k, e)| (k.clone(), e.version))
                .collect();
            let mut batch = Batch::new(node.epoch_id());
            batch.entries = entries;
            if first {
                batch.discard = lost;
            }
            match self.send(node, peer, Message::HintReplay(batch)).await {
                Ok(_) => {
                    {
                        let mut h = self.hints.lock();
                        h.confirm(peer, &sent);
                        if first {
                            h.clear_lost(peer);
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
            .map(|m| m.addr)
            .ok_or_else(|| TransportError::Connect(format!("node {peer} is not a member")))?;
        let info = link.transport.peer(peer, addr).await?;
        let code = message.type_code();
        if info.capabilities & caps::REPLICATION == 0 {
            return Err(TransportError::PeerUnsupported(code));
        }
        let frame = link
            .transport
            .request_on(
                Lane::Bulk,
                peer,
                addr,
                message,
                TraceContext::default(),
                self.settings.timeout,
                true,
            )
            .await?;
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
                let applied = self.apply_entries(&node, b.entries);
                ack(&node, applied, Vec::new())
            }
            Message::HintReplay(b) => {
                if !b.discard.is_empty() {
                    self.discard(&node, from, &b.discard);
                }
                let applied = self.apply_entries(&node, b.entries);
                ack(&node, applied, Vec::new())
            }
            Message::Handoff(b) => {
                let want: Vec<Bytes> = b
                    .offers
                    .into_iter()
                    .filter(|(k, v)| self.store.version(k).is_none_or(|local| local < *v))
                    .map(|(k, _)| k)
                    .collect();
                let applied = self.apply_entries(&node, b.entries);
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

    /// Drop the keys of the lost refill buckets whose primary is `from`, before a replay
    /// refills what it can (spec section 9).
    fn discard(&self, node: &Node, from: NodeId, buckets: &[u8]) {
        let epoch = node.epochs().current().clone();
        self.store
            .retain(|k| !(buckets.contains(&refill_bucket(k)) && epoch.primary(k) == Some(from)));
    }

    /// Apply entries that are newer than the local ones. An entry with a version too far ahead
    /// of the local clock is dropped (spec section 8.1).
    pub fn apply_entries(&self, node: &Node, entries: Vec<(Bytes, Entry)>) -> u32 {
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

fn ack(node: &Node, applied: u32, want: Vec<Bytes>) -> Message {
    Message::BatchAck(BatchAck {
        applied,
        want,
        epoch: node.epoch_id(),
    })
}
