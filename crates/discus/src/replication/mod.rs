//! Replication, read repair, hinted handoff, handoff, and promotion reconciliation (spec
//! sections 5.2, 8.2, and 9).
//!
//! The primary acknowledges a write once it applied it locally (`W = 1`). Then
//! [`Replicator::written`] queues the keys for every other current owner. One sender task per
//! peer drains its queue in batches on the bulk lane. A replica applies an entry only if its
//! version is newer, so replication is idempotent and order-tolerant.

mod queue;
pub mod repair;

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::{Epoch, NodeId};
use discus_store::{Entry, Store};
use discus_transport::{Lane, TransportError};
use discus_wire::{caps, Batch, BatchAck, Message, TraceContext};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{Config, Node};
use queue::{PeerQueue, Pushed};

/// The most bytes of entries in one replication batch.
const BATCH_BYTES: u64 = 4 << 20;

/// How often the replicator reports its gauges.
const TICK: Duration = Duration::from_secs(1);

/// The replication settings from the flags.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub queue_keys: usize,
    pub batch_keys: usize,
    pub timeout: Duration,
    pub retry: Duration,
    pub rf: usize,
}

impl Settings {
    pub fn from_config(c: &Config) -> Settings {
        Settings {
            queue_keys: c.replication_queue_keys.max(1),
            batch_keys: c.replication_batch_keys.max(1),
            timeout: Duration::from_millis(c.replication_timeout_ms),
            retry: Duration::from_millis(c.replication_retry_ms.max(1)),
            rf: c.replication_factor,
        }
    }
}

pub struct Replicator {
    node: Weak<Node>,
    id: NodeId,
    store: Arc<Store>,
    settings: Settings,
    peers: Mutex<HashMap<NodeId, Arc<PeerQueue>>>,
    stop: CancellationToken,
    me: Weak<Replicator>,
}

impl Replicator {
    pub fn new(node: &Arc<Node>) -> Arc<Replicator> {
        Arc::new_cyclic(|me| Replicator {
            me: me.clone(),
            node: Arc::downgrade(node),
            id: node.id,
            store: node.store.clone(),
            settings: Settings::from_config(&node.config),
            peers: Mutex::new(HashMap::new()),
            stop: CancellationToken::new(),
        })
    }

    /// Run the gauge reporter until `stop`. Sender tasks start with their first key.
    pub fn spawn(self: &Arc<Self>, stop: CancellationToken) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(TICK);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => {
                        me.stop.cancel();
                        return;
                    }
                    _ = every.tick() => me.report(),
                }
            }
        });
    }

    fn report(&self) {
        let metrics = discus_telemetry::global();
        metrics.tombstones(self.store.tombstones() as u64);
        for (peer, q) in self.peers.lock().iter() {
            if let Some(label) = crate::telemetry::node_label(*peer) {
                metrics.replication_depth(&label, q.len() as u64);
            }
        }
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
                let q = self.queue(owner);
                match q.push(key) {
                    Pushed::Queued => {}
                    Pushed::Overflow(_) => {}
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

    /// No queue and no batch in flight holds `key`: every replica acknowledged its latest write.
    pub fn settled(&self, key: &[u8]) -> bool {
        self.peers.lock().values().all(|q| !q.holds(key))
    }

    /// A newer epoch is current. Queues stop carrying keys that this node or the target no
    /// longer owns (spec section 5.1), and peers that left the epoch lose their queue.
    pub fn on_epoch(&self, _old: &Arc<Epoch>, new: &Arc<Epoch>) {
        let rf = self.settings.rf;
        let mut peers = self.peers.lock();
        peers.retain(|peer, q| {
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
    }

    async fn send_loop(self: Arc<Self>, q: Arc<PeerQueue>) {
        loop {
            if q.is_closed() || self.stop.is_cancelled() {
                return;
            }
            if q.is_down() {
                tokio::select! {
                    _ = self.stop.cancelled() => return,
                    _ = tokio::time::sleep(self.settings.retry) => {}
                }
                if self.probe(&q).await {
                    q.set_up();
                }
                continue;
            }
            let keys = q.take(self.settings.batch_keys);
            if keys.is_empty() {
                tokio::select! {
                    _ = self.stop.cancelled() => return,
                    _ = q.notify.notified() => {}
                }
                continue;
            }
            let Some(node) = self.node.upgrade() else {
                return;
            };
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
                    if let Some(label) = crate::telemetry::node_label(q.peer) {
                        discus_telemetry::global().replication_lag(&label, started.elapsed());
                    }
                }
                Err(TransportError::PeerUnsupported(_)) => q.acked(),
                Err(e) => {
                    tracing::debug!(peer = %q.peer, error = %e, "a replication batch failed");
                    q.failed();
                }
            }
        }
    }

    /// An empty batch tells whether the peer takes replication again.
    async fn probe(&self, q: &PeerQueue) -> bool {
        let Some(node) = self.node.upgrade() else {
            return false;
        };
        let batch = Batch::new(node.epoch_id());
        self.send(&node, q.peer, Message::Replicate(batch))
            .await
            .is_ok()
    }

    /// Send one bulk message to `peer` and return its acknowledgement. A peer without the
    /// replication capability gets nothing.
    async fn send(
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
    pub async fn handle(&self, _from: NodeId, message: Message) -> Message {
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
            Message::Fetch { keys } => {
                let mut out = Batch::new(node.epoch_id());
                for k in keys {
                    if let Some(e) = self.store.entry(&k) {
                        out.entries.push((k, e));
                    }
                }
                Message::Entries(out)
            }
            other => Message::Unsupported {
                message_type: other.type_code(),
            },
        }
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
