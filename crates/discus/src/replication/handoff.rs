//! Handoff: entries move to a node that gains ownership (spec section 5.2).
//!
//! Every ownership change registers inside `Node::adopt_epoch`, under the epoch lock, so the
//! startup probe never sees a joined node without its incoming handoff. A source scans its store
//! once for every pending change: from the oldest pending epoch to the current one. For each
//! entry and each gained owner, the first old owner that the current epoch still lists sends
//! the full entry, and every other one sends an offer. The receiver asks for the offered keys it
//! lacks. Each source ends a target with a `done` batch, and the receiver's handoff completes
//! once every source is done or gone.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::{Epoch, EpochId, MemberState, NodeId, Version};
use discus_wire::{Batch, Message};

use crate::node::EpochPin;
use crate::Node;

/// Entries per scan step, and the most entries in one batch.
const STEP: usize = 256;
/// The most bytes of entries in one batch.
const BATCH_BYTES: u64 = 1 << 20;
/// Attempts per batch before the source gives up on a target for this scan.
const ATTEMPTS: u32 = 4;
const FIRST_BACKOFF: Duration = Duration::from_millis(200);

/// One ownership change that this node sends entries for.
pub struct Outgoing {
    pub old: Arc<Epoch>,
    pub gained: Vec<NodeId>,
    /// Keeps the old epoch in the history until the handoff ends.
    pub _pin: EpochPin,
}

/// One ownership change that gave this node owner slots.
pub struct Incoming {
    pub old: Arc<Epoch>,
    pub new: EpochId,
    /// The sources that have not finished yet.
    pub sources: BTreeSet<NodeId>,
    pub started: Instant,
    /// Keeps the old epoch in the history until the handoff ends.
    pub _pin: EpochPin,
}

#[derive(Default)]
pub struct State {
    pub pending: Vec<Outgoing>,
    pub scanning: bool,
    pub incoming: Vec<Incoming>,
    /// The newest epoch that each source finished a scan up to. A source can finish before
    /// this node adopts the epoch of the change.
    pub finished: HashMap<NodeId, EpochId>,
}

impl State {
    /// Track a new incoming handoff. Sources that already finished a scan up to its epoch
    /// are not waited for.
    pub fn expect(&mut self, mut inc: Incoming) {
        inc.sources
            .retain(|s| self.finished.get(s).is_none_or(|to| !covers(*to, inc.new)));
        self.incoming.push(inc);
    }

    /// Mark `source` done up to epoch `to`. Returns the handoffs that completed, so the caller
    /// drops their pins after it releases the lock.
    pub fn done(&mut self, source: NodeId, to: EpochId) -> Vec<Incoming> {
        let last = self.finished.entry(source).or_insert(to);
        if to > *last {
            *last = to;
        }
        for inc in &mut self.incoming {
            if covers(to, inc.new) {
                inc.sources.remove(&source);
            }
        }
        self.take_finished(|_| false)
    }

    /// Drop sources that left the epoch, and give up on handoffs older than `timeout`.
    pub fn prune(&mut self, current: &Epoch, timeout: Duration) -> Vec<Incoming> {
        for inc in &mut self.incoming {
            inc.sources.retain(|s| current.member(*s).is_some());
        }
        self.take_finished(|inc| inc.started.elapsed() >= timeout)
    }

    fn take_finished(&mut self, abandon: impl Fn(&Incoming) -> bool) -> Vec<Incoming> {
        let (done, open): (Vec<Incoming>, Vec<Incoming>) = std::mem::take(&mut self.incoming)
            .into_iter()
            .partition(|inc| inc.sources.is_empty() || abandon(inc));
        self.incoming = open;
        done
    }
}

/// A scan up to `to` covers a change to `new`. Two nodes can create competing epochs of one
/// generation with the same members, so the generation decides, not the origin.
fn covers(to: EpochId, new: EpochId) -> bool {
    to.generation >= new.generation
}

/// What a scan sends to one target.
struct Target {
    batch: Vec<(Bytes, discus_store::Entry)>,
    offers: Vec<(Bytes, Version)>,
    bytes: u64,
    /// Keys the target asked for in its last acknowledgement.
    wanted: Vec<Bytes>,
    abandoned: bool,
}

impl Target {
    fn new() -> Target {
        Target {
            batch: Vec::new(),
            offers: Vec::new(),
            bytes: 0,
            wanted: Vec::new(),
            abandoned: false,
        }
    }

    fn full(&self) -> bool {
        self.batch.len() + self.offers.len() >= STEP || self.bytes >= BATCH_BYTES
    }
}

/// The settings of a scan.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub rate_bytes: u64,
    pub rf: usize,
}

/// Run one scan for every change in `changes`, from the oldest old epoch to `to`. Returns when
/// every target got its `done` batch or was abandoned.
pub async fn scan(node: &Arc<Node>, changes: &[Outgoing], to: Arc<Epoch>, limits: Limits) {
    let Some(replicator) = node.replicator().cloned() else {
        return;
    };
    let Some(from) = changes.iter().map(|c| c.old.clone()).min_by_key(|e| e.id()) else {
        return;
    };
    let me = node.id;
    let mut targets: HashMap<NodeId, Target> = changes
        .iter()
        .flat_map(|c| c.gained.iter().copied())
        .filter(|g| *g != me && to.member(*g).is_some())
        .map(|g| (g, Target::new()))
        .collect();
    if targets.is_empty() {
        return;
    }
    // Entries written after this point replicate to their current owners on their own.
    let watermark = node.hlc.current();
    let rf = limits.rf;
    tracing::info!(from = %from.id(), to = %to.id(), targets = targets.len(), "handoff scan");
    let mut cursor = 0;
    loop {
        let (next, entries) = node.store.scan_entries(cursor, STEP);
        for (k, e) in entries {
            if e.version.time() > watermark {
                continue;
            }
            let old_owners = from.owners(&k, rf);
            let survivors: Vec<NodeId> = old_owners
                .iter()
                .copied()
                .filter(|o| to.member(*o).is_some())
                .collect();
            let Some(rank) = survivors.iter().position(|s| *s == me) else {
                continue;
            };
            for g in to.owners(&k, rf) {
                if old_owners.contains(&g) {
                    continue;
                }
                let Some(t) = targets.get_mut(&g) else {
                    continue;
                };
                if t.abandoned {
                    continue;
                }
                if rank == 0 {
                    t.bytes += discus_store::estimate_size(&k, &e);
                    t.batch.push((k.clone(), e.clone()));
                } else {
                    t.offers.push((k.clone(), e.version));
                }
            }
        }
        for (g, t) in targets.iter_mut() {
            if t.full() && !t.abandoned {
                flush(node, &replicator, *g, t, None, limits).await;
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    let range = (from.id(), to.id());
    for (g, t) in targets.iter_mut() {
        while !t.abandoned && (!t.wanted.is_empty() || t.full()) {
            flush(node, &replicator, *g, t, None, limits).await;
        }
        if !t.abandoned {
            flush(node, &replicator, *g, t, Some(range), limits).await;
        }
    }
}

/// Send the batch of one target with retries. A target that keeps failing is abandoned for
/// this scan. Wanted keys from the acknowledgement go into the next batch.
async fn flush(
    node: &Arc<Node>,
    replicator: &crate::replication::Replicator,
    target: NodeId,
    t: &mut Target,
    done: Option<(EpochId, EpochId)>,
    limits: Limits,
) {
    let mut batch = Batch::new(node.epoch_id());
    batch.entries = std::mem::take(&mut t.batch);
    for k in std::mem::take(&mut t.wanted) {
        if let Some(e) = node.store.entry(&k) {
            batch.entries.push((k, e));
        }
    }
    batch.offers = std::mem::take(&mut t.offers);
    batch.done = done;
    let bytes = std::mem::take(&mut t.bytes);
    let mut backoff = FIRST_BACKOFF;
    for attempt in 0..ATTEMPTS {
        match replicator
            .send(node, target, Message::Handoff(batch.clone()))
            .await
        {
            Ok(Message::BatchAck(ack)) => {
                t.wanted = ack.want;
                pace(bytes, limits.rate_bytes).await;
                return;
            }
            Ok(_) => break,
            Err(e) => {
                tracing::debug!(%target, attempt, error = %e, "a handoff batch failed");
                if attempt + 1 < ATTEMPTS {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
    }
    tracing::warn!(%target, "abandoned a handoff target after its retries");
    t.abandoned = true;
}

/// Wait long enough that the scan stays under `rate` bytes per second.
async fn pace(bytes: u64, rate: u64) {
    if rate == 0 || bytes == 0 {
        return;
    }
    let secs = bytes as f64 / rate as f64;
    tokio::time::sleep(Duration::from_secs_f64(secs)).await;
}

/// The records that an adoption from `old` to `new` creates for node `me`.
pub fn changes(
    old: &Arc<Epoch>,
    new: &Arc<Epoch>,
    me: NodeId,
    rf: usize,
) -> (Option<Vec<NodeId>>, Option<BTreeSet<NodeId>>) {
    let gained = discus_membership::gained_owners(old, new, rf);
    if gained.is_empty() {
        return (None, None);
    }
    let normal = |e: &Epoch, id| e.member(id).is_some_and(|m| m.state == MemberState::Normal);
    let outgoing = (normal(old, me) && new.member(me).is_some() && gained.iter().any(|g| *g != me))
        .then(|| gained.clone());
    let incoming = (gained.contains(&me) && normal(new, me))
        .then(|| {
            old.members()
                .iter()
                .filter(|m| m.state == MemberState::Normal && m.id != me)
                .filter(|m| new.member(m.id).is_some())
                .map(|m| m.id)
                .collect::<BTreeSet<NodeId>>()
        })
        .filter(|s| !s.is_empty());
    (outgoing, incoming)
}
