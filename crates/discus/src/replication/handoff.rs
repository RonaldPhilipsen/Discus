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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::{range_of, slot, Epoch, EpochId, NodeId, Owners, Version, NTP64, RANGES};
use discus_wire::{Batch, Message};

use super::Settings;
use crate::node::EpochPin;
use crate::Node;

/// Entries per scan step, and the most entries in one batch.
const STEP: usize = 256;
/// The most bytes of entries in one batch.
const BATCH_BYTES: u64 = 1 << 20;
/// Attempts per batch before the source gives up on a target for this scan.
const ATTEMPTS: u32 = 4;
const FIRST_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(2);
/// Tries per page of the drop of moved keys while an owner runs an older epoch. With the backoff
/// this waits about 20 seconds before the pass keeps the keys of that owner.
const BEHIND_TRIES: u32 = 12;

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
#[derive(Default)]
struct Target {
    batch: Vec<(Bytes, discus_store::Entry)>,
    offers: Vec<(Bytes, Version)>,
    bytes: u64,
    /// Keys the target asked for in its last acknowledgement.
    wanted: Vec<Bytes>,
    abandoned: bool,
}

impl Target {
    fn full(&self) -> bool {
        self.batch.len() + self.offers.len() >= STEP || self.bytes >= BATCH_BYTES
    }
}

/// Run one scan for every change in `changes`, from the oldest old epoch to `to`. Returns when
/// every target got its `done` batch or was abandoned, and whether every target got it.
pub async fn scan(
    node: &Arc<Node>,
    changes: &[Outgoing],
    to: Arc<Epoch>,
    settings: &Settings,
) -> bool {
    let Some(replicator) = node.replicator().cloned() else {
        return false;
    };
    let Some(from) = changes.iter().map(|c| c.old.clone()).min_by_key(|e| e.id()) else {
        return false;
    };
    let me = node.id;
    let mut targets: HashMap<NodeId, Target> = changes
        .iter()
        .flat_map(|c| c.gained.iter().copied())
        .filter(|g| *g != me && to.member(*g).is_some())
        .map(|g| (g, Target::default()))
        .collect();
    if targets.is_empty() {
        return true;
    }
    // Entries written after this point replicate to their current owners on their own.
    let watermark = node.hlc.current();
    let rf = settings.rf;
    let changed = from.changed_ranges(&to, rf);
    tracing::info!(from = %from.id(), to = %to.id(), targets = targets.len(), "handoff scan");
    let mut cursor = 0;
    loop {
        #[cfg(feature = "testing")]
        crate::node::Faults::wait(&node.faults.hold_scan, &node.faults.scan_held).await;
        let (next, entries) = node
            .store
            .scan_entries(cursor, STEP, |_, e| e.version.time() <= watermark);
        for (k, e) in entries {
            if !changed.contains(range_of(slot(&k))) {
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
                flush(node, &replicator, *g, t, None, settings).await;
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
            flush(node, &replicator, *g, t, None, settings).await;
        }
        if !t.abandoned {
            flush(node, &replicator, *g, t, Some(range), settings).await;
        }
    }
    targets.values().all(|t| !t.abandoned)
}

/// Drop the keys that this node owns neither in `to` nor in the current epoch, after a scan up
/// to `to` that reached every target. The old owner keeps its entries only until the new owners
/// acknowledge the handoff (spec section 5.2): after its `done` batch, no new owner falls back to
/// it. Returns how many keys it dropped.
///
/// A write can reach this node after the scan passed its key, from a command that routed before
/// the adoption. The scan never sent it, and its replication can still be queued. So each key is
/// first offered at its version to every current owner, which asks for it if its stored copy,
/// expired or not, is older. Asked-for keys are sent and offered again, so an owner that refused
/// them asks again and the keys stay. A key goes only if every owner answered in the epoch of
/// this node, holds the key at the offered version or newer, no queue or hint holds it, and the
/// entry still has the offered version. An owner in an older epoch can still forward a command
/// on the key here, so its keys stay; an owner in a newer epoch ends the pass.
///
/// A key also stays when its entry is newer than `watermark`, because a peer in a newer epoch can
/// forward a write here first. Every key stays once this node adopts another epoch during the
/// pass, or once a peer sent a batch from an epoch newer than the current one: such a batch can
/// carry entries of ranges that this node gains in that epoch.
pub async fn drop_lost(
    node: &Arc<Node>,
    to: &Epoch,
    watermark: NTP64,
    settings: &Settings,
) -> usize {
    let Some(replicator) = node.replicator().cloned() else {
        return 0;
    };
    let adoptions = replicator.adoptions.load(Ordering::SeqCst);
    let (me, rf) = (node.id, settings.rf);
    let current = node.epochs().current().clone();
    if !current.is_normal(me) {
        return 0;
    }
    let epoch = current.id();
    let lost: Vec<bool> = (0..RANGES)
        .map(|r| {
            !to.range_owners(r, rf).contains(&me) && !current.range_owners(r, rf).contains(&me)
        })
        .collect();
    let moved_on = || {
        replicator.peer_generation.load(Ordering::SeqCst) > epoch.generation
            || replicator.adoptions.load(Ordering::SeqCst) != adoptions
    };
    let mut failed: BTreeSet<NodeId> = BTreeSet::new();
    let mut dropped = 0;
    let mut cursor = 0;
    loop {
        #[cfg(feature = "testing")]
        crate::node::Faults::wait(&node.faults.hold_drop, &node.faults.drop_held).await;
        if moved_on() {
            stopped(node, "this node moved on to another epoch");
            return dropped;
        }
        let (next, entries) = node.store.scan_entries(cursor, STEP, |k, e| {
            lost[usize::from(range_of(slot(k)))] && e.version.time() <= watermark
        });
        let offered: Vec<(Bytes, Version, Owners)> = entries
            .into_iter()
            .map(|(k, e)| (k.clone(), e.version, current.owners(&k, rf)))
            .filter(|(_, _, owners)| {
                !owners.is_empty() && !owners.iter().any(|o| failed.contains(o))
            })
            .collect();
        let mut offers: HashMap<NodeId, Vec<(Bytes, Version)>> = HashMap::new();
        for (k, v, owners) in &offered {
            for o in owners {
                offers.entry(*o).or_default().push((k.clone(), *v));
            }
        }
        let mut answers: HashMap<NodeId, Answer> = HashMap::new();
        for (owner, offers) in offers {
            let mut answer = confirm(node, &replicator, owner, offers.clone(), epoch).await;
            // An owner that lags behind adopts the epoch soon, as a rule.
            let mut wait = FIRST_BACKOFF;
            for _ in 0..BEHIND_TRIES {
                if !matches!(answer, Answer::Behind) || moved_on() {
                    break;
                }
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(MAX_BACKOFF);
                answer = confirm(node, &replicator, owner, offers.clone(), epoch).await;
            }
            match answer {
                Answer::Ahead => {
                    stopped(node, "an owner runs ahead of this node");
                    return dropped;
                }
                Answer::Failed | Answer::Behind => {
                    failed.insert(owner);
                }
                Answer::Holds(_) => {}
            }
            answers.insert(owner, answer);
        }
        for (k, v, owners) in offered {
            let held = owners.iter().all(|o| match answers.get(o) {
                Some(Answer::Holds(missing)) => !missing.contains(&k),
                _ => false,
            });
            if !held || !replicator.settled(&k) {
                continue;
            }
            let removed = node.store.remove_if(&k, |e| e.version == v && !moved_on());
            dropped += usize::from(removed);
        }
        if next == 0 {
            return dropped;
        }
        cursor = next;
    }
}

/// What one owner answered to the offers of a page.
enum Answer {
    /// The owner is in the epoch of the drop and holds every offered key at the offered version
    /// or newer, but these.
    Holds(BTreeSet<Bytes>),
    /// The owner still runs an older epoch, where this node can own the keys. It can forward a
    /// command on them here, so they stay.
    Behind,
    /// The owner runs a newer epoch that this node has not adopted.
    Ahead,
    /// The owner did not answer.
    Failed,
}

/// Offer `offers` to `owner`, send the keys it asks for, and offer those again at the versions
/// sent. A key that the owner still asks for after that was refused, for example for clock
/// drift or size.
async fn confirm(
    node: &Arc<Node>,
    replicator: &crate::replication::Replicator,
    owner: NodeId,
    offers: Vec<(Bytes, Version)>,
    epoch: EpochId,
) -> Answer {
    let mut batch = Batch::new(epoch);
    batch.offers = offers;
    for round in 0..2 {
        let Some(ack) = send(node, replicator, owner, &batch).await else {
            return Answer::Failed;
        };
        if ack.epoch != epoch {
            return if ack.epoch.generation > epoch.generation {
                Answer::Ahead
            } else {
                Answer::Behind
            };
        }
        if ack.want.is_empty() || round == 1 {
            return Answer::Holds(ack.want.into_iter().collect());
        }
        let mut missing = BTreeSet::new();
        batch = Batch::new(epoch);
        for k in ack.want {
            match node.store.entry(&k) {
                Some(e) => {
                    batch.offers.push((k.clone(), e.version));
                    batch.entries.push((k, e));
                }
                None => {
                    missing.insert(k);
                }
            }
        }
        if batch.offers.is_empty() {
            return Answer::Holds(missing);
        }
    }
    unreachable!("the second round returns")
}

#[cfg(feature = "testing")]
fn stopped(node: &Node, why: &'static str) {
    *node.faults.drop_stopped.lock() = Some(why);
}

#[cfg(not(feature = "testing"))]
fn stopped(_: &Node, _: &'static str) {}

/// Send the batch of one target with retries. A target that keeps failing is abandoned for
/// this scan. Wanted keys from the acknowledgement go into the next batch.
async fn flush(
    node: &Arc<Node>,
    replicator: &crate::replication::Replicator,
    target: NodeId,
    t: &mut Target,
    done: Option<(EpochId, EpochId)>,
    settings: &Settings,
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
    match send(node, replicator, target, &batch).await {
        Some(ack) => {
            t.wanted = ack.want;
            pace(bytes, settings.handoff_rate).await;
        }
        None => {
            tracing::warn!(%target, "abandoned a handoff target after its retries");
            t.abandoned = true;
        }
    }
}

/// Send one handoff batch with retries. `None` when every attempt failed.
async fn send(
    node: &Arc<Node>,
    replicator: &crate::replication::Replicator,
    target: NodeId,
    batch: &Batch,
) -> Option<discus_wire::BatchAck> {
    let mut backoff = FIRST_BACKOFF;
    for attempt in 0..ATTEMPTS {
        match replicator
            .send(node, target, Message::Handoff(batch.clone()))
            .await
        {
            Ok(Message::BatchAck(ack)) => return Some(ack),
            Ok(_) => return None,
            Err(e) => {
                tracing::debug!(%target, attempt, error = %e, "a handoff batch failed");
                if attempt + 1 < ATTEMPTS {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
    }
    None
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
    let outgoing =
        (old.is_normal(me) && new.member(me).is_some() && gained.iter().any(|g| *g != me))
            .then(|| gained.clone());
    let incoming = (gained.contains(&me) && new.is_normal(me))
        .then(|| {
            old.normal()
                .filter(|m| m.id != me)
                .filter(|m| new.member(m.id).is_some())
                .map(|m| m.id)
                .collect::<BTreeSet<NodeId>>()
        })
        .filter(|s| !s.is_empty());
    (outgoing, incoming)
}
