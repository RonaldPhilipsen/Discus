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
use discus_placement::{range_of, slot, Epoch, EpochId, NodeId, Version};
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
/// every target got its `done` batch or was abandoned, and whether every target got it.
pub async fn scan(node: &Arc<Node>, changes: &[Outgoing], to: Arc<Epoch>, limits: Limits) -> bool {
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
        .map(|g| (g, Target::new()))
        .collect();
    if targets.is_empty() {
        return true;
    }
    // Entries written after this point replicate to their current owners on their own.
    let watermark = node.hlc.current();
    let rf = limits.rf;
    let changed = from.changed_ranges(&to, rf);
    tracing::info!(from = %from.id(), to = %to.id(), targets = targets.len(), "handoff scan");
    let mut cursor = 0;
    loop {
        #[cfg(feature = "testing")]
        while node
            .faults
            .hold_scan
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            node.faults
                .scan_held
                .store(true, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
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
    targets.values().all(|t| !t.abandoned)
}

/// Drop the keys that `me` owns neither in `to` nor in `current`, after a scan up to `to` that
/// reached every target. The old owner keeps its entries only until the new owners acknowledge
/// the handoff (spec section 5.2): after its `done` batch, no new owner falls back to it.
///
/// A key stays when its entry is newer than `watermark`, because a peer in a newer epoch can
/// forward a write here first. It also stays once `newest` passes the generation of `current`:
/// a peer in an epoch that this node has not adopted yet sent entries, maybe of ranges that
/// this node gains in that epoch.
pub fn drop_lost(
    store: &discus_store::Store,
    me: NodeId,
    (to, current): (&Epoch, &Epoch),
    rf: usize,
    watermark: discus_placement::NTP64,
    newest: &std::sync::atomic::AtomicU64,
) {
    let owned: Vec<bool> = (0..discus_placement::RANGES)
        .map(|r| to.range_owners(r, rf).contains(&me) || current.range_owners(r, rf).contains(&me))
        .collect();
    let generation = current.id().generation;
    store.retain(|k, e| {
        owned[usize::from(range_of(slot(k)))]
            || e.version.time() > watermark
            || newest.load(std::sync::atomic::Ordering::SeqCst) > generation
    });
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;

    use discus_placement::{ClusterId, ManualClock, Member, MemberState, Weight, NTP64};
    use discus_store::{Entry, Store, StoreConfig, Value};

    use super::*;

    fn member(id: u128) -> Member {
        let addr: SocketAddr = "10.0.0.1:7000".parse().unwrap();
        Member {
            id: NodeId::from_u128(id),
            addr,
            rpc_addr: addr,
            client_addr: None,
            client_host: None,
            state: MemberState::Normal,
            weight: Weight::default(),
        }
    }

    fn put(store: &Store, key: &str, time: u64) {
        let mut e = Entry::new(Value::Str(Bytes::from_static(b"v")), None);
        e.version = Version::new(NTP64(time), NodeId::from_u128(9));
        assert!(store.apply(key.as_bytes(), e));
    }

    /// Two members with `RF = 1`, and keys on both sides of the watermark.
    fn setup() -> (Store, Epoch, Vec<String>, Vec<String>) {
        let id = EpochId {
            generation: 5,
            origin: NodeId::from_u128(1),
        };
        let epoch = Epoch::new(id, ClusterId::NIL, vec![member(1), member(2)]).unwrap();
        epoch.table(1);
        let store = Store::new(StoreConfig::default(), Arc::new(ManualClock::new(1)));
        let (mut mine, mut lost) = (Vec::new(), Vec::new());
        for i in 0..200 {
            let k = format!("k{i}");
            put(&store, &k, 10);
            if epoch.owners(k.as_bytes(), 1) == [NodeId::from_u128(1)] {
                mine.push(k);
            } else {
                lost.push(k);
            }
        }
        (store, epoch, mine, lost)
    }

    fn held(store: &Store, keys: &[String]) -> usize {
        keys.iter()
            .filter(|k| store.read(k.as_bytes(), |e| e.is_some()))
            .count()
    }

    #[test]
    fn drop_lost_keeps_owned_and_newer_keys() {
        let (store, epoch, mine, lost) = setup();
        put(&store, &lost[0], 30);
        let me = NodeId::from_u128(1);
        drop_lost(
            &store,
            me,
            (&epoch, &epoch),
            1,
            NTP64(20),
            &AtomicU64::new(5),
        );
        assert_eq!(held(&store, &mine), mine.len());
        assert_eq!(held(&store, &lost), 1);
        assert!(store.read(lost[0].as_bytes(), |e| e.is_some()));
    }

    #[test]
    fn drop_lost_keeps_every_key_after_a_batch_from_a_newer_epoch() {
        let (store, epoch, mine, lost) = setup();
        let me = NodeId::from_u128(1);
        drop_lost(
            &store,
            me,
            (&epoch, &epoch),
            1,
            NTP64(20),
            &AtomicU64::new(6),
        );
        assert_eq!(held(&store, &mine) + held(&store, &lost), 200);
    }
}
