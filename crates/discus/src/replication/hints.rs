//! Hints: writes that the primary holds for a secondary it cannot reach (spec section 9).
//!
//! Hints live per target and key, so they coalesce to the latest entry of a key. Every bound
//! evicts the oldest hint first: the entry cap and the byte cap of a target, the global cap, the
//! maximum age, and memory pressure. An evicted hint is lost, so its refill bucket is marked for
//! the target, and the first replay batch tells the target to discard those buckets.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::{NodeId, Version};
use discus_store::{estimate_size, refill_bucket, Entry};
use lru::LruCache;

/// The bookkeeping cost of one hint on top of its entry: its LRU node and its place in the
/// replay order.
const HINT_OVERHEAD: u64 = 128;

#[derive(Debug, Clone, Copy)]
pub struct HintLimits {
    pub max_entries: usize,
    pub max_bytes: u64,
    pub global_bytes: u64,
    pub max_age: Duration,
}

#[derive(Debug, Clone)]
pub struct Hint {
    pub entry: Entry,
    pub at: Instant,
    size: u64,
}

struct Target {
    hints: LruCache<Bytes, Hint>,
    /// The hints by version. A replay batch takes the oldest versions from here, so a replay of
    /// N hints costs O(N log N) in all, not a sort of every hint per batch.
    order: BTreeSet<(Version, Bytes)>,
    bytes: u64,
    /// The refill buckets of lost hints, each with the mark of its latest loss.
    lost: BTreeMap<u8, u64>,
    /// Raised on every loss.
    marks: u64,
}

impl Target {
    /// Store a hint in place of an older hint of the key.
    fn put(&mut self, key: Bytes, hint: Hint) {
        self.pop(&key);
        self.bytes += hint.size;
        self.order.insert((hint.entry.version, key.clone()));
        self.hints.push(key, hint);
    }

    fn pop(&mut self, key: &[u8]) -> Option<Hint> {
        let (k, h) = self.hints.pop_entry(key)?;
        self.order.remove(&(h.entry.version, k));
        self.bytes -= h.size;
        Some(h)
    }

    /// Evict the oldest hint and mark its bucket lost. False when no hint is left.
    fn evict(&mut self) -> bool {
        let Some((k, h)) = self.hints.pop_lru() else {
            return false;
        };
        self.order.remove(&(h.entry.version, k.clone()));
        self.bytes -= h.size;
        self.marks += 1;
        self.lost.insert(refill_bucket(&k), self.marks);
        true
    }
}

/// The lost buckets of a target at one moment, for the first batch of a replay.
#[derive(Debug, Clone, Default)]
pub struct Lost {
    pub buckets: Vec<u8>,
    mark: u64,
}

impl Target {
    fn new() -> Target {
        Target {
            hints: LruCache::unbounded(),
            order: BTreeSet::new(),
            bytes: 0,
            lost: BTreeMap::new(),
            marks: 0,
        }
    }
}

pub struct HintStore {
    targets: HashMap<NodeId, Target>,
    limits: HintLimits,
}

impl HintStore {
    pub fn new(limits: HintLimits) -> HintStore {
        HintStore {
            targets: HashMap::new(),
            limits,
        }
    }

    /// Bytes that every hint holds together.
    pub fn bytes(&self) -> u64 {
        self.targets.values().map(|t| t.bytes).sum()
    }

    pub fn count(&self, target: NodeId) -> usize {
        self.targets.get(&target).map_or(0, |t| t.hints.len())
    }

    /// Any target holds a hint for `key`.
    pub fn holds(&self, key: &[u8]) -> bool {
        self.targets.values().any(|t| t.hints.contains(key))
    }

    /// Record the latest entry of `key` for `target`, replacing an older hint. Returns the
    /// hints that the bounds evicted, by target.
    pub fn record(
        &mut self,
        target: NodeId,
        key: Bytes,
        entry: Entry,
        now: Instant,
    ) -> Vec<(NodeId, u64)> {
        let size = estimate_size(&key, &entry) + HINT_OVERHEAD;
        let t = self.targets.entry(target).or_insert_with(Target::new);
        let hint = Hint {
            entry,
            at: now,
            size,
        };
        t.put(key, hint);
        let mut evicted = 0;
        while (t.hints.len() > self.limits.max_entries || t.bytes > self.limits.max_bytes)
            && t.evict()
        {
            evicted += 1;
        }
        let mut out = Vec::new();
        if evicted > 0 {
            out.push((target, evicted));
        }
        while self.bytes() > self.limits.global_bytes {
            match self.evict_oldest() {
                Some(t) => bump(&mut out, t),
                None => break,
            }
        }
        out
    }

    /// Evict the oldest hint over every target. Returns its target.
    fn evict_oldest(&mut self) -> Option<NodeId> {
        let target = self
            .targets
            .iter()
            .filter_map(|(id, t)| t.hints.peek_lru().map(|(_, h)| (h.at, *id)))
            .min()?
            .1;
        self.targets.get_mut(&target)?.evict().then_some(target)
    }

    /// Evict hints older than the maximum age. Returns the evictions by target.
    pub fn expire(&mut self, now: Instant) -> Vec<(NodeId, u64)> {
        let mut out = Vec::new();
        for (id, t) in &mut self.targets {
            while t
                .hints
                .peek_lru()
                .is_some_and(|(_, h)| now.duration_since(h.at) >= self.limits.max_age)
                && t.evict()
            {
                bump(&mut out, *id);
            }
        }
        out
    }

    /// Evict the oldest hints until `want` bytes are free or no hint is left. Returns the bytes
    /// freed and the evictions by target.
    pub fn free(&mut self, want: u64) -> (u64, Vec<(NodeId, u64)>) {
        let before = self.bytes();
        let mut out = Vec::new();
        while before - self.bytes() < want {
            match self.evict_oldest() {
                Some(t) => bump(&mut out, t),
                None => break,
            }
        }
        (before - self.bytes(), out)
    }

    /// Up to `max` hints of `target` in HLC order, oldest version first.
    pub fn batch(&self, target: NodeId, max: usize) -> Vec<(Bytes, Entry)> {
        let Some(t) = self.targets.get(&target) else {
            return Vec::new();
        };
        t.order
            .iter()
            .take(max)
            .filter_map(|(_, k)| t.hints.peek(k).map(|h| (k.clone(), h.entry.clone())))
            .collect()
    }

    /// The target acknowledged these hints. A hint that a newer write replaced stays.
    pub fn confirm(&mut self, target: NodeId, sent: &[(Bytes, Version)]) {
        let Some(t) = self.targets.get_mut(&target) else {
            return;
        };
        for (k, v) in sent {
            if t.hints.peek(k).is_some_and(|h| h.entry.version == *v) {
                t.pop(k);
            }
        }
    }

    /// The refill buckets of `target` whose hints were lost.
    pub fn lost(&self, target: NodeId) -> Lost {
        self.targets
            .get(&target)
            .map(|t| Lost {
                buckets: t.lost.keys().copied().collect(),
                mark: t.marks,
            })
            .unwrap_or_default()
    }

    /// The target discarded the buckets of `sent`. A bucket lost again since `sent` was read
    /// stays, because the target may have discarded it before that loss.
    pub fn clear_lost(&mut self, target: NodeId, sent: &Lost) {
        if let Some(t) = self.targets.get_mut(&target) {
            for b in &sent.buckets {
                if t.lost.get(b).is_some_and(|m| *m <= sent.mark) {
                    t.lost.remove(b);
                }
            }
        }
    }

    /// Drop every hint of `target`, which never returns.
    /// Drop every hint of `target`. Returns the keys of the dropped hints.
    pub fn drop_target(&mut self, target: NodeId) -> Vec<Bytes> {
        self.targets
            .remove(&target)
            .map(|t| t.hints.iter().map(|(k, _)| k.clone()).collect())
            .unwrap_or_default()
    }

    /// Keep only the hints for which `keep(target, key)` holds. Dropped hints are not lost:
    /// the target no longer owns the key.
    pub fn retain(&mut self, mut keep: impl FnMut(NodeId, &Bytes) -> bool) {
        for (id, t) in &mut self.targets {
            let drop: Vec<Bytes> = t
                .hints
                .iter()
                .map(|(k, _)| k)
                .filter(|k| !keep(*id, k))
                .cloned()
                .collect();
            for k in drop {
                t.pop(&k);
            }
        }
        self.targets
            .retain(|_, t| !t.hints.is_empty() || !t.lost.is_empty());
    }

    pub fn targets(&self) -> Vec<NodeId> {
        self.targets.keys().copied().collect()
    }

    pub fn clear(&mut self) {
        self.targets.clear();
    }
}

fn bump(out: &mut Vec<(NodeId, u64)>, target: NodeId) {
    match out.iter_mut().find(|(t, _)| *t == target) {
        Some((_, n)) => *n += 1,
        None => out.push((target, 1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use discus_placement::NTP64;
    use discus_store::Value;

    const T: NodeId = NodeId::from_u128(9);

    fn limits(max_entries: usize) -> HintLimits {
        HintLimits {
            max_entries,
            max_bytes: u64::MAX,
            global_bytes: u64::MAX,
            max_age: Duration::from_secs(60),
        }
    }

    fn entry(t: u64) -> Entry {
        let mut e = Entry::new(Value::Str("v".into()), None);
        e.version = Version::new(NTP64(t), NodeId::from_u128(1));
        e
    }

    fn k(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn hints_coalesce_to_the_latest_entry() {
        let mut h = HintStore::new(limits(10));
        let now = Instant::now();
        h.record(T, k("a"), entry(1), now);
        let one = h.bytes();
        h.record(T, k("a"), entry(2), now);
        assert_eq!(h.count(T), 1);
        assert_eq!(h.bytes(), one);
        assert_eq!(h.batch(T, 10)[0].1.version, entry(2).version);
    }

    #[test]
    fn overflow_evicts_the_oldest_and_marks_its_bucket() {
        let mut h = HintStore::new(limits(2));
        let now = Instant::now();
        h.record(T, k("a"), entry(1), now);
        h.record(T, k("b"), entry(2), now);
        let evicted = h.record(T, k("c"), entry(3), now);
        assert_eq!(evicted, vec![(T, 1)]);
        assert_eq!(h.count(T), 2);
        assert_eq!(h.lost(T).buckets, vec![refill_bucket(b"a")]);
    }

    #[test]
    fn the_global_cap_evicts_the_oldest_hint_of_any_target() {
        let mut l = limits(100);
        let mut h = HintStore::new(l);
        let now = Instant::now();
        h.record(T, k("a"), entry(1), now);
        l.global_bytes = h.bytes() * 2;
        h.limits = l;
        let other = NodeId::from_u128(10);
        h.record(other, k("b"), entry(2), now + Duration::from_millis(1));
        let evicted = h.record(other, k("c"), entry(3), now + Duration::from_millis(2));
        assert_eq!(evicted, vec![(T, 1)]);
        assert_eq!(h.count(T), 0);
        assert_eq!(h.count(other), 2);
    }

    #[test]
    fn old_hints_age_out_and_count_as_lost() {
        let mut h = HintStore::new(limits(10));
        let now = Instant::now();
        h.record(T, k("a"), entry(1), now);
        h.record(T, k("b"), entry(2), now + Duration::from_secs(30));
        let evicted = h.expire(now + Duration::from_secs(61));
        assert_eq!(evicted, vec![(T, 1)]);
        assert_eq!(h.count(T), 1);
        assert_eq!(h.lost(T).buckets, vec![refill_bucket(b"a")]);
    }

    #[test]
    fn replay_batches_come_in_hlc_order() {
        let mut h = HintStore::new(limits(10));
        let now = Instant::now();
        h.record(T, k("late"), entry(30), now);
        h.record(T, k("early"), entry(10), now);
        h.record(T, k("mid"), entry(20), now);
        let keys: Vec<Bytes> = h.batch(T, 2).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![k("early"), k("mid")]);
        h.confirm(
            T,
            &[
                (k("early"), entry(10).version),
                (k("mid"), entry(99).version),
            ],
        );
        assert_eq!(h.count(T), 2);
    }

    #[test]
    fn retain_drops_hints_whose_target_lost_the_key() {
        let mut h = HintStore::new(limits(10));
        let now = Instant::now();
        h.record(T, k("keep"), entry(1), now);
        h.record(T, k("gone"), entry(2), now);
        h.retain(|_, key| key.as_ref() == b"keep");
        assert_eq!(h.count(T), 1);
        assert!(h.lost(T).buckets.is_empty());
        assert_eq!(h.drop_target(T), vec![k("keep")]);
        assert_eq!(h.bytes(), 0);
    }

    /// A replay names the lost buckets in its first batch and clears them once the target
    /// acknowledged it. A bucket lost while that batch was on the wire was not named, so it
    /// stays for the next replay.
    #[test]
    fn a_bucket_lost_during_a_replay_stays_lost() {
        assert_ne!(refill_bucket(b"a"), refill_bucket(b"b"));
        let mut h = HintStore::new(limits(1));
        let now = Instant::now();
        h.record(T, k("a"), entry(1), now);
        h.record(T, k("b"), entry(2), now);
        let sent = h.lost(T);
        assert_eq!(sent.buckets, vec![refill_bucket(b"a")]);
        h.record(T, k("c"), entry(3), now);
        h.clear_lost(T, &sent);
        assert_eq!(h.lost(T).buckets, vec![refill_bucket(b"b")]);
    }

    /// A bucket that was named and then lost again before the acknowledgement stays too: the
    /// target may have discarded it before that loss.
    #[test]
    fn a_bucket_lost_again_during_a_replay_stays_lost() {
        let mut h = HintStore::new(limits(1));
        let now = Instant::now();
        h.record(T, k("a"), entry(1), now);
        h.record(T, k("b"), entry(2), now);
        let sent = h.lost(T);
        h.record(T, k("a"), entry(3), now);
        h.record(T, k("c"), entry(4), now);
        h.clear_lost(T, &sent);
        let mut want = vec![refill_bucket(b"a"), refill_bucket(b"b")];
        want.sort();
        want.dedup();
        assert_eq!(h.lost(T).buckets, want);
        let again = h.lost(T);
        h.clear_lost(T, &again);
        assert!(h.lost(T).buckets.is_empty());
    }

    /// Every hint is in the replay order once, with its current version, through replacement,
    /// eviction, confirmation, and retain. A replay batch reads that order and sorts nothing.
    #[test]
    fn the_replay_order_follows_every_change() {
        let mut h = HintStore::new(limits(4));
        let now = Instant::now();
        for (i, key) in ["a", "b", "c", "d"].iter().enumerate() {
            h.record(T, k(key), entry(10 - i as u64), now);
        }
        h.record(T, k("b"), entry(20), now);
        h.record(T, k("e"), entry(1), now);
        h.confirm(T, &[(k("c"), entry(8).version)]);
        h.retain(|_, key| key.as_ref() != b"d");
        for t in h.targets.values() {
            assert_eq!(t.order.len(), t.hints.len());
            for (v, key) in &t.order {
                assert_eq!(t.hints.peek(key).map(|x| x.entry.version), Some(*v));
            }
        }
        let keys: Vec<Bytes> = h.batch(T, 10).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![k("e"), k("b")]);
        h.expire(now + Duration::from_secs(61));
        assert!(h.batch(T, 10).is_empty());
        assert!(h.targets.values().all(|t| t.order.is_empty()));
    }

    #[test]
    fn free_evicts_until_enough_is_free() {
        let mut h = HintStore::new(limits(10));
        let now = Instant::now();
        h.record(T, k("a"), entry(1), now);
        h.record(T, k("b"), entry(2), now);
        let (freed, _) = h.free(1);
        assert!(freed > 0);
        assert_eq!(h.count(T), 1);
        let (_, _) = h.free(u64::MAX);
        assert_eq!(h.bytes(), 0);
    }
}
