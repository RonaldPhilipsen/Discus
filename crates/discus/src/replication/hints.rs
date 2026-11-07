//! Hints: writes that the primary holds for a secondary it cannot reach (spec section 9).
//!
//! Hints live per target and key, so they coalesce to the latest entry of a key. Every bound
//! evicts the oldest hint first: the entry cap and the byte cap of a target, the global cap, the
//! maximum age, and memory pressure. An evicted hint is lost, so its refill bucket is marked for
//! the target, and the first replay batch tells the target to discard those buckets.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::{NodeId, Version};
use discus_store::{estimate_size, refill_bucket, Entry};
use lru::LruCache;

/// The bookkeeping cost of one hint on top of its entry.
const HINT_OVERHEAD: u64 = 64;

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
    bytes: u64,
    lost: BTreeSet<u8>,
}

impl Target {
    fn new() -> Target {
        Target {
            hints: LruCache::unbounded(),
            bytes: 0,
            lost: BTreeSet::new(),
        }
    }
}

pub struct HintStore {
    targets: HashMap<NodeId, Target>,
    bytes: u64,
    limits: HintLimits,
}

impl HintStore {
    pub fn new(limits: HintLimits) -> HintStore {
        HintStore {
            targets: HashMap::new(),
            bytes: 0,
            limits,
        }
    }

    /// Bytes that every hint holds together.
    pub fn bytes(&self) -> u64 {
        self.bytes
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
        if let Some(old) = t.hints.pop(&key) {
            t.bytes -= old.size;
            self.bytes -= old.size;
        }
        t.hints.push(
            key,
            Hint {
                entry,
                at: now,
                size,
            },
        );
        t.bytes += size;
        self.bytes += size;
        let mut evicted = 0;
        while t.hints.len() > self.limits.max_entries || t.bytes > self.limits.max_bytes {
            let Some((k, h)) = t.hints.pop_lru() else {
                break;
            };
            t.bytes -= h.size;
            self.bytes -= h.size;
            t.lost.insert(refill_bucket(&k));
            evicted += 1;
        }
        let mut out = Vec::new();
        if evicted > 0 {
            out.push((target, evicted));
        }
        while self.bytes > self.limits.global_bytes {
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
        let t = self.targets.get_mut(&target)?;
        let (k, h) = t.hints.pop_lru()?;
        t.bytes -= h.size;
        self.bytes -= h.size;
        t.lost.insert(refill_bucket(&k));
        Some(target)
    }

    /// Evict hints older than the maximum age. Returns the evictions by target.
    pub fn expire(&mut self, now: Instant) -> Vec<(NodeId, u64)> {
        let mut out = Vec::new();
        for (id, t) in &mut self.targets {
            while t
                .hints
                .peek_lru()
                .is_some_and(|(_, h)| now.duration_since(h.at) >= self.limits.max_age)
            {
                let Some((k, h)) = t.hints.pop_lru() else {
                    break;
                };
                t.bytes -= h.size;
                self.bytes -= h.size;
                t.lost.insert(refill_bucket(&k));
                bump(&mut out, *id);
            }
        }
        out
    }

    /// Evict the oldest hints until `want` bytes are free or no hint is left. Returns the bytes
    /// freed and the evictions by target.
    pub fn free(&mut self, want: u64) -> (u64, Vec<(NodeId, u64)>) {
        let before = self.bytes;
        let mut out = Vec::new();
        while before - self.bytes < want {
            match self.evict_oldest() {
                Some(t) => bump(&mut out, t),
                None => break,
            }
        }
        (before - self.bytes, out)
    }

    /// Up to `max` hints of `target` in HLC order, oldest version first.
    pub fn batch(&self, target: NodeId, max: usize) -> Vec<(Bytes, Entry)> {
        let Some(t) = self.targets.get(&target) else {
            return Vec::new();
        };
        let mut all: Vec<(&Bytes, &Hint)> = t.hints.iter().collect();
        all.sort_by_key(|(_, h)| h.entry.version);
        all.into_iter()
            .take(max)
            .map(|(k, h)| (k.clone(), h.entry.clone()))
            .collect()
    }

    /// The target acknowledged these hints. A hint that a newer write replaced stays.
    pub fn confirm(&mut self, target: NodeId, sent: &[(Bytes, Version)]) {
        let Some(t) = self.targets.get_mut(&target) else {
            return;
        };
        for (k, v) in sent {
            if t.hints.peek(k).is_some_and(|h| h.entry.version == *v) {
                if let Some(h) = t.hints.pop(k) {
                    t.bytes -= h.size;
                    self.bytes -= h.size;
                }
            }
        }
    }

    /// The refill buckets of `target` whose hints were lost.
    pub fn lost(&self, target: NodeId) -> Vec<u8> {
        self.targets
            .get(&target)
            .map(|t| t.lost.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn clear_lost(&mut self, target: NodeId) {
        if let Some(t) = self.targets.get_mut(&target) {
            t.lost.clear();
        }
    }

    /// Drop every hint of `target`, which never returns.
    pub fn drop_target(&mut self, target: NodeId) {
        if let Some(t) = self.targets.remove(&target) {
            self.bytes -= t.bytes;
        }
    }

    /// Keep only the hints for which `keep(target, key)` holds. Dropped hints are not lost:
    /// the target no longer owns the key.
    pub fn retain(&mut self, keep: impl Fn(NodeId, &Bytes) -> bool) {
        for (id, t) in &mut self.targets {
            let drop: Vec<Bytes> = t
                .hints
                .iter()
                .map(|(k, _)| k)
                .filter(|k| !keep(*id, k))
                .cloned()
                .collect();
            for k in drop {
                if let Some(h) = t.hints.pop(&k) {
                    t.bytes -= h.size;
                    self.bytes -= h.size;
                }
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
        self.bytes = 0;
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
        assert_eq!(h.lost(T), vec![refill_bucket(b"a")]);
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
        assert_eq!(h.lost(T), vec![refill_bucket(b"a")]);
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
        assert!(h.lost(T).is_empty());
        h.drop_target(T);
        assert_eq!(h.bytes(), 0);
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
