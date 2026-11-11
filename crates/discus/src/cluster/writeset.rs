//! The write set of one connection (spec section 7.4).
//!
//! After a write, the entry node records the key with the acknowledged HLC. A read of a key in
//! the set goes to the primary, or stays on this node when it owns the key and holds a version
//! at least as new. The key leaves the set after a read that observes a new enough version that
//! every replica holds, after the maximum age, or by LRU. The set ends with the connection.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_placement::Version;
use discus_store::Store;
use lru::LruCache;
use xxhash_rust::xxh3::xxh3_64;

/// The bookkeeping cost of one recorded key on top of the key bytes.
const KEY_OVERHEAD: usize = 48;

/// A set takes the node budget in chunks of this size, so most writes touch no shared counter.
/// It keeps at most two chunks unused, and none when it is empty.
const BUDGET_CHUNK: usize = 1024;

#[derive(Debug, Clone)]
pub struct WriteSetLimits {
    pub keys: usize,
    pub bytes: usize,
    pub max_age: Duration,
}

/// The node-wide share of the memory budget for write sets. Write sets are bounded at
/// admission and never evicted for memory (spec section 11).
pub struct WriteSetBudget {
    used: AtomicU64,
    cap: u64,
    store: Option<Arc<Store>>,
}

impl WriteSetBudget {
    pub fn new(cap: u64, store: Option<Arc<Store>>) -> WriteSetBudget {
        WriteSetBudget {
            used: AtomicU64::new(0),
            cap,
            store,
        }
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    fn try_add(&self, n: u64) -> bool {
        let mut cur = self.used.load(Ordering::Relaxed);
        loop {
            if cur + n > self.cap {
                return false;
            }
            match self.used.compare_exchange_weak(
                cur,
                cur + n,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(seen) => cur = seen,
            }
        }
        if let Some(s) = &self.store {
            s.charge(n);
        }
        true
    }

    fn sub(&self, n: u64) {
        self.used.fetch_sub(n, Ordering::Relaxed);
        if let Some(s) = &self.store {
            s.release(n);
        }
    }
}

struct Recorded {
    version: Version,
    at: Instant,
    size: usize,
}

/// A key as the write set tracks it: the xxh3 hash and the length of the key. The set stores no
/// key bytes, so recording a key copies nothing. Two keys with one hash share an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetKey {
    hash: u64,
    len: usize,
}

impl SetKey {
    pub fn of(key: &[u8]) -> SetKey {
        SetKey {
            hash: xxh3_64(key),
            len: key.len(),
        }
    }
}

pub struct WriteSet {
    /// Bounded by the key limit, so a push to a full set reuses the node of the key it evicts.
    map: LruCache<u64, Recorded>,
    /// The bytes of the recorded keys.
    bytes: usize,
    /// The node budget that this set holds, at least `bytes`.
    held: usize,
    limits: WriteSetLimits,
    budget: Arc<WriteSetBudget>,
}

impl WriteSet {
    pub fn new(limits: WriteSetLimits, budget: Arc<WriteSetBudget>) -> WriteSet {
        let cap = NonZeroUsize::new(limits.keys).unwrap_or(NonZeroUsize::MIN);
        WriteSet {
            map: LruCache::new(cap),
            bytes: 0,
            held: 0,
            limits,
            budget,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn remove(&mut self, key: SetKey) {
        if let Some(r) = self.map.pop(&key.hash) {
            self.bytes -= r.size;
            self.trim();
        }
    }

    /// Drop the oldest key from the set but keep its budget share. False when the set is empty.
    fn evict_oldest(&mut self) -> bool {
        let Some((_, r)) = self.map.pop_lru() else {
            return false;
        };
        self.bytes -= r.size;
        true
    }

    /// Take `n` more bytes of the node budget, a whole chunk when the budget has room.
    fn reserve(&mut self, n: usize) -> bool {
        let got = [n.max(BUDGET_CHUNK), n]
            .into_iter()
            .find(|&m| self.budget.try_add(m as u64));
        if let Some(m) = got {
            self.held += m;
        }
        got.is_some()
    }

    /// Give back the unused budget beyond one chunk, or all of it when the set is empty.
    fn trim(&mut self) {
        let spare = self.held - self.bytes;
        let keep = if self.bytes == 0 { 0 } else { BUDGET_CHUNK };
        if spare > 2 * BUDGET_CHUNK || (self.bytes == 0 && spare > 0) {
            let give = spare - keep;
            self.budget.sub(give as u64);
            self.held -= give;
        }
    }

    /// Record a write of `key` that the primary acknowledged at `version`. A key that does not
    /// fit the node budget even after this set gave up its own oldest keys is not recorded.
    pub fn record(&mut self, key: SetKey, version: Version, now: Instant) {
        if let Some(r) = self.map.get_mut(&key.hash) {
            r.version = version;
            r.at = now;
            return;
        }
        let size = key.len + KEY_OVERHEAD;
        while self.bytes + size > self.limits.bytes {
            if !self.evict_oldest() {
                return self.trim();
            }
        }
        // A full set evicts its oldest key in `push`. The budget of evicted keys passes to the
        // new key first.
        loop {
            let pushed_out = match self.map.len() >= self.map.cap().get() {
                true => self.map.peek_lru().map_or(0, |(_, r)| r.size),
                false => 0,
            };
            let short = (self.bytes - pushed_out + size).saturating_sub(self.held);
            if short == 0 || self.reserve(short) {
                break;
            }
            if !self.evict_oldest() {
                return self.trim();
            }
        }
        let recorded = Recorded {
            version,
            at: now,
            size,
        };
        if let Some((_, old)) = self.map.push(key.hash, recorded) {
            self.bytes -= old.size;
        }
        self.bytes += size;
        self.trim();
    }

    /// The version that a read of `key` has to see, while the key is in the set. `now` is called
    /// only for a key in the set.
    pub fn required(&mut self, key: SetKey, now: impl FnOnce() -> Instant) -> Option<Version> {
        let r = self.map.peek(&key.hash)?;
        if now().duration_since(r.at) >= self.limits.max_age {
            self.remove(key);
            return None;
        }
        Some(r.version)
    }

    /// A read of `key` saw `version`. With `settled`, every replica holds it, so a version at
    /// least as new as the recorded one ends the entry.
    pub fn observed(&mut self, key: SetKey, version: Option<Version>, settled: bool) {
        let done = self
            .map
            .peek(&key.hash)
            .is_some_and(|r| settled && version.is_some_and(|v| v >= r.version));
        if done {
            self.remove(key);
        }
    }
}

impl std::fmt::Debug for WriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSet")
            .field("keys", &self.map.len())
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for WriteSet {
    fn drop(&mut self) {
        self.budget.sub(self.held as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use discus_placement::{NodeId, NTP64};

    fn v(t: u64) -> Version {
        Version::new(NTP64(t), NodeId::from_u128(1))
    }

    fn limits(keys: usize, bytes: usize) -> WriteSetLimits {
        WriteSetLimits {
            keys,
            bytes,
            max_age: Duration::from_secs(5),
        }
    }

    fn set(keys: usize, bytes: usize, cap: u64) -> (WriteSet, Arc<WriteSetBudget>) {
        let budget = Arc::new(WriteSetBudget::new(cap, None));
        (WriteSet::new(limits(keys, bytes), budget.clone()), budget)
    }

    fn k(s: &str) -> SetKey {
        SetKey::of(s.as_bytes())
    }

    #[test]
    fn an_entry_leaves_after_five_seconds() {
        let (mut w, _) = set(10, 1 << 20, u64::MAX);
        let now = Instant::now();
        w.record(k("a"), v(1), now);
        assert_eq!(
            w.required(k("a"), || now + Duration::from_millis(4_999)),
            Some(v(1))
        );
        assert_eq!(w.required(k("a"), || now + Duration::from_secs(5)), None);
        assert!(w.is_empty());
    }

    #[test]
    fn the_key_cap_evicts_by_lru() {
        let (mut w, _) = set(2, 1 << 20, u64::MAX);
        let now = Instant::now();
        w.record(k("a"), v(1), now);
        w.record(k("b"), v(2), now);
        w.record(k("c"), v(3), now);
        assert_eq!(w.len(), 2);
        assert_eq!(w.required(k("a"), || now), None);
        assert_eq!(w.required(k("c"), || now), Some(v(3)));
    }

    #[test]
    fn the_byte_cap_evicts_by_lru() {
        let (mut w, _) = set(100, 2 * (1 + KEY_OVERHEAD), u64::MAX);
        let now = Instant::now();
        for s in ["a", "b", "c"] {
            w.record(k(s), v(1), now);
        }
        assert_eq!(w.len(), 2);
        assert_eq!(w.required(k("a"), || now), None);
    }

    #[test]
    fn the_node_cap_refuses_admission() {
        let budget = Arc::new(WriteSetBudget::new((1 + KEY_OVERHEAD) as u64, None));
        let mut one = WriteSet::new(limits(10, 1 << 20), budget.clone());
        let mut two = WriteSet::new(limits(10, 1 << 20), budget.clone());
        let now = Instant::now();
        one.record(k("a"), v(1), now);
        two.record(k("b"), v(1), now);
        assert_eq!(one.len(), 1);
        assert_eq!(two.len(), 0);
        // A set gives up its own oldest key for a new one.
        one.record(k("c"), v(2), now);
        assert_eq!(one.required(k("c"), || now), Some(v(2)));
        assert_eq!(one.len(), 1);
    }

    #[test]
    fn a_settled_new_read_removes_the_key() {
        let (mut w, _) = set(10, 1 << 20, u64::MAX);
        let now = Instant::now();
        w.record(k("a"), v(5), now);
        w.observed(k("a"), Some(v(4)), true);
        w.observed(k("a"), Some(v(9)), false);
        assert_eq!(w.len(), 1);
        w.observed(k("a"), Some(v(5)), true);
        assert!(w.is_empty());
    }

    proptest::proptest! {
        #[test]
        fn the_node_budget_matches_the_recorded_keys(
            ops in proptest::collection::vec((0u8..40, 1usize..30, proptest::bool::ANY), 1..300),
            keys in 1usize..12,
            cap in 0u64..2_000,
        ) {
            let budget = Arc::new(WriteSetBudget::new(cap, None));
            let mut one = WriteSet::new(limits(keys, 600), budget.clone());
            let mut two = WriteSet::new(limits(keys, 600), budget.clone());
            let now = Instant::now();
            for (i, (id, len, first)) in ops.into_iter().enumerate() {
                let key = SetKey::of(&vec![id; len % 8 + id as usize % 3 + 1]);
                let w = if first { &mut one } else { &mut two };
                w.record(key, v(i as u64 + 1), now);
                if len % 7 == 0 {
                    w.observed(key, Some(v(u64::MAX)), true);
                }
                proptest::prop_assert_eq!(budget.used(), (one.held + two.held) as u64);
                proptest::prop_assert!(budget.used() <= cap);
                for w in [&one, &two] {
                    proptest::prop_assert!(w.map.len() <= keys && w.bytes <= 600);
                    proptest::prop_assert!(w.bytes <= w.held && w.held - w.bytes <= 2 * BUDGET_CHUNK);
                    proptest::prop_assert!(w.bytes > 0 || w.held == 0);
                }
            }
            drop(one);
            drop(two);
            proptest::prop_assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn dropping_the_set_releases_the_node_budget() {
        let (mut w, budget) = set(10, 1 << 20, u64::MAX);
        w.record(k("a"), v(1), Instant::now());
        assert!(budget.used() > 0);
        drop(w);
        assert_eq!(budget.used(), 0);
    }
}
