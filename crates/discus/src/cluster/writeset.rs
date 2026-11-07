//! The write set of one connection (spec section 7.4).
//!
//! After a write, the entry node records the key with the acknowledged HLC. A read of a key in
//! the set goes to the primary, or stays on this node when it owns the key and holds a version
//! at least as new. The key leaves the set after a read that observes a new enough version that
//! every replica holds, after the maximum age, or by LRU. The set ends with the connection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::Version;
use discus_store::Store;
use lru::LruCache;

/// The bookkeeping cost of one recorded key on top of the key bytes.
const KEY_OVERHEAD: usize = 48;

#[derive(Debug, Clone)]
pub struct WriteSetLimits {
    pub keys: usize,
    pub bytes: usize,
    pub max_age: Duration,
    /// Keys with this prefix take the strong path and never enter a write set.
    pub counter_prefix: Bytes,
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

pub struct WriteSet {
    map: LruCache<Bytes, Recorded>,
    bytes: usize,
    limits: WriteSetLimits,
    budget: Arc<WriteSetBudget>,
}

impl WriteSet {
    pub fn new(limits: WriteSetLimits, budget: Arc<WriteSetBudget>) -> WriteSet {
        WriteSet {
            map: LruCache::unbounded(),
            bytes: 0,
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

    fn remove(&mut self, key: &[u8]) {
        if let Some(r) = self.map.pop(key) {
            self.release(r.size);
        }
    }

    fn release(&mut self, size: usize) {
        self.bytes -= size;
        self.budget.sub(size as u64);
    }

    fn pop_oldest(&mut self) -> bool {
        match self.map.pop_lru() {
            Some((_, r)) => {
                self.release(r.size);
                true
            }
            None => false,
        }
    }

    /// Record a write of `key` that the primary acknowledged at `version`. A key that does not
    /// fit the node budget even after this set gave up its own oldest keys is not recorded.
    pub fn record(&mut self, key: &Bytes, version: Version, now: Instant) {
        if key.starts_with(&self.limits.counter_prefix) {
            return;
        }
        self.remove(key);
        let size = key.len() + KEY_OVERHEAD;
        while self.map.len() >= self.limits.keys || self.bytes + size > self.limits.bytes {
            if !self.pop_oldest() {
                return;
            }
        }
        while !self.budget.try_add(size as u64) {
            if !self.pop_oldest() {
                return;
            }
        }
        self.bytes += size;
        self.map.push(
            key.clone(),
            Recorded {
                version,
                at: now,
                size,
            },
        );
    }

    /// The version that a read of `key` has to see, while the key is in the set.
    pub fn required(&mut self, key: &[u8], now: Instant) -> Option<Version> {
        let r = self.map.peek(key)?;
        if now.duration_since(r.at) >= self.limits.max_age {
            self.remove(key);
            return None;
        }
        Some(r.version)
    }

    /// A read of `key` saw `version`. With `settled`, every replica holds it, so a version at
    /// least as new as the recorded one ends the entry.
    pub fn observed(&mut self, key: &[u8], version: Option<Version>, settled: bool) {
        let done = self
            .map
            .peek(key)
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
        self.budget.sub(self.bytes as u64);
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
            counter_prefix: Bytes::from_static(b"cnt:"),
        }
    }

    fn set(keys: usize, bytes: usize, cap: u64) -> (WriteSet, Arc<WriteSetBudget>) {
        let budget = Arc::new(WriteSetBudget::new(cap, None));
        (WriteSet::new(limits(keys, bytes), budget.clone()), budget)
    }

    fn k(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn an_entry_leaves_after_five_seconds() {
        let (mut w, _) = set(10, 1 << 20, u64::MAX);
        let now = Instant::now();
        w.record(&k("a"), v(1), now);
        assert_eq!(
            w.required(b"a", now + Duration::from_millis(4_999)),
            Some(v(1))
        );
        assert_eq!(w.required(b"a", now + Duration::from_secs(5)), None);
        assert!(w.is_empty());
    }

    #[test]
    fn the_key_cap_evicts_by_lru() {
        let (mut w, _) = set(2, 1 << 20, u64::MAX);
        let now = Instant::now();
        w.record(&k("a"), v(1), now);
        w.record(&k("b"), v(2), now);
        w.record(&k("c"), v(3), now);
        assert_eq!(w.len(), 2);
        assert_eq!(w.required(b"a", now), None);
        assert_eq!(w.required(b"c", now), Some(v(3)));
    }

    #[test]
    fn the_byte_cap_evicts_by_lru() {
        let (mut w, _) = set(100, 2 * (1 + KEY_OVERHEAD), u64::MAX);
        let now = Instant::now();
        for s in ["a", "b", "c"] {
            w.record(&k(s), v(1), now);
        }
        assert_eq!(w.len(), 2);
        assert_eq!(w.required(b"a", now), None);
    }

    #[test]
    fn the_node_cap_refuses_admission() {
        let budget = Arc::new(WriteSetBudget::new((1 + KEY_OVERHEAD) as u64, None));
        let mut one = WriteSet::new(limits(10, 1 << 20), budget.clone());
        let mut two = WriteSet::new(limits(10, 1 << 20), budget.clone());
        let now = Instant::now();
        one.record(&k("a"), v(1), now);
        two.record(&k("b"), v(1), now);
        assert_eq!(one.len(), 1);
        assert_eq!(two.len(), 0);
        // A set gives up its own oldest key for a new one.
        one.record(&k("c"), v(2), now);
        assert_eq!(one.required(b"c", now), Some(v(2)));
        assert_eq!(one.len(), 1);
    }

    #[test]
    fn a_settled_new_read_removes_the_key() {
        let (mut w, _) = set(10, 1 << 20, u64::MAX);
        let now = Instant::now();
        w.record(&k("a"), v(5), now);
        w.observed(b"a", Some(v(4)), true);
        w.observed(b"a", Some(v(9)), false);
        assert_eq!(w.len(), 1);
        w.observed(b"a", Some(v(5)), true);
        assert!(w.is_empty());
    }

    #[test]
    fn counter_keys_are_never_recorded() {
        let (mut w, _) = set(10, 1 << 20, u64::MAX);
        w.record(&k("cnt:hits"), v(1), Instant::now());
        assert!(w.is_empty());
    }

    #[test]
    fn dropping_the_set_releases_the_node_budget() {
        let (mut w, budget) = set(10, 1 << 20, u64::MAX);
        w.record(&k("a"), v(1), Instant::now());
        assert!(budget.used() > 0);
        drop(w);
        assert_eq!(budget.used(), 0);
    }
}
