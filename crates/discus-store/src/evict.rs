//! Memory admission and sampled approximate-LRU eviction.

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::Store;

/// Entries that one eviction step samples from a shard.
const EVICTION_SAMPLES: usize = 5;

/// The outcome of asking the store for room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    Ok,
    /// The request alone is larger than the whole memory budget.
    TooLarge,
}

impl Store {
    /// Make room for `bytes` more. The order is the one of spec section 11: sampled
    /// least-recently-used entries, then the pressure hook (the oldest hints), then the oldest
    /// tombstones, which marks their shards `needs_refill`.
    ///
    /// A request larger than `max_memory` is refused up front, so one huge value cannot flush
    /// the whole store. Callers must not hold a store lock while they call this.
    pub fn reserve(&self, bytes: u64) -> Admit {
        let max = self.config.max_memory;
        if bytes > max {
            return Admit::TooLarge;
        }
        while self.used_memory() + bytes > max {
            if self.evict_one() {
                continue;
            }
            let want = self.used_memory() + bytes - max;
            if self.pressure.get().is_some_and(|free| free(want) > 0) {
                continue;
            }
            if !self.drop_oldest_tombstone() {
                break;
            }
        }
        Admit::Ok
    }

    /// Evict the oldest of a small sample of live entries, of any database, from the next shard
    /// that has one.
    /// Tombstones are never sampled. Returns false when the store holds no live entry.
    pub fn evict_one(&self) -> bool {
        let n = self.shards.len();
        let first = self.evict_shard.fetch_add(1, Ordering::Relaxed);
        for offset in 0..n {
            let shard = &self.shards[(first + offset) % n];
            let mut guard = shard.inner.write();
            let len = guard.map_len();
            if len == guard.tomb_len() {
                continue;
            }
            let start = shard.cursor.fetch_add(EVICTION_SAMPLES, Ordering::Relaxed) % len;
            let victim: Option<(u32, Bytes)> = guard
                .entries()
                .skip(start)
                .chain(guard.entries())
                .take(len)
                .filter(|(_, _, e)| !e.tombstone)
                .take(EVICTION_SAMPLES)
                .min_by_key(|(_, _, e)| e.last_access_ms)
                .map(|(db, k, _)| (db, k.clone()));
            if let Some((db, k)) = victim {
                if self.take(&mut guard, db, &k).is_some() {
                    self.evicted.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            }
        }
        false
    }

    /// Total entries evicted for memory since start.
    pub fn evictions(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }
}
