//! Memory admission and sampled approximate-LRU eviction.

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::entry::estimate_size;
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
    /// Make room for `bytes` more, evicting sampled least-recently-used entries.
    ///
    /// A request larger than `max_memory` is refused up front, so one huge value cannot flush
    /// the whole store. Callers must not hold a store lock while they call this.
    pub fn reserve(&self, bytes: u64) -> Admit {
        let max = self.config.max_memory;
        if bytes > max {
            return Admit::TooLarge;
        }
        while self.used_memory() + bytes > max {
            if !self.evict_one() {
                break;
            }
        }
        Admit::Ok
    }

    /// Evict the oldest of a small sample from the next non-empty shard. Returns false when the
    /// store holds nothing to evict.
    pub fn evict_one(&self) -> bool {
        let n = self.shards.len();
        let first = self.evict_shard.fetch_add(1, Ordering::Relaxed);
        for offset in 0..n {
            let shard = &self.shards[(first + offset) % n];
            let mut guard = shard.inner.write();
            let len = guard.map.len();
            if len == 0 {
                continue;
            }
            let start = shard.cursor.fetch_add(EVICTION_SAMPLES, Ordering::Relaxed) % len;
            let victim: Option<Bytes> = guard
                .map
                .iter()
                .skip(start)
                .chain(guard.map.iter())
                .take(EVICTION_SAMPLES.min(len))
                .min_by_key(|(_, e)| e.last_access_ms)
                .map(|(k, _)| k.clone());
            if let Some(k) = victim {
                if let Some(e) = guard.map.remove(&k) {
                    self.adjust_used(estimate_size(&k, &e), 0);
                    self.drop_len(1);
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
