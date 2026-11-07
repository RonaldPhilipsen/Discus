//! The sharded in-memory map.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::entry::estimate_size;
use crate::{Clock, Entry};

/// The seed of the key hash. A change to it or to the hash is a breaking change, because
/// placement in later plans depends on it.
pub const HASH_SEED: u64 = 0x4449_5343_5553_0001;

/// Reads refresh the access time at most this often per key, so they rarely need the write lock.
const ACCESS_RESOLUTION_MS: u64 = 1_000;

#[derive(Debug, Clone, Copy)]
pub struct StoreConfig {
    pub shards: usize,
    pub max_memory: u64,
    pub seed: u64,
}

#[derive(Default)]
pub(crate) struct ShardInner {
    pub(crate) map: HashMap<Bytes, Entry>,
}

pub(crate) struct Shard {
    pub(crate) inner: RwLock<ShardInner>,
    /// Where the next sweep or eviction sample starts within the shard's iteration order.
    pub(crate) cursor: AtomicUsize,
}

pub struct Store {
    pub(crate) shards: Vec<Shard>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) config: StoreConfig,
    used: AtomicU64,
    len: AtomicUsize,
    pub(crate) evict_shard: AtomicUsize,
    pub(crate) evicted: AtomicU64,
}

impl Store {
    pub fn new(config: StoreConfig, clock: Arc<dyn Clock>) -> Store {
        let count = config.shards.max(1);
        let shards = (0..count)
            .map(|_| Shard {
                inner: RwLock::new(ShardInner::default()),
                cursor: AtomicUsize::new(0),
            })
            .collect();
        Store {
            shards,
            clock,
            config,
            used: AtomicU64::new(0),
            len: AtomicUsize::new(0),
            evict_shard: AtomicUsize::new(0),
            evicted: AtomicU64::new(0),
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    pub fn shard_index(&self, key: &[u8]) -> usize {
        (xxh3_64_with_seed(key, self.config.seed) as usize) % self.shards.len()
    }

    /// Read an entry. An expired entry reads as absent but is not deleted here.
    pub fn read<R>(&self, key: &[u8], f: impl FnOnce(Option<&Entry>) -> R) -> R {
        let shard = &self.shards[self.shard_index(key)];
        let now = self.clock.now_ms();
        let (result, stale) = {
            let guard = shard.inner.read();
            match guard.map.get(key) {
                Some(e) if !e.is_expired(now) => {
                    (f(Some(e)), e.last_access_ms + ACCESS_RESOLUTION_MS < now)
                }
                _ => (f(None), false),
            }
        };
        if stale {
            if let Some(e) = shard.inner.write().map.get_mut(key) {
                e.last_access_ms = now;
            }
        }
        result
    }

    /// Modify an entry in place. An expired entry is deleted before `f` runs, so `f` sees `None`.
    /// Setting the slot to `None` deletes the key.
    pub fn write<R>(&self, key: &[u8], f: impl FnOnce(&mut Option<Entry>) -> R) -> R {
        let shard = &self.shards[self.shard_index(key)];
        let now = self.clock.now_ms();
        let mut guard = shard.inner.write();
        let (key_bytes, mut slot, before) = match guard.map.remove_entry(key) {
            Some((k, e)) => {
                let size = estimate_size(&k, &e);
                if e.is_expired(now) {
                    (Some(k), None, size)
                } else {
                    (Some(k), Some(e), size)
                }
            }
            None => (None, None, 0),
        };
        let existed = key_bytes.is_some();
        let result = f(&mut slot);
        let after = match slot {
            Some(mut e) => {
                e.last_access_ms = now;
                let k = key_bytes.unwrap_or_else(|| Bytes::copy_from_slice(key));
                let size = estimate_size(&k, &e);
                guard.map.insert(k, e);
                if !existed {
                    self.len.fetch_add(1, Ordering::Relaxed);
                }
                size
            }
            None => {
                if existed {
                    self.len.fetch_sub(1, Ordering::Relaxed);
                }
                0
            }
        };
        self.adjust_used(before, after);
        result
    }

    /// Delete a key. Returns true when a live entry was removed.
    pub fn remove(&self, key: &[u8]) -> bool {
        self.write(key, |slot| slot.take().is_some())
    }

    /// Number of stored entries, including expired entries that nobody reaped yet.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn used_memory(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub(crate) fn adjust_used(&self, before: u64, after: u64) {
        if after >= before {
            self.used.fetch_add(after - before, Ordering::Relaxed);
        } else {
            self.used.fetch_sub(before - after, Ordering::Relaxed);
        }
    }

    pub(crate) fn drop_len(&self, n: usize) {
        self.len.fetch_sub(n, Ordering::Relaxed);
    }
}
