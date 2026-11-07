//! Cursor iteration, random key, and clearing.

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::glob::glob_match;
use crate::{Entry, Store};

const POSITION_BITS: u32 = 48;
const POSITION_MASK: u64 = (1 << POSITION_BITS) - 1;

fn encode(shard: usize, pos: usize) -> u64 {
    ((shard as u64) << POSITION_BITS) | (pos as u64 & POSITION_MASK)
}

impl Store {
    /// Walk the map with a cursor and give `f` every entry that is not expired. `count` is the
    /// number of entries to examine, as in Redis.
    ///
    /// The cursor holds a shard index in its high 16 bits and a position within the shard in its
    /// low 48 bits. A returned cursor of 0 means the walk is complete. Keys that exist for the
    /// whole walk are seen at least once.
    ///
    /// The position is a count into the shard's iteration order, so each call is O(position). A
    /// later plan can replace it with an indexed map if that matters.
    fn walk(&self, cursor: u64, count: usize, mut f: impl FnMut(&Bytes, &Entry)) -> u64 {
        let now = self.clock.now_ms();
        let count = count.max(1);
        let mut shard_idx = (cursor >> POSITION_BITS) as usize;
        let mut pos = (cursor & POSITION_MASK) as usize;
        let mut examined = 0;
        while shard_idx < self.shards.len() {
            let guard = self.shards[shard_idx].inner.read();
            let len = guard.map.len();
            for (k, e) in guard.map.iter().skip(pos) {
                pos += 1;
                examined += 1;
                if !e.is_expired(now) {
                    f(k, e);
                }
                if examined >= count && pos < len {
                    return encode(shard_idx, pos);
                }
            }
            shard_idx += 1;
            pos = 0;
            if examined >= count {
                break;
            }
        }
        if shard_idx < self.shards.len() {
            encode(shard_idx, 0)
        } else {
            0
        }
    }

    /// Walk the live keys with a cursor, as `SCAN` does. Tombstones and expired keys are skipped.
    pub fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>) {
        let mut out = Vec::new();
        let next = self.walk(cursor, count, |k, e| {
            if !e.tombstone && pattern.is_none_or(|p| glob_match(p, k)) {
                out.push(k.clone());
            }
        });
        (next, out)
    }

    /// Walk the entries with a cursor, tombstones included, for handoff and reconciliation.
    pub fn scan_entries(&self, cursor: u64, count: usize) -> (u64, Vec<(Bytes, Entry)>) {
        let mut out = Vec::new();
        let next = self.walk(cursor, count, |k, e| out.push((k.clone(), e.clone())));
        (next, out)
    }

    /// A random live key, or `None` when the store is empty.
    pub fn random_key(&self) -> Option<Bytes> {
        let now = self.clock.now_ms();
        let n = self.shards.len();
        let first = fastrand::usize(..n);
        for offset in 0..n {
            let guard = self.shards[(first + offset) % n].inner.read();
            let len = guard.map.len();
            if len == 0 {
                continue;
            }
            let start = fastrand::usize(..len);
            let live = guard
                .map
                .iter()
                .skip(start)
                .chain(guard.map.iter())
                .take(len)
                .find(|(_, e)| !e.tombstone && !e.is_expired(now));
            if let Some((k, _)) = live {
                return Some(k.clone());
            }
        }
        None
    }

    /// Drop every entry and tombstone, and every refill mark.
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut guard = shard.inner.write();
            let keys: Vec<Bytes> = guard.map.keys().cloned().collect();
            for k in keys {
                self.take(&mut guard, &k);
            }
            shard.needs_refill.store(false, Ordering::Relaxed);
        }
    }
}
