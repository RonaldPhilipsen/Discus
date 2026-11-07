//! Cursor iteration, random key, and clearing.

use bytes::Bytes;

use crate::glob::glob_match;
use crate::Store;

const POSITION_BITS: u32 = 48;
const POSITION_MASK: u64 = (1 << POSITION_BITS) - 1;

fn encode(shard: usize, pos: usize) -> u64 {
    ((shard as u64) << POSITION_BITS) | (pos as u64 & POSITION_MASK)
}

impl Store {
    /// Walk the keys with a cursor. `count` is the number of entries to examine, as in Redis.
    ///
    /// The cursor holds a shard index in its high 16 bits and a position within the shard in its
    /// low 48 bits. A returned cursor of 0 means the walk is complete. Keys that exist for the
    /// whole walk are returned at least once. Expired keys are skipped.
    ///
    /// The position is a count into the shard's iteration order, so each call is O(position). A
    /// later plan can replace it with an indexed map if that matters.
    pub fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>) {
        let now = self.clock.now_ms();
        let count = count.max(1);
        let mut shard_idx = (cursor >> POSITION_BITS) as usize;
        let mut pos = (cursor & POSITION_MASK) as usize;
        let mut examined = 0;
        let mut out = Vec::new();
        while shard_idx < self.shards.len() {
            let guard = self.shards[shard_idx].inner.read();
            let len = guard.map.len();
            for (k, e) in guard.map.iter().skip(pos) {
                pos += 1;
                examined += 1;
                if !e.is_expired(now) && pattern.is_none_or(|p| glob_match(p, k)) {
                    out.push(k.clone());
                }
                if examined >= count && pos < len {
                    return (encode(shard_idx, pos), out);
                }
            }
            shard_idx += 1;
            pos = 0;
            if examined >= count {
                break;
            }
        }
        let next = if shard_idx < self.shards.len() {
            encode(shard_idx, 0)
        } else {
            0
        };
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
                .find(|(_, e)| !e.is_expired(now));
            if let Some((k, _)) = live {
                return Some(k.clone());
            }
        }
        None
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            let mut guard = shard.inner.write();
            let drained = std::mem::take(&mut guard.map);
            for (k, e) in &drained {
                self.adjust_used(crate::estimate_size(k, e), 0);
            }
            self.drop_len(drained.len());
        }
    }
}
