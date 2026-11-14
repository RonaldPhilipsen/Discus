//! Cursor iteration, random key, and clearing.

use std::collections::BinaryHeap;

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::glob::glob_match;
use crate::{DbKey, Entry, EntryCursor, Store, HASH_SEED};

const ORDER_BITS: u32 = 48;
const ORDER_MAX: u64 = (1 << ORDER_BITS) - 1;

fn encode(shard: usize, from: u64) -> u64 {
    ((shard as u64) << ORDER_BITS) | (from & ORDER_MAX)
}

impl Store {
    /// The place of a key in the walk order of its shard: the top 48 bits of its shard hash.
    /// The shard comes from the low bits of the same hash.
    fn order(&self, key: &[u8]) -> u64 {
        xxh3_64_with_seed(key, HASH_SEED) >> (64 - ORDER_BITS)
    }

    /// Walk the map of database `db` with a cursor and give `f` every entry that is not expired.
    /// `count` is the number of entries to examine, as in Redis. Returns the next cursor and the
    /// number of entries examined. Only the map of `db` is read, so a large database does not
    /// slow down the walk of another.
    ///
    /// The cursor holds a shard index in its high 16 bits and, in its low 48 bits, the lowest
    /// walk order that is still to come in that shard. A returned cursor of 0 means the walk is
    /// complete. The order depends only on the key, so deletes, rewrites, and a rehash of the
    /// map between calls move no key across the cursor: a key that exists for the whole walk is
    /// seen at least once. Keys that share an order value are always given in the same call.
    ///
    /// The map keeps no order, so each call reads the whole shard and keeps the `count` lowest
    /// orders past the cursor. That costs O(shard length * log count) time and O(count) memory
    /// per call, and no memory per entry. The position cursor that this replaced cost O(position)
    /// per call and skipped keys when the map changed. An ordered index per shard would make a
    /// call O(count) but costs memory and work on every insert and delete.
    fn walk(
        &self,
        db: u32,
        cursor: u64,
        count: usize,
        mut f: impl FnMut(&Bytes, &Entry),
    ) -> (u64, usize) {
        let now = self.clock.now_ms();
        let count = count.max(1);
        let mut shard_idx = (cursor >> ORDER_BITS) as usize;
        let mut from = cursor & ORDER_MAX;
        let mut examined = 0;
        while shard_idx < self.shards.len() {
            if examined >= count {
                return (encode(shard_idx, from), examined);
            }
            let want = count - examined;
            let guard = self.shards[shard_idx].inner.read();
            let Some(d) = guard.get(db) else {
                shard_idx += 1;
                from = 0;
                continue;
            };
            // A max-heap of the `want` lowest orders so far. `picked` holds every entry at or
            // below its top, ties included, and is trimmed when it grows.
            let mut lowest: BinaryHeap<u64> = BinaryHeap::with_capacity(want + 1);
            let mut picked: Vec<(u64, &Bytes, &Entry)> = Vec::new();
            for (k, e) in d.map.iter() {
                let o = self.order(k);
                if o < from {
                    continue;
                }
                match lowest.peek() {
                    Some(&top) if lowest.len() >= want && o > top => continue,
                    Some(&top) if lowest.len() >= want && o < top => {
                        lowest.pop();
                        lowest.push(o);
                    }
                    Some(_) if lowest.len() >= want => {}
                    _ => lowest.push(o),
                }
                picked.push((o, k, e));
                if picked.len() > 2 * want + 16 {
                    let top = lowest.peek().copied().unwrap_or(ORDER_MAX);
                    picked.retain(|p| p.0 <= top);
                }
            }
            let full = lowest.len() >= want;
            let top = lowest.peek().copied().unwrap_or(ORDER_MAX);
            for (o, k, e) in picked {
                if o > top {
                    continue;
                }
                examined += 1;
                if !e.is_expired(now) {
                    f(k, e);
                }
            }
            if full && top < ORDER_MAX {
                from = top + 1;
            } else {
                shard_idx += 1;
                from = 0;
            }
        }
        (0, examined)
    }

    /// Walk the live keys of database `db` with a cursor, as `SCAN` does. Tombstones and
    /// expired keys are skipped.
    pub fn scan(
        &self,
        db: u32,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> (u64, Vec<Bytes>) {
        let mut out = Vec::new();
        let (next, _) = self.walk(db, cursor, count, |k, e| {
            if !e.tombstone && pattern.is_none_or(|p| glob_match(p, k)) {
                out.push(k.clone());
            }
        });
        (next, out)
    }

    /// Walk the entries of database `db` with a cursor, tombstones included, for handoff and
    /// reconciliation. Only the entries that `keep` accepts are cloned. It runs under the shard
    /// lock.
    pub fn scan_entries(
        &self,
        db: u32,
        cursor: u64,
        count: usize,
        mut keep: impl FnMut(&Bytes, &Entry) -> bool,
    ) -> (u64, Vec<(Bytes, Entry)>) {
        let mut out = Vec::new();
        let (next, _) = self.walk(db, cursor, count, |k, e| {
            if keep(k, e) {
                out.push((k.clone(), e.clone()));
            }
        });
        (next, out)
    }

    /// [`Store::scan_entries`] over every database in ascending order. `keep` gets the database
    /// too. Returns `None` as the next cursor when the walk is complete. At the end of a
    /// database, the cursor names the next one, so a database that appears during the walk is
    /// seen when its number is still to come.
    pub fn scan_all_entries(
        &self,
        cursor: EntryCursor,
        count: usize,
        mut keep: impl FnMut(u32, &Bytes, &Entry) -> bool,
    ) -> (Option<EntryCursor>, Vec<(DbKey, Entry)>) {
        let mut out = Vec::new();
        let mut cur = cursor;
        let mut left = count.max(1);
        loop {
            let db = cur.db;
            let (next, examined) = self.walk(db, cur.pos, left, |k, e| {
                if keep(db, k, e) {
                    out.push((DbKey::new(db, k.clone()), e.clone()));
                }
            });
            if next != 0 {
                return (Some(EntryCursor { db, pos: next }), out);
            }
            let Some(db) = self.next_database(db) else {
                return (None, out);
            };
            cur = EntryCursor { db, pos: 0 };
            left = left.saturating_sub(examined);
            if left == 0 {
                return (Some(cur), out);
            }
        }
    }

    /// A random live key of database `db`, or `None` when the database is empty.
    pub fn random_key(&self, db: u32) -> Option<Bytes> {
        let now = self.clock.now_ms();
        let n = self.shards.len();
        let first = fastrand::usize(..n);
        for offset in 0..n {
            let guard = self.shards[(first + offset) % n].inner.read();
            let Some(d) = guard.get(db).filter(|d| d.live > 0) else {
                continue;
            };
            let len = d.map.len();
            let start = fastrand::usize(..len);
            let live = d
                .map
                .iter()
                .skip(start)
                .chain(d.map.iter())
                .take(len)
                .find(|(_, e)| !e.tombstone && !e.is_expired(now));
            if let Some((k, _)) = live {
                return Some(k.clone());
            }
        }
        None
    }

    /// Drop every entry and tombstone of every database, and every refill mark.
    pub fn clear(&self) {
        for i in 0..self.shards.len() {
            self.clear_shard(i);
        }
    }
}
