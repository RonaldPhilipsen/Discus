//! The sharded in-memory map.

use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use hashbrown::hash_map::EntryRef;
use hashbrown::HashMap;
use parking_lot::RwLock;
use smallvec::SmallVec;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::entry::estimate_size;
use crate::{Clock, Entry, Value, Version};

/// The seed of the key hash that picks a shard. Placement does not use it.
pub const HASH_SEED: u64 = 0x4449_5343_5553_0001;

/// Reads refresh the access time at most this often per key, so they rarely need the write lock.
const ACCESS_RESOLUTION_MS: u64 = 1_000;

/// The default tombstone TTL: one hour (spec section 8.1).
pub const DEFAULT_TOMBSTONE_TTL_MS: u64 = 3_600_000;

/// The refill bucket of a key: a split of the key space that does not depend on the shard count,
/// so two nodes agree on it.
pub fn refill_bucket(key: &[u8]) -> u8 {
    (xxh3_64_with_seed(key, HASH_SEED) % 256) as u8
}

#[derive(Debug, Clone, Copy)]
pub struct StoreConfig {
    pub shards: usize,
    pub max_memory: u64,
    /// How long a delete marker lives.
    pub tombstone_ttl_ms: u64,
    /// Tombstone bytes above this drop the oldest tombstones (spec section 8.1).
    pub tombstone_reserve: u64,
}

impl Default for StoreConfig {
    fn default() -> StoreConfig {
        StoreConfig {
            shards: 256,
            max_memory: 1 << 30,
            tombstone_ttl_ms: DEFAULT_TOMBSTONE_TTL_MS,
            tombstone_reserve: u64::MAX,
        }
    }
}

/// A key with the database that holds it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DbKey {
    pub db: u32,
    pub key: Bytes,
}

impl DbKey {
    pub fn new(db: u32, key: impl Into<Bytes>) -> DbKey {
        DbKey {
            db,
            key: key.into(),
        }
    }
}

/// Where [`Store::scan_all_entries`] continues: a database and a cursor of that database.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EntryCursor {
    pub db: u32,
    pub pos: u64,
}

/// The keys of one database in one shard.
#[derive(Default)]
pub(crate) struct DbShard {
    /// The default hasher of hashbrown is foldhash with a random seed, so clients cannot choose
    /// keys that collide.
    pub(crate) map: HashMap<Bytes, Entry>,
    /// The tombstones by deadline, so the oldest one is found without a walk.
    pub(crate) tombs: BTreeSet<(u64, Bytes)>,
    /// Live entries in `map`: every entry that is not a tombstone.
    pub(crate) live: usize,
}

/// A database gets its [`DbShard`] at its first insert, so a read or a miss never allocates.
/// A database that loses its last key keeps its (empty) maps until a flush, so a key that comes
/// and goes does not allocate each time.
#[derive(Default)]
pub(crate) struct ShardInner {
    /// Sorted by database. Most shards hold one database, so a lookup is one compare. The first
    /// database stays inline, so its map is found without a load through a heap pointer.
    pub(crate) dbs: SmallVec<[(u32, DbShard); 1]>,
}

impl ShardInner {
    pub(crate) fn get(&self, db: u32) -> Option<&DbShard> {
        let i = self.dbs.binary_search_by_key(&db, |(d, _)| *d).ok()?;
        Some(&self.dbs[i].1)
    }

    pub(crate) fn get_mut(&mut self, db: u32) -> Option<&mut DbShard> {
        let i = self.dbs.binary_search_by_key(&db, |(d, _)| *d).ok()?;
        Some(&mut self.dbs[i].1)
    }

    fn get_or_insert(&mut self, db: u32) -> &mut DbShard {
        let i = match self.dbs.binary_search_by_key(&db, |(d, _)| *d) {
            Ok(i) => i,
            Err(i) => {
                self.dbs.insert(i, (db, DbShard::default()));
                i
            }
        };
        &mut self.dbs[i].1
    }

    /// Every entry of the shard, database by database in ascending order.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (u32, &Bytes, &Entry)> + Clone {
        self.dbs
            .iter()
            .flat_map(|(db, d)| d.map.iter().map(move |(k, e)| (*db, k, e)))
    }

    pub(crate) fn map_len(&self) -> usize {
        self.dbs.iter().map(|(_, d)| d.map.len()).sum()
    }

    pub(crate) fn tomb_len(&self) -> usize {
        self.dbs.iter().map(|(_, d)| d.tombs.len()).sum()
    }
}

pub(crate) struct Shard {
    pub(crate) inner: RwLock<ShardInner>,
    /// Where the next sweep or eviction sample starts within the shard's iteration order.
    pub(crate) cursor: AtomicUsize,
    /// Reads that found a live entry, and reads that did not. Per shard, so reads on different
    /// shards never touch the same counter.
    hits: AtomicU64,
    misses: AtomicU64,
    /// A tombstone of this shard was dropped before its TTL, so a rejoin discards the shard.
    pub(crate) needs_refill: AtomicBool,
}

/// The slot of one key during [`Store::write`]. Reading goes through `Deref`. [`Slot::set`] and
/// every mutable access through `DerefMut`, such as `take` or `as_mut`, count as a change. So a
/// write decides with `as_ref` first and changes the slot only when it must.
pub struct Slot {
    entry: Option<Entry>,
    touched: bool,
}

impl Deref for Slot {
    type Target = Option<Entry>;

    fn deref(&self) -> &Option<Entry> {
        &self.entry
    }
}

impl DerefMut for Slot {
    fn deref_mut(&mut self) -> &mut Option<Entry> {
        self.touched = true;
        &mut self.entry
    }
}

impl Slot {
    /// Replace the entry. This counts as a change.
    pub fn set(&mut self, entry: Option<Entry>) {
        **self = entry;
    }

    /// Mutable access that does not count as a change. The caller reports a change with
    /// [`Slot::touch`].
    pub fn untracked(&mut self) -> &mut Option<Entry> {
        &mut self.entry
    }

    pub fn touch(&mut self) {
        self.touched = true;
    }
}

/// Frees memory held outside the store (hints) when the store runs out of normal entries.
pub type Pressure = Box<dyn Fn(u64) -> u64 + Send + Sync>;

pub struct Store {
    pub(crate) shards: Vec<Shard>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) config: StoreConfig,
    used: AtomicU64,
    /// Live entries, including expired ones that nobody reaped yet. Tombstones are apart.
    len: AtomicUsize,
    tomb_count: AtomicUsize,
    tomb_bytes: AtomicU64,
    pub(crate) evict_shard: AtomicUsize,
    pub(crate) evicted: AtomicU64,
    pub(crate) pressure: OnceLock<Pressure>,
}

impl Store {
    pub fn new(config: StoreConfig, clock: Arc<dyn Clock>) -> Store {
        let count = config.shards.max(1);
        let shards = (0..count)
            .map(|_| Shard {
                inner: RwLock::new(ShardInner::default()),
                cursor: AtomicUsize::new(0),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                needs_refill: AtomicBool::new(false),
            })
            .collect();
        Store {
            shards,
            clock,
            config,
            used: AtomicU64::new(0),
            len: AtomicUsize::new(0),
            tomb_count: AtomicUsize::new(0),
            tomb_bytes: AtomicU64::new(0),
            evict_shard: AtomicUsize::new(0),
            evicted: AtomicU64::new(0),
            pressure: OnceLock::new(),
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    pub fn shard_index(&self, key: &[u8]) -> usize {
        (xxh3_64_with_seed(key, HASH_SEED) as usize) % self.shards.len()
    }

    /// Read an entry. An expired entry or a tombstone reads as absent but is not deleted here.
    pub fn read<R>(&self, db: u32, key: &[u8], f: impl FnOnce(Option<&Entry>) -> R) -> R {
        let shard = &self.shards[self.shard_index(key)];
        let now = self.clock.now_ms();
        let (result, stale) = {
            let guard = shard.inner.read();
            match guard.get(db).and_then(|d| d.map.get(key)) {
                Some(e) if !e.tombstone && !e.is_expired(now) => {
                    shard.hits.fetch_add(1, Ordering::Relaxed);
                    (f(Some(e)), e.last_access_ms + ACCESS_RESOLUTION_MS < now)
                }
                _ => {
                    shard.misses.fetch_add(1, Ordering::Relaxed);
                    (f(None), false)
                }
            }
        };
        if stale {
            let mut guard = shard.inner.write();
            if let Some(e) = guard.get_mut(db).and_then(|d| d.map.get_mut(key)) {
                e.last_access_ms = now;
            }
        }
        result
    }

    /// Modify an entry in place. An expired entry is deleted before `f` runs, and a tombstone
    /// gives `f` an empty slot too. Setting the slot to `None` deletes the key. A changed entry
    /// gets `stamp` as its version. An entry that `f` did not change keeps its version, so a
    /// read-only outcome (`SET NX` on an existing key, a type error) never makes an old value
    /// newer than a write elsewhere.
    ///
    /// A delete of a live entry leaves a tombstone with `stamp`, so the delete replicates and
    /// wins over older copies (spec section 8.1). `Version::zero()` is an internal stamp and
    /// leaves no tombstone.
    pub fn write<R>(
        &self,
        db: u32,
        key: &[u8],
        stamp: Version,
        f: impl FnOnce(&mut Slot) -> R,
    ) -> R {
        self.write_changed(db, key, stamp, f).0
    }

    /// [`Store::write`], and whether `f` changed the slot. An empty slot that stays empty is no
    /// change.
    pub fn write_changed<R>(
        &self,
        db: u32,
        key: &[u8],
        stamp: Version,
        f: impl FnOnce(&mut Slot) -> R,
    ) -> (R, bool) {
        let shard = &self.shards[self.shard_index(key)];
        let now = self.clock.now_ms();
        let mut added_tomb = false;
        let (result, changed) = {
            let mut guard = shard.inner.write();
            // A live entry changes in its bucket, so the common overwrite hashes and probes once.
            if let Some(DbShard { map, tombs, live }) = guard.get_mut(db) {
                if let EntryRef::Occupied(o) = map.entry_ref(key) {
                    if !o.get().tombstone && !o.get().is_expired(now) {
                        let mut out = None;
                        o.replace_entry_with(|k, e| {
                            let before = estimate_size(k, &e);
                            let mut slot = Slot {
                                entry: Some(e),
                                touched: false,
                            };
                            let result = f(&mut slot);
                            let changed = slot.touched;
                            let next = match slot.entry {
                                Some(mut e) => {
                                    e.last_access_ms = now;
                                    if changed {
                                        e.version = stamp;
                                        e.tombstone = false;
                                    }
                                    self.adjust_used(before, estimate_size(k, &e));
                                    Some(e)
                                }
                                None => {
                                    self.adjust_used(before, 0);
                                    self.len.fetch_sub(1, Ordering::Relaxed);
                                    *live -= 1;
                                    (stamp != Version::zero()).then(|| {
                                        let t = self.tombstone(stamp, now);
                                        let size = estimate_size(k, &t);
                                        self.adjust_used(0, size);
                                        self.tomb_count.fetch_add(1, Ordering::Relaxed);
                                        self.tomb_bytes.fetch_add(size, Ordering::Relaxed);
                                        tombs.insert((t.expires_at_ms.unwrap_or(0), k.clone()));
                                        added_tomb = true;
                                        t
                                    })
                                }
                            };
                            out = Some((result, changed));
                            next
                        });
                        drop(guard);
                        if added_tomb {
                            self.enforce_reserve();
                        }
                        return out.expect("the closure of replace_entry_with ran");
                    }
                }
            }
            let (key_bytes, entry, old_tomb) = match self.take(&mut guard, db, key) {
                Some((k, e)) if e.tombstone && !e.is_expired(now) => (Some(k), None, Some(e)),
                Some((k, e)) if e.tombstone || e.is_expired(now) => (Some(k), None, None),
                Some((k, e)) => (Some(k), Some(e), None),
                None => (None, None, None),
            };
            let had_live = entry.is_some();
            let mut slot = Slot {
                entry,
                touched: false,
            };
            let result = f(&mut slot);
            let changed = slot.touched && (had_live || slot.entry.is_some());
            let k = || key_bytes.unwrap_or_else(|| Bytes::copy_from_slice(key));
            match slot.entry {
                Some(mut e) => {
                    e.last_access_ms = now;
                    if changed {
                        e.version = stamp;
                        e.tombstone = false;
                    }
                    self.insert(&mut guard, db, k(), e);
                }
                None if had_live && stamp != Version::zero() => {
                    self.insert(&mut guard, db, k(), self.tombstone(stamp, now));
                    added_tomb = true;
                }
                None => {
                    if let Some(t) = old_tomb {
                        self.insert(&mut guard, db, k(), t);
                    }
                }
            }
            (result, changed)
        };
        if added_tomb {
            self.enforce_reserve();
        }
        (result, changed)
    }

    fn tombstone(&self, stamp: Version, now: u64) -> Entry {
        Entry {
            value: Value::Str(Bytes::new()),
            expires_at_ms: Some(now.saturating_add(self.config.tombstone_ttl_ms)),
            last_access_ms: now,
            version: stamp,
            tombstone: true,
        }
    }

    /// Store a replicated or handed-off entry if its version is newer than the local one,
    /// tombstone or not. The entry keeps its own version and deadline. An entry that is already
    /// expired is not stored. Returns true when the entry was stored.
    pub fn apply(&self, db: u32, key: &[u8], mut entry: Entry) -> bool {
        let now = self.clock.now_ms();
        if entry.is_expired(now) {
            return false;
        }
        entry.last_access_ms = now;
        let tomb = entry.tombstone;
        let size = estimate_size(key, &entry);
        let shard = &self.shards[self.shard_index(key)];
        // One lookup under the lock serves the common case: a newer live entry that fits the
        // budget replaces the live entry in place. Only a stored entry that needs room drops the
        // lock, because `reserve` can evict from any shard.
        {
            let mut guard = shard.inner.write();
            let mut old = 0;
            if let Some(cur) = guard.get_mut(db).and_then(|d| d.map.get_mut(key)) {
                // An expired entry still refuses an older version, or a late copy would bring
                // back a value that a newer write replaced before it expired.
                if cur.version >= entry.version {
                    return false;
                }
                old = estimate_size(key, cur);
                let fits = self.used_memory() + size.saturating_sub(old) <= self.config.max_memory;
                if !tomb && !cur.tombstone && fits {
                    self.adjust_used(old, size);
                    *cur = entry;
                    return true;
                }
            }
            if tomb || self.used_memory() + size.saturating_sub(old) <= self.config.max_memory {
                self.replace(&mut guard, db, key, entry);
                drop(guard);
                if tomb {
                    self.enforce_reserve();
                }
                return true;
            }
        }
        if self.reserve(size) != crate::Admit::Ok {
            return false;
        }
        let mut guard = shard.inner.write();
        if guard
            .get(db)
            .and_then(|d| d.map.get(key))
            .is_some_and(|cur| cur.version >= entry.version)
        {
            return false;
        }
        self.replace(&mut guard, db, key, entry);
        true
    }

    /// Put `entry` in the slot of `key` of a locked shard, with the accounting of both.
    fn replace(&self, inner: &mut ShardInner, db: u32, key: &[u8], entry: Entry) {
        let k = match self.take(inner, db, key) {
            Some((k, _)) => k,
            None => Bytes::copy_from_slice(key),
        };
        self.insert(inner, db, k, entry);
    }

    /// A copy of the entry, tombstone or not. An expired entry is absent.
    pub fn entry(&self, db: u32, key: &[u8]) -> Option<Entry> {
        let now = self.clock.now_ms();
        let shard = &self.shards[self.shard_index(key)];
        let guard = shard.inner.read();
        guard
            .get(db)
            .and_then(|d| d.map.get(key))
            .filter(|e| !e.is_expired(now))
            .cloned()
    }

    /// Remove the entry of `key`, tombstone or not, when `f` accepts it. `f` runs under the
    /// shard lock, so no write to `key` lands between the check and the removal. Returns true
    /// when the entry was removed.
    pub fn remove_if(&self, db: u32, key: &[u8], f: impl FnOnce(&Entry) -> bool) -> bool {
        let shard = &self.shards[self.shard_index(key)];
        let mut guard = shard.inner.write();
        if !guard.get(db).and_then(|d| d.map.get(key)).is_some_and(f) {
            return false;
        }
        self.take(&mut guard, db, key).is_some()
    }

    /// The version of the stored entry, tombstone or not, expired or not.
    pub fn stored_version(&self, db: u32, key: &[u8]) -> Option<Version> {
        let shard = &self.shards[self.shard_index(key)];
        let guard = shard.inner.read();
        guard
            .get(db)
            .and_then(|d| d.map.get(key))
            .map(|e| e.version)
    }

    /// The version of the entry, tombstone or not. An expired entry is absent.
    pub fn version(&self, db: u32, key: &[u8]) -> Option<Version> {
        let now = self.clock.now_ms();
        let shard = &self.shards[self.shard_index(key)];
        let guard = shard.inner.read();
        guard
            .get(db)
            .and_then(|d| d.map.get(key))
            .filter(|e| !e.is_expired(now))
            .map(|e| e.version)
    }

    /// Delete a key with no tombstone. Returns true when a live entry was removed.
    ///
    /// This is for internal deletes. A client `DEL` uses [`Store::write`].
    pub fn remove(&self, db: u32, key: &[u8]) -> bool {
        let now = self.clock.now_ms();
        let shard = &self.shards[self.shard_index(key)];
        let mut guard = shard.inner.write();
        matches!(self.take(&mut guard, db, key), Some((_, e)) if !e.tombstone && !e.is_expired(now))
    }

    /// Remove an entry from a locked shard with its accounting.
    pub(crate) fn take(
        &self,
        inner: &mut ShardInner,
        db: u32,
        key: &[u8],
    ) -> Option<(Bytes, Entry)> {
        self.take_from(inner.get_mut(db)?, key)
    }

    pub(crate) fn take_from(&self, d: &mut DbShard, key: &[u8]) -> Option<(Bytes, Entry)> {
        let (k, e) = d.map.remove_entry(key)?;
        let size = estimate_size(&k, &e);
        self.adjust_used(size, 0);
        if e.tombstone {
            d.tombs.remove(&(e.expires_at_ms.unwrap_or(0), k.clone()));
            self.tomb_count.fetch_sub(1, Ordering::Relaxed);
            self.tomb_bytes.fetch_sub(size, Ordering::Relaxed);
        } else {
            d.live -= 1;
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
        Some((k, e))
    }

    /// Insert an entry into a locked shard whose slot is empty, with its accounting.
    pub(crate) fn insert(&self, inner: &mut ShardInner, db: u32, k: Bytes, e: Entry) {
        let d = inner.get_or_insert(db);
        let size = estimate_size(&k, &e);
        self.adjust_used(0, size);
        if e.tombstone {
            d.tombs.insert((e.expires_at_ms.unwrap_or(0), k.clone()));
            self.tomb_count.fetch_add(1, Ordering::Relaxed);
            self.tomb_bytes.fetch_add(size, Ordering::Relaxed);
        } else {
            d.live += 1;
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        d.map.insert(k, e);
    }

    /// Take away the accounting of a database shard that was removed from its shard. Returns
    /// the number of entries it held, tombstones included.
    fn discard(&self, d: DbShard) -> usize {
        let mut bytes = 0;
        let mut tomb_bytes = 0;
        for (k, e) in &d.map {
            let size = estimate_size(k, e);
            bytes += size;
            if e.tombstone {
                tomb_bytes += size;
            }
        }
        self.adjust_used(bytes, 0);
        self.len.fetch_sub(d.live, Ordering::Relaxed);
        self.tomb_count.fetch_sub(d.tombs.len(), Ordering::Relaxed);
        self.tomb_bytes.fetch_sub(tomb_bytes, Ordering::Relaxed);
        d.map.len()
    }

    /// Live entries in every database, including expired entries that nobody reaped yet.
    /// Tombstones do not count.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Live entries of one database, as [`Store::len`] counts them.
    pub fn db_len(&self, db: u32) -> usize {
        self.shards
            .iter()
            .map(|s| s.inner.read().get(db).map_or(0, |d| d.live))
            .sum()
    }

    /// The databases that hold an entry or a tombstone, in ascending order.
    pub fn databases(&self) -> Vec<u32> {
        let mut out = BTreeSet::new();
        for shard in &self.shards {
            let guard = shard.inner.read();
            out.extend(
                guard
                    .dbs
                    .iter()
                    .filter(|(_, d)| !d.map.is_empty())
                    .map(|(db, _)| *db),
            );
        }
        out.into_iter().collect()
    }

    /// The lowest database above `after` that holds an entry or a tombstone.
    pub(crate) fn next_database(&self, after: u32) -> Option<u32> {
        self.shards
            .iter()
            .filter_map(|s| {
                let guard = s.inner.read();
                let from = guard.dbs.partition_point(|(db, _)| *db <= after);
                guard.dbs[from..]
                    .iter()
                    .find(|(_, d)| !d.map.is_empty())
                    .map(|(db, _)| *db)
            })
            .min()
    }

    /// Drop every entry and tombstone of one database.
    pub fn clear_db(&self, db: u32) {
        for shard in &self.shards {
            let removed = {
                let mut guard = shard.inner.write();
                match guard.dbs.binary_search_by_key(&db, |(d, _)| *d) {
                    Ok(i) => guard.dbs.remove(i).1,
                    Err(_) => continue,
                }
            };
            self.discard(removed);
        }
    }

    /// Drop every database from `count` up, for a node that now has `count` databases. Returns
    /// the number of entries dropped, tombstones included.
    pub fn drop_databases_from(&self, count: u32) -> usize {
        let mut dropped = 0;
        for shard in &self.shards {
            let removed = {
                let mut guard = shard.inner.write();
                let from = guard.dbs.partition_point(|(db, _)| *db < count);
                guard.dbs.drain(from..).collect::<Vec<_>>()
            };
            dropped += removed
                .into_iter()
                .map(|(_, d)| self.discard(d))
                .sum::<usize>();
        }
        dropped
    }

    /// Stored tombstones.
    pub fn tombstones(&self) -> usize {
        self.tomb_count.load(Ordering::Relaxed)
    }

    pub fn tombstone_bytes(&self) -> u64 {
        self.tomb_bytes.load(Ordering::Relaxed)
    }

    /// Reads that found a live entry since start.
    pub fn hits(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.hits.load(Ordering::Relaxed))
            .sum()
    }

    /// Reads that found no live entry since start.
    pub fn misses(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.misses.load(Ordering::Relaxed))
            .sum()
    }

    /// Bytes of entries, tombstones, and everything charged with [`Store::charge`].
    pub fn used_memory(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// Count memory held outside the store, such as hints and write sets, in the one budget
    /// (spec section 11). The next [`Store::reserve`] makes room for it.
    pub fn charge(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn release(&self, bytes: u64) {
        self.adjust_used(bytes, 0);
    }

    /// Set the hook that frees outside memory under pressure. It gets the bytes wanted and
    /// returns the bytes freed. Only the first call has an effect.
    pub fn set_pressure(&self, f: Pressure) {
        let _ = self.pressure.set(f);
    }

    /// Shards that dropped a tombstone before its TTL.
    pub fn needs_refill(&self) -> Vec<usize> {
        self.shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.needs_refill.load(Ordering::Relaxed))
            .map(|(i, _)| i)
            .collect()
    }

    /// Drop every entry and tombstone of shard `i`, in every database, and clear its refill
    /// mark.
    pub fn clear_shard(&self, i: usize) {
        let Some(shard) = self.shards.get(i) else {
            return;
        };
        let removed = std::mem::take(&mut shard.inner.write().dbs);
        for (_, d) in removed {
            self.discard(d);
        }
        shard.needs_refill.store(false, Ordering::Relaxed);
    }

    /// Keep only the keys for which `f` is true, in every database, tombstones included. `f`
    /// gets the user key.
    pub fn retain(&self, f: impl Fn(&[u8]) -> bool) {
        for shard in &self.shards {
            let mut guard = shard.inner.write();
            for (_, d) in &mut guard.dbs {
                let drop: Vec<Bytes> = d.map.keys().filter(|k| !f(k)).cloned().collect();
                for k in drop {
                    self.take_from(d, &k);
                }
            }
        }
    }

    /// Drop the oldest tombstones while their bytes exceed the reserve.
    fn enforce_reserve(&self) {
        while self.tombstone_bytes() > self.config.tombstone_reserve {
            if !self.drop_oldest_tombstone() {
                break;
            }
        }
    }

    /// Drop the tombstone with the earliest deadline, in any database, and mark its shard
    /// `needs_refill`.
    pub(crate) fn drop_oldest_tombstone(&self) -> bool {
        let oldest = self
            .shards
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                let guard = s.inner.read();
                guard
                    .dbs
                    .iter()
                    .filter_map(|(db, d)| d.tombs.first().map(|(t, _)| (*t, *db)))
                    .min()
                    .map(|(t, db)| (t, i, db))
            })
            .min();
        let Some((_, i, db)) = oldest else {
            return false;
        };
        let shard = &self.shards[i];
        let mut guard = shard.inner.write();
        let Some(d) = guard.get_mut(db) else {
            return true;
        };
        let Some((_, key)) = d.tombs.first().cloned() else {
            return true;
        };
        self.take_from(d, &key);
        shard.needs_refill.store(true, Ordering::Relaxed);
        true
    }

    pub(crate) fn adjust_used(&self, before: u64, after: u64) {
        if after >= before {
            self.used.fetch_add(after - before, Ordering::Relaxed);
        } else {
            let less = before - after;
            let mut cur = self.used.load(Ordering::Relaxed);
            while let Err(seen) = self.used.compare_exchange_weak(
                cur,
                cur.saturating_sub(less),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                cur = seen;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use discus_placement::ManualClock;

    use super::*;

    #[test]
    fn an_unused_database_creates_no_map() {
        let s = Store::new(
            StoreConfig {
                shards: 16,
                ..StoreConfig::default()
            },
            Arc::new(ManualClock::new(0)),
        );
        let stamp = Version::new(crate::NTP64(1), crate::NodeId::from_u128(1));
        for i in 0..64 {
            let key = format!("k{i}");
            assert!(!s.read(7, key.as_bytes(), |e| e.is_some()));
            assert_eq!(s.version(7, key.as_bytes()), None);
            assert_eq!(s.entry(7, key.as_bytes()), None);
            let (_, changed) = s.write_changed(7, key.as_bytes(), stamp, |slot| slot.take());
            assert!(!changed);
        }
        assert_eq!(s.scan(7, 0, 100, None), (0, Vec::new()));
        assert_eq!(s.random_key(7), None);
        for shard in &s.shards {
            assert!(shard.inner.read().dbs.is_empty());
        }
    }
}
