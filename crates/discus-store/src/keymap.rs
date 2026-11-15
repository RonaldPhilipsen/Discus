//! The map of one database in one shard, with access to its buckets by index.
//!
//! Eviction and the TTL sweep read entries at a bucket index, so a sample costs the buckets it
//! reads, not a walk from the start of the map. `HashMap` of hashbrown hides its buckets, so the
//! map is a `HashTable` of pairs with its own hasher.

use bytes::Bytes;
use hashbrown::hash_table::{Entry as Slot, OccupiedEntry};
use hashbrown::{DefaultHashBuilder, HashTable};
use std::hash::BuildHasher;

use crate::Entry;

/// The default hasher of hashbrown is foldhash with a random seed, so clients cannot choose keys
/// that collide.
#[derive(Default)]
pub(crate) struct KeyMap {
    table: HashTable<(Bytes, Entry)>,
    hasher: DefaultHashBuilder,
}

impl KeyMap {
    fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.table
            .find(self.hash(key), |(k, _)| k.as_ref() == key)
            .map(|(_, e)| e)
    }

    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let hash = self.hash(key);
        self.table
            .find_mut(hash, |(k, _)| k.as_ref() == key)
            .map(|(_, e)| e)
    }

    /// The occupied bucket of `key`, to change the entry in place or to remove it.
    pub(crate) fn find_entry(&mut self, key: &[u8]) -> Option<OccupiedEntry<'_, (Bytes, Entry)>> {
        let hash = self.hash(key);
        self.table.find_entry(hash, |(k, _)| k.as_ref() == key).ok()
    }

    pub(crate) fn remove_entry(&mut self, key: &[u8]) -> Option<(Bytes, Entry)> {
        self.find_entry(key).map(|o| o.remove().0)
    }

    /// Insert or replace the entry of `key`.
    pub(crate) fn insert(&mut self, key: Bytes, entry: Entry) {
        let hash = self.hash(&key);
        let hasher = &self.hasher;
        match self.table.entry(
            hash,
            |(k, _)| *k == key,
            |(k, _)| hasher.hash_one(k.as_ref()),
        ) {
            Slot::Occupied(mut o) => o.get_mut().1 = entry,
            Slot::Vacant(v) => {
                v.insert((key, entry));
            }
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Bytes, &Entry)> + Clone {
        self.table.iter().map(|(k, e)| (k, e))
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.table.iter().map(|(k, _)| k)
    }

    /// The bucket count. A bucket index is below it. Removals never move other entries, and only
    /// growth moves them, so an index stays a valid place to continue from.
    pub(crate) fn num_buckets(&self) -> usize {
        self.table.num_buckets()
    }

    /// The entry in bucket `index`, or `None` when the bucket is empty.
    pub(crate) fn bucket(&self, index: usize) -> Option<(&Bytes, &Entry)> {
        self.table.get_bucket(index).map(|(k, e)| (k, e))
    }
}
