//! The record of one connection: each key that its writes changed, with the version of the key
//! after the write. `WAIT` offers these versions to the owners of the keys.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use discus_placement::Version;
use foldhash::fast::RandomState;

use super::writeset::{Held, WriteSetBudget, WriteSetLimits};

/// The bookkeeping cost of one entry on top of the key bytes.
const ENTRY_OVERHEAD: usize = 64;

type Keys = HashMap<Bytes, Version, RandomState>;

pub struct Record {
    /// The entries by database. A connection writes to few databases, so a scan finds one.
    dbs: Vec<(u32, Keys)>,
    len: usize,
    /// The bytes of the entries, with [`ENTRY_OVERHEAD`] each.
    bytes: usize,
    /// A cap or the budget refused an entry since the last [`Record::clear`].
    overflow: bool,
    max_keys: usize,
    max_bytes: usize,
    held: Held,
}

impl Record {
    pub fn new(limits: &WriteSetLimits, budget: Arc<WriteSetBudget>) -> Record {
        Record {
            dbs: Vec::new(),
            len: 0,
            bytes: 0,
            overflow: false,
            max_keys: limits.keys.max(1),
            max_bytes: limits.bytes,
            held: Held::new(budget),
        }
    }

    /// Note that a write changed `key` in database `db` to `version`. An entry that a cap or the
    /// budget refuses ends every entry and marks the overflow: no write leaves without notice.
    pub fn note(&mut self, db: u32, key: &Bytes, version: Version) {
        if self.overflow {
            return;
        }
        let i = match self.dbs.iter().position(|(d, _)| *d == db) {
            Some(i) => i,
            None => {
                self.dbs.push((db, Keys::default()));
                self.dbs.len() - 1
            }
        };
        if let Some(v) = self.dbs[i].1.get_mut(&key[..]) {
            *v = version;
            return;
        }
        let size = key.len() + ENTRY_OVERHEAD;
        let bytes = self.bytes + size;
        if self.len >= self.max_keys || bytes > self.max_bytes || !self.held.cover(bytes) {
            self.clear();
            self.overflow = true;
            return;
        }
        self.dbs[i].1.insert(key.clone(), version);
        self.len += 1;
        self.bytes = bytes;
    }

    /// Drop the entries of database `db`. The overflow stays.
    pub fn clear_db(&mut self, db: u32) {
        let Some(i) = self.dbs.iter().position(|(d, _)| *d == db) else {
            return;
        };
        let (_, keys) = self.dbs.swap_remove(i);
        self.len -= keys.len();
        self.bytes -= keys.keys().map(|k| k.len() + ENTRY_OVERHEAD).sum::<usize>();
        self.held.trim(self.bytes);
    }

    /// Drop every entry and end the overflow.
    pub fn clear(&mut self) {
        self.dbs = Vec::new();
        self.len = 0;
        self.bytes = 0;
        self.overflow = false;
        self.held.trim(0);
    }

    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    pub fn entries(&self) -> impl Iterator<Item = (u32, &Bytes, Version)> {
        self.dbs
            .iter()
            .flat_map(|(db, keys)| keys.iter().map(move |(k, v)| (*db, k, *v)))
    }

    pub fn remove(&mut self, db: u32, key: &[u8]) {
        let Some(i) = self.dbs.iter().position(|(d, _)| *d == db) else {
            return;
        };
        if self.dbs[i].1.remove(key).is_none() {
            return;
        }
        if self.dbs[i].1.is_empty() {
            self.dbs.swap_remove(i);
        }
        self.len -= 1;
        self.bytes -= key.len() + ENTRY_OVERHEAD;
        self.held.trim(self.bytes);
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("keys", &self.len)
            .field("bytes", &self.bytes)
            .field("overflow", &self.overflow)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use discus_placement::{NodeId, NTP64};
    use std::time::Duration;

    fn v(t: u64) -> Version {
        Version::new(NTP64(t), NodeId::from_u128(1))
    }

    fn record(keys: usize, bytes: usize, cap: u64) -> (Record, Arc<WriteSetBudget>) {
        let budget = Arc::new(WriteSetBudget::new(cap, None));
        let limits = WriteSetLimits {
            keys,
            bytes,
            max_age: Duration::from_secs(5),
        };
        (Record::new(&limits, budget.clone()), budget)
    }

    fn k(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn entries(r: &Record) -> Vec<(u32, Bytes, Version)> {
        let mut all: Vec<_> = r.entries().map(|(d, k, v)| (d, k.clone(), v)).collect();
        all.sort();
        all
    }

    #[test]
    fn a_rewrite_replaces_the_entry() {
        let (mut r, _) = record(10, 1 << 20, u64::MAX);
        r.note(0, &k("a"), v(1));
        r.note(0, &k("a"), v(2));
        assert_eq!(entries(&r), vec![(0, k("a"), v(2))]);
    }

    #[test]
    fn the_key_cap_drops_every_entry_and_overflows() {
        let (mut r, budget) = record(2, 1 << 20, u64::MAX);
        r.note(0, &k("a"), v(1));
        r.note(0, &k("b"), v(2));
        assert!(!r.overflowed());
        r.note(0, &k("c"), v(3));
        assert!(r.overflowed());
        assert!(r.is_empty());
        assert_eq!(budget.used(), 0);
        r.note(0, &k("d"), v(4));
        assert!(r.is_empty());
    }

    #[test]
    fn the_byte_cap_and_the_budget_overflow() {
        let (mut r, _) = record(10, 2 * (1 + ENTRY_OVERHEAD), u64::MAX);
        for s in ["a", "b", "c"] {
            r.note(0, &k(s), v(1));
        }
        assert!(r.overflowed() && r.is_empty());
        let (mut r, budget) = record(10, 1 << 20, (1 + ENTRY_OVERHEAD) as u64);
        r.note(0, &k("a"), v(1));
        assert!(!r.overflowed());
        r.note(0, &k("b"), v(1));
        assert!(r.overflowed() && r.is_empty());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn clear_resets_the_record() {
        let (mut r, budget) = record(1, 1 << 20, u64::MAX);
        r.note(0, &k("a"), v(1));
        r.note(0, &k("b"), v(1));
        assert!(r.overflowed());
        r.clear();
        assert!(!r.overflowed() && r.is_empty());
        r.note(0, &k("c"), v(2));
        assert_eq!(entries(&r), vec![(0, k("c"), v(2))]);
        r.clear();
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn clear_db_keeps_other_databases() {
        let (mut r, _) = record(10, 1 << 20, u64::MAX);
        r.note(0, &k("a"), v(1));
        r.note(1, &k("a"), v(2));
        r.note(2, &k("b"), v(3));
        r.clear_db(1);
        assert_eq!(entries(&r), vec![(0, k("a"), v(1)), (2, k("b"), v(3))]);
        r.note(1, &k("c"), v(4));
        assert_eq!(r.entries().count(), 3);
    }

    #[test]
    fn remove_drops_one_entry() {
        let (mut r, budget) = record(10, 1 << 20, u64::MAX);
        r.note(0, &k("a"), v(1));
        r.note(0, &k("b"), v(2));
        r.remove(0, b"a");
        r.remove(1, b"b");
        assert_eq!(entries(&r), vec![(0, k("b"), v(2))]);
        r.remove(0, b"b");
        assert!(r.is_empty());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn the_budget_comes_back_on_drop() {
        let (mut r, budget) = record(10, 1 << 20, u64::MAX);
        r.note(0, &k("a"), v(1));
        assert!(budget.used() > 0);
        drop(r);
        assert_eq!(budget.used(), 0);
    }
}
