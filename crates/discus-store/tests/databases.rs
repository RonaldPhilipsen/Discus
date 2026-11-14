use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use discus_placement::ManualClock;
use discus_store::{
    estimate_size, Admit, DbKey, Entry, EntryCursor, NodeId, Store, StoreConfig, Value, Version,
    NTP64,
};

fn store(clock: &Arc<ManualClock>) -> Store {
    let cfg = StoreConfig {
        shards: 16,
        max_memory: 1 << 30,
        ..StoreConfig::default()
    };
    Store::new(cfg, clock.clone())
}

fn str_entry(v: &str) -> Entry {
    Entry::new(Value::Str(Bytes::copy_from_slice(v.as_bytes())), None)
}

fn put(s: &Store, db: u32, key: &[u8], v: &str) {
    s.write(db, key, Version::zero(), |slot| {
        slot.set(Some(str_entry(v)))
    });
}

fn value(s: &Store, db: u32, key: &[u8]) -> Option<Bytes> {
    s.read(db, key, |e| match e.map(|e| &e.value) {
        Some(Value::Str(b)) => Some(b.clone()),
        _ => None,
    })
}

fn v(t: u64) -> Version {
    Version::new(NTP64(t), NodeId::from_u128(1))
}

#[test]
fn the_same_key_holds_two_values_in_two_databases() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    put(&s, 0, b"k", "a");
    put(&s, 1, b"k", "b");
    assert_eq!(value(&s, 0, b"k").as_deref(), Some(&b"a"[..]));
    assert_eq!(value(&s, 1, b"k").as_deref(), Some(&b"b"[..]));
    assert_eq!(s.db_len(0), 1);
    assert_eq!(s.db_len(1), 1);
    assert_eq!(s.len(), 2);
}

#[test]
fn a_flush_of_one_database_keeps_the_others() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    for db in 0..3 {
        put(&s, db, b"k", "v");
    }
    let before = s.used_memory();
    s.clear_db(1);
    assert_eq!(s.db_len(1), 0);
    assert_eq!(s.db_len(0), 1);
    assert_eq!(s.db_len(2), 1);
    assert_eq!(s.len(), 2);
    assert_eq!(
        s.used_memory(),
        before - estimate_size(b"k", &str_entry("v"))
    );
    assert_eq!(s.databases(), vec![0, 2]);
}

#[test]
fn an_unused_database_holds_no_memory() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    assert!(!s.read(7, b"k", |e| e.is_some()));
    assert_eq!(s.version(7, b"k"), None);
    let (_, changed) = s.write_changed(7, b"k", v(1), |slot| slot.take());
    assert!(!changed);
    assert!(s.databases().is_empty());
    assert_eq!(s.used_memory(), 0);
    assert_eq!(s.db_len(7), 0);
}

#[test]
fn scan_sees_only_its_database() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    for i in 0..500 {
        put(&s, 1, format!("big{i}").as_bytes(), "v");
    }
    let small: HashSet<Bytes> = (0..3).map(|i| Bytes::from(format!("small{i}"))).collect();
    for k in &small {
        put(&s, 2, k, "v");
    }
    let mut seen = Vec::new();
    let mut cursor = 0;
    loop {
        let (next, keys) = s.scan(2, cursor, 2, None);
        seen.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    assert_eq!(seen.len(), 3);
    assert_eq!(seen.into_iter().collect::<HashSet<_>>(), small);
    assert!(small.contains(&s.random_key(2).expect("database 2 holds keys")));
    assert_eq!(s.random_key(5), None);
}

#[test]
fn scan_all_entries_visits_every_database() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let mut want = HashSet::new();
    for db in [0, 4, 9] {
        for i in 0..10 {
            let key = format!("k{i}");
            put(&s, db, key.as_bytes(), "v");
            want.insert(DbKey::new(db, key));
        }
    }
    let mut seen = Vec::new();
    let mut cursor = EntryCursor::default();
    for _ in 0..1_000 {
        let (next, page) = s.scan_all_entries(cursor, 3, |_, _, _| true);
        seen.extend(page.into_iter().map(|(k, _)| k));
        match next {
            Some(c) => cursor = c,
            None => break,
        }
    }
    assert_eq!(seen.len(), 30);
    assert_eq!(seen.into_iter().collect::<HashSet<_>>(), want);
}

#[test]
fn apply_compares_versions_per_database() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let mut newer = str_entry("new");
    newer.version = v(2);
    let mut older = str_entry("old");
    older.version = v(1);
    assert!(s.apply(1, b"k", newer));
    assert!(s.apply(0, b"k", older));
    assert_eq!(s.version(0, b"k"), Some(v(1)));
    assert_eq!(s.version(1, b"k"), Some(v(2)));
}

#[test]
fn the_sweep_reaps_every_database() {
    let clock = Arc::new(ManualClock::new(1_000));
    let s = store(&clock);
    s.write(3, b"k", Version::zero(), |slot| {
        slot.set(Some(Entry::new(Value::Str("v".into()), Some(1_010))));
    });
    assert_eq!(s.db_len(3), 1);
    clock.advance(20);
    for _ in 0..100 {
        if s.sweep_expired(20) == 0 && s.db_len(3) == 0 {
            break;
        }
    }
    assert_eq!(s.db_len(3), 0);
    assert_eq!(s.len(), 0);
    assert_eq!(s.used_memory(), 0);
}

#[test]
fn eviction_finds_keys_in_any_database() {
    let clock = Arc::new(ManualClock::new(0));
    let cfg = StoreConfig {
        shards: 4,
        max_memory: 10_000,
        ..StoreConfig::default()
    };
    let s = Store::new(cfg, clock.clone());
    for i in 0..8 {
        clock.advance(1);
        s.write(2, format!("k{i}").as_bytes(), Version::zero(), |slot| {
            slot.set(Some(Entry::new(Value::Str(vec![0u8; 1_000].into()), None)));
        });
    }
    let before = s.db_len(2);
    assert_eq!(s.reserve(3_000), Admit::Ok);
    assert!(s.used_memory() + 3_000 <= 10_000);
    assert!(s.evictions() >= 1);
    assert_eq!(s.db_len(2), before - s.evictions() as usize);
}

#[test]
fn drop_databases_from_removes_the_high_databases() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    for db in 0..3 {
        put(&s, db, b"k", "v");
    }
    assert_eq!(s.drop_databases_from(1), 2);
    assert_eq!(s.databases(), vec![0]);
    assert_eq!(s.len(), 1);
}

#[test]
fn a_tombstone_counts_in_its_database_only() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    put(&s, 2, b"k", "v");
    assert!(s.write(2, b"k", v(5), |slot| slot.take().is_some()));
    assert_eq!(s.db_len(2), 0);
    assert_eq!(s.tombstones(), 1);
    assert_eq!(s.databases(), vec![2]);
    assert_eq!(s.version(0, b"k"), None);
    assert_eq!(s.version(2, b"k"), Some(v(5)));
}
