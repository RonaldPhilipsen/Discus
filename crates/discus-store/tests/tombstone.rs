#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use discus_placement::ManualClock;
use discus_store::{Entry, NodeId, Store, StoreConfig, Value, Version, NTP64};

const TTL: u64 = 60_000;

fn store(max_memory: u64, reserve: u64) -> (Store, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(1_000));
    let cfg = StoreConfig {
        shards: 4,
        max_memory,
        tombstone_ttl_ms: TTL,
        tombstone_reserve: reserve,
        ..StoreConfig::default()
    };
    (Store::new(cfg, clock.clone()), clock)
}

fn v(t: u64) -> Version {
    Version::new(NTP64(t), NodeId::from_u128(7))
}

fn put(s: &Store, key: &str, stamp: Version) {
    s.write(key.as_bytes(), stamp, |slot| {
        slot.set(Some(Entry::new(Value::Str("x".into()), None)))
    });
}

fn del(s: &Store, key: &str, stamp: Version) -> bool {
    s.write(key.as_bytes(), stamp, |slot| slot.take().is_some())
}

fn value(s: &str, version: Version) -> Entry {
    let mut e = Entry::new(Value::Str(s.to_string().into()), None);
    e.version = version;
    e
}

#[test]
fn a_delete_leaves_a_tombstone() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(10));
    assert!(del(&s, "k", v(20)));
    assert!(!s.read(b"k", |e| e.is_some()));
    let t = s.entry(b"k").unwrap();
    assert!(t.tombstone);
    assert_eq!(t.version, v(20));
    assert_eq!(t.expires_at_ms, Some(1_000 + TTL));
    assert_eq!(s.len(), 0);
    assert_eq!(s.tombstones(), 1);
}

#[test]
fn a_delete_of_a_missing_key_leaves_nothing() {
    let (s, _) = store(1 << 20, 1 << 20);
    assert!(!del(&s, "k", v(20)));
    assert!(s.entry(b"k").is_none());
    assert_eq!(s.tombstones(), 0);
}

#[test]
fn a_write_over_a_tombstone_sees_an_empty_slot() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(10));
    del(&s, "k", v(20));
    let saw = s.write(b"k", v(30), |slot| slot.is_some());
    assert!(!saw);
    put(&s, "k", v(40));
    assert!(s.read(b"k", |e| e.is_some()));
    assert_eq!(s.tombstones(), 0);
    assert_eq!(s.len(), 1);
}

#[test]
fn scan_and_random_key_skip_tombstones() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "dead", v(1));
    del(&s, "dead", v(2));
    let (_, keys) = s.scan(0, 100, None);
    assert!(keys.is_empty());
    assert_eq!(s.random_key(), None);
    put(&s, "live", v(3));
    let (_, keys) = s.scan(0, 100, None);
    assert_eq!(keys, vec![bytes::Bytes::from_static(b"live")]);
    let (_, entries) = s.scan_entries(0, 100);
    assert_eq!(entries.len(), 2);
}

#[test]
fn apply_takes_only_a_newer_version() {
    let (s, _) = store(1 << 20, 1 << 20);
    assert!(s.apply(b"k", value("b", v(20))));
    assert!(!s.apply(b"k", value("a", v(10))));
    assert_eq!(s.entry(b"k").unwrap().version, v(20));
    assert!(s.apply(b"k", value("c", v(30))));
    let mut tomb = value("", v(40));
    tomb.tombstone = true;
    tomb.expires_at_ms = Some(1_000 + TTL);
    assert!(s.apply(b"k", tomb));
    assert!(!s.read(b"k", |e| e.is_some()));
    assert_eq!(s.version(b"k"), Some(v(40)));

    // Any arrival order ends in the same entry.
    let orders = [[10, 20, 30], [30, 10, 20], [20, 30, 10]];
    for (i, order) in orders.iter().enumerate() {
        let key = format!("o{i}");
        for t in order {
            s.apply(key.as_bytes(), value(&t.to_string(), v(*t)));
        }
        assert_eq!(
            s.entry(key.as_bytes()).unwrap().value,
            Value::Str("30".into())
        );
    }
}

#[test]
fn apply_never_revives_a_deleted_key() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(10));
    del(&s, "k", v(20));
    assert!(!s.apply(b"k", value("old", v(15))));
    assert!(!s.read(b"k", |e| e.is_some()));
}

#[test]
fn apply_keeps_the_expiry_deadline() {
    let (s, clock) = store(1 << 20, 1 << 20);
    let mut e = value("x", v(10));
    e.expires_at_ms = Some(5_000);
    assert!(s.apply(b"k", e));
    assert_eq!(s.entry(b"k").unwrap().expires_at_ms, Some(5_000));
    clock.set(5_000);
    assert!(s.entry(b"k").is_none());
    let mut late = value("y", v(20));
    late.expires_at_ms = Some(4_000);
    assert!(!s.apply(b"j", late));
}

#[test]
fn eviction_never_takes_a_tombstone() {
    let (s, _) = store(20_000, 1 << 20);
    for i in 0..10 {
        put(&s, &format!("t{i}"), v(i));
        del(&s, &format!("t{i}"), v(100 + i));
    }
    for i in 0..200 {
        s.reserve(200);
        put(&s, &format!("k{i}"), v(1_000 + i));
    }
    assert!(s.used_memory() <= 20_000 + 200);
    assert_eq!(s.tombstones(), 10);
    assert!(s.needs_refill().is_empty());
}

#[test]
fn a_full_reserve_drops_the_oldest_tombstone_and_marks_its_shard() {
    let one = {
        let (s, _) = store(1 << 20, 1 << 20);
        put(&s, "t0", v(1));
        del(&s, "t0", v(2));
        s.used_memory()
    };
    let (s, clock) = store(1 << 20, one * 3);
    for i in 0..3 {
        clock.advance(10);
        put(&s, &format!("t{i}"), v(i));
        del(&s, &format!("t{i}"), v(10 + i));
    }
    assert_eq!(s.tombstones(), 3);
    assert!(s.needs_refill().is_empty());
    clock.advance(10);
    put(&s, "t3", v(3));
    del(&s, "t3", v(13));
    assert_eq!(s.tombstones(), 3);
    assert!(s.entry(b"t0").is_none());
    assert_eq!(s.needs_refill(), vec![s.shard_index(b"t0")]);
    s.clear_shard(s.shard_index(b"t0"));
    assert!(s.needs_refill().is_empty());
}

#[test]
fn pressure_runs_entries_then_the_hook_then_tombstones() {
    let (s, _) = store(10_000, 1 << 20);
    for i in 0..5 {
        put(&s, &format!("t{i}"), v(i));
        del(&s, &format!("t{i}"), v(10 + i));
    }
    let freed = Arc::new(AtomicU64::new(0));
    let hook_freed = freed.clone();
    s.charge(3_000);
    let charged = Arc::new(AtomicU64::new(3_000));
    let left = charged.clone();
    let store_ref: &'static Store = Box::leak(Box::new(s));
    store_ref.set_pressure(Box::new(move |want| {
        let have = left.load(Ordering::Relaxed);
        let n = want.min(have);
        left.fetch_sub(n, Ordering::Relaxed);
        hook_freed.fetch_add(n, Ordering::Relaxed);
        store_ref.release(n);
        n
    }));
    put(store_ref, "live", v(100));
    // The live entry goes first, then the hook frees its bytes, and the tombstones survive.
    let room = 10_000 - store_ref.used_memory() + 1_000;
    store_ref.reserve(room);
    assert!(!store_ref.read(b"live", |e| e.is_some()));
    assert!(freed.load(Ordering::Relaxed) > 0);
    assert_eq!(store_ref.tombstones(), 5);
    // With the hook empty, the oldest tombstones go and mark their shards.
    store_ref.reserve(10_000 - 100);
    assert!(store_ref.tombstones() < 5);
    assert!(!store_ref.needs_refill().is_empty());
}

#[test]
fn charged_bytes_count_in_the_budget() {
    let (s, _) = store(10_000, 1 << 20);
    for i in 0..5 {
        put(&s, &format!("k{i}"), v(i));
    }
    let before = s.used_memory();
    s.charge(9_800);
    assert_eq!(s.used_memory(), before + 9_800);
    s.reserve(100);
    assert!(s.used_memory() + 100 <= 10_000);
    assert!(s.len() < 5);
    s.release(9_800);
}

#[test]
fn the_sweep_removes_expired_tombstones() {
    let (s, clock) = store(1 << 20, 1 << 20);
    put(&s, "k", v(1));
    del(&s, "k", v(2));
    clock.advance(TTL);
    s.sweep_expired(1);
    assert_eq!(s.tombstones(), 0);
    assert!(s.entry(b"k").is_none());
    assert!(s.used_memory() == 0);
}

#[test]
fn an_internal_remove_leaves_no_tombstone() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(1));
    assert!(s.remove(b"k"));
    assert!(s.entry(b"k").is_none());
}

#[test]
fn retain_and_clear_drop_tombstones_too() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "a", v(1));
    put(&s, "b", v(1));
    del(&s, "b", v(2));
    s.retain(|k| k != b"a");
    assert!(s.entry(b"a").is_none());
    assert!(s.entry(b"b").is_some());
    s.clear();
    assert_eq!(s.tombstones(), 0);
    assert_eq!(s.used_memory(), 0);
}
