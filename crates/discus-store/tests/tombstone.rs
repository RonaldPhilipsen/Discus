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
    };
    (Store::new(cfg, clock.clone()), clock)
}

fn v(t: u64) -> Version {
    Version::new(NTP64(t), NodeId::from_u128(7))
}

fn put(s: &Store, key: &str, stamp: Version) {
    s.write(0, key.as_bytes(), stamp, |slot| {
        slot.set(Some(Entry::new(Value::Str("x".into()), None)))
    });
}

fn del(s: &Store, key: &str, stamp: Version) -> bool {
    s.write(0, key.as_bytes(), stamp, |slot| slot.take().is_some())
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
    assert!(!s.read(0, b"k", |e| e.is_some()));
    let t = s.entry(0, b"k").unwrap();
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
    assert!(s.entry(0, b"k").is_none());
    assert_eq!(s.tombstones(), 0);
}

#[test]
fn a_write_over_a_tombstone_sees_an_empty_slot() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(10));
    del(&s, "k", v(20));
    let saw = s.write(0, b"k", v(30), |slot| slot.is_some());
    assert!(!saw);
    put(&s, "k", v(40));
    assert!(s.read(0, b"k", |e| e.is_some()));
    assert_eq!(s.tombstones(), 0);
    assert_eq!(s.len(), 1);
}

#[test]
fn scan_and_random_key_skip_tombstones() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "dead", v(1));
    del(&s, "dead", v(2));
    let (_, keys) = s.scan(0, 0, 100, None);
    assert!(keys.is_empty());
    assert_eq!(s.random_key(0), None);
    put(&s, "live", v(3));
    let (_, keys) = s.scan(0, 0, 100, None);
    assert_eq!(keys, vec![bytes::Bytes::from_static(b"live")]);
    let (_, entries) = s.scan_entries(0, 0, 100, |_, _| true);
    assert_eq!(entries.len(), 2);
}

#[test]
fn apply_takes_only_a_newer_version() {
    let (s, _) = store(1 << 20, 1 << 20);
    assert!(s.apply(0, b"k", value("b", v(20))));
    assert!(!s.apply(0, b"k", value("a", v(10))));
    assert_eq!(s.entry(0, b"k").unwrap().version, v(20));
    assert!(s.apply(0, b"k", value("c", v(30))));
    let mut tomb = value("", v(40));
    tomb.tombstone = true;
    tomb.expires_at_ms = Some(1_000 + TTL);
    assert!(s.apply(0, b"k", tomb));
    assert!(!s.read(0, b"k", |e| e.is_some()));
    assert_eq!(s.version(0, b"k"), Some(v(40)));

    // Any arrival order ends in the same entry.
    let orders = [[10, 20, 30], [30, 10, 20], [20, 30, 10]];
    for (i, order) in orders.iter().enumerate() {
        let key = format!("o{i}");
        for t in order {
            s.apply(0, key.as_bytes(), value(&t.to_string(), v(*t)));
        }
        assert_eq!(
            s.entry(0, key.as_bytes()).unwrap().value,
            Value::Str("30".into())
        );
    }
}

#[test]
fn apply_never_revives_a_deleted_key() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(10));
    del(&s, "k", v(20));
    assert!(!s.apply(0, b"k", value("old", v(15))));
    assert!(!s.read(0, b"k", |e| e.is_some()));
}

#[test]
fn apply_keeps_the_expiry_deadline() {
    let (s, clock) = store(1 << 20, 1 << 20);
    let mut e = value("x", v(10));
    e.expires_at_ms = Some(5_000);
    assert!(s.apply(0, b"k", e));
    assert_eq!(s.entry(0, b"k").unwrap().expires_at_ms, Some(5_000));
    clock.set(5_000);
    assert!(s.entry(0, b"k").is_none());
    let mut late = value("y", v(20));
    late.expires_at_ms = Some(4_000);
    assert!(!s.apply(0, b"j", late));
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
    assert!(s.entry(0, b"t0").is_none());
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
    assert!(!store_ref.read(0, b"live", |e| e.is_some()));
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
    assert!(s.entry(0, b"k").is_none());
    assert!(s.used_memory() == 0);
}

#[test]
fn a_write_over_a_live_entry_keeps_the_accounting() {
    let (s, clock) = store(1 << 20, 1 << 20);
    let (fresh, _) = store(1 << 20, 1 << 20);
    let set = |s: &Store, n: usize, stamp: Version| {
        s.write_changed(0, b"k", stamp, |slot| {
            slot.set(Some(Entry::new(Value::Str(vec![b'x'; n].into()), None)));
        })
    };
    set(&s, 4096, v(1));
    assert!(set(&s, 10, v(2)).1);
    set(&fresh, 10, v(2));
    assert_eq!(s.used_memory(), fresh.used_memory());
    assert_eq!(s.entry(0, b"k").unwrap().version, v(2));

    // A write that only reads changes nothing and keeps the version.
    let (len, changed) = s.write_changed(0, b"k", v(3), |slot| slot.as_ref().map(|_| 1));
    assert_eq!((len, changed), (Some(1), false));
    assert_eq!(s.entry(0, b"k").unwrap().version, v(2));
    assert_eq!(s.used_memory(), fresh.used_memory());

    assert!(del(&s, "k", v(4)));
    del(&fresh, "k", v(4));
    assert_eq!((s.len(), s.tombstones()), (0, 1));
    assert_eq!(s.used_memory(), fresh.used_memory());
    assert_eq!(s.tombstone_bytes(), fresh.tombstone_bytes());
    clock.advance(TTL);
    s.sweep_expired(1);
    assert_eq!((s.tombstones(), s.used_memory()), (0, 0));
}

#[test]
fn an_internal_delete_of_a_live_entry_leaves_no_tombstone() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(1));
    assert!(del(&s, "k", Version::zero()));
    assert!(s.entry(0, b"k").is_none());
    assert_eq!((s.len(), s.tombstones(), s.used_memory()), (0, 0, 0));
}

#[test]
fn an_internal_remove_leaves_no_tombstone() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "k", v(1));
    assert!(s.remove(0, b"k"));
    assert!(s.entry(0, b"k").is_none());
}

#[test]
fn retain_and_clear_drop_tombstones_too() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "a", v(1));
    put(&s, "b", v(1));
    del(&s, "b", v(2));
    s.retain(|k| k != b"a");
    assert!(s.entry(0, b"a").is_none());
    assert!(s.entry(0, b"b").is_some());
    s.clear();
    assert_eq!(s.tombstones(), 0);
    assert_eq!(s.used_memory(), 0);
}

#[test]
fn apply_keeps_the_accounting_exact_through_every_transition() {
    let (s, _) = store(1 << 20, 1 << 20);
    let mut tomb = value("", v(30));
    tomb.tombstone = true;
    tomb.expires_at_ms = Some(1_000 + TTL);
    let steps = [
        value("a", v(10)),
        value("a much longer value than before", v(20)),
        tomb,
        value("back", v(40)),
    ];
    for e in steps {
        let (fresh, _) = store(1 << 20, 1 << 20);
        assert!(s.apply(0, b"k", e.clone()));
        assert!(fresh.apply(0, b"k", e));
        assert_eq!(s.used_memory(), fresh.used_memory());
        assert_eq!(s.len(), fresh.len());
        assert_eq!(s.tombstones(), fresh.tombstones());
        assert_eq!(s.tombstone_bytes(), fresh.tombstone_bytes());
    }
}

#[test]
fn a_stale_apply_never_evicts() {
    let (s, _) = store(4_000, 1 << 20);
    for i in 0..1_000 {
        s.apply(0, format!("k{i}").as_bytes(), value("0123456789", v(10)));
    }
    let evicted = s.evictions();
    assert!(evicted > 0, "the store must be full for this test");
    let mut big = value(&"x".repeat(2_000), v(5));
    big.version = v(5);
    let key = (0..1_000)
        .map(|i| format!("k{i}"))
        .find(|k| s.entry(0, k.as_bytes()).is_some())
        .unwrap();
    assert!(!s.apply(0, key.as_bytes(), big));
    assert_eq!(s.evictions(), evicted);
}

#[test]
fn a_newer_apply_makes_room_under_memory_pressure() {
    let (s, _) = store(4_000, 1 << 20);
    for i in 0..1_000 {
        s.apply(0, format!("k{i}").as_bytes(), value("0123456789", v(10)));
    }
    let key = (0..1_000)
        .map(|i| format!("k{i}"))
        .find(|k| s.entry(0, k.as_bytes()).is_some())
        .unwrap();
    let evicted = s.evictions();
    assert!(s.apply(0, key.as_bytes(), value(&"y".repeat(1_000), v(20))));
    assert!(s.evictions() > evicted);
    assert!(s.used_memory() <= 4_000);
    assert_eq!(s.entry(0, key.as_bytes()).unwrap().version, v(20));
}

#[test]
fn remove_if_checks_the_entry_and_keeps_the_accounting() {
    let (s, _) = store(1 << 20, 1 << 20);
    put(&s, "a", v(1));
    put(&s, "b", v(1));
    del(&s, "b", v(2));
    assert!(!s.remove_if(0, b"a", |e| e.version == v(9)));
    assert!(s.entry(0, b"a").is_some());
    assert!(s.remove_if(0, b"a", |e| e.version == v(1)));
    assert!(s.remove_if(0, b"b", |e| e.version == v(2)));
    assert!(!s.remove_if(0, b"missing", |_| true));
    assert_eq!((s.len(), s.tombstones(), s.used_memory()), (0, 0, 0));
}

#[test]
fn an_expired_entry_still_refuses_an_older_version() {
    let (s, clock) = store(1 << 20, 1 << 20);
    let mut newer = value("new", v(20));
    newer.expires_at_ms = Some(s.now_ms() + 10);
    assert!(s.apply(0, b"k", newer));
    clock.advance(20);
    assert!(s.entry(0, b"k").is_none());
    assert_eq!(s.stored_version(0, b"k"), Some(v(20)));
    assert!(!s.apply(0, b"k", value("old", v(10))));
    assert!(s.entry(0, b"k").is_none());
    assert!(s.apply(0, b"k", value("newest", v(30))));
}
