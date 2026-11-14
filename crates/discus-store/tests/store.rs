use discus_placement::ManualClock;
use discus_store::Version;
use discus_store::{Entry, Store, StoreConfig, Value};
use std::sync::Arc;

fn store(clock: &Arc<ManualClock>) -> Store {
    let cfg = StoreConfig {
        shards: 16,
        max_memory: 1 << 30,
        ..StoreConfig::default()
    };
    Store::new(cfg, clock.clone())
}

#[test]
fn shard_index_is_stable_and_within_range() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let a = s.shard_index(b"user:1");
    assert_eq!(a, s.shard_index(b"user:1"));
    assert!(a < 16);
    assert_eq!(s.shard_index(b"user:1"), 15);
}

#[test]
fn write_then_read_round_trips_and_expired_reads_as_absent() {
    let clock = Arc::new(ManualClock::new(1_000));
    let s = store(&clock);
    s.write(0, b"k", Version::zero(), |slot| {
        slot.set(Some(Entry::new(Value::Str("v".into()), Some(2_000))));
    });
    assert!(s.read(0, b"k", |e| e.is_some()));
    clock.advance(1_000);
    assert!(s.read(0, b"k", |e| e.is_none()));
    assert_eq!(s.len(), 1);
    assert!(!s.write(0, b"k", Version::zero(), |slot| slot.is_some()));
    assert_eq!(s.len(), 0);
}

#[test]
fn used_memory_tracks_inserts_and_removes() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let before = s.used_memory();
    s.write(0, b"big", Version::zero(), |slot| {
        slot.set(Some(Entry::new(Value::Str(vec![0u8; 4096].into()), None)));
    });
    assert!(s.used_memory() >= before + 4096);
    assert!(s.remove(0, b"big"));
    assert_eq!(s.used_memory(), before);
}

#[test]
fn write_stamps_the_entry() {
    use discus_store::{NodeId, NTP64};
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let origin = NodeId::from_u128(3);
    let v1 = Version::new(NTP64(10), origin);
    let v2 = Version::new(NTP64(11), origin);
    s.write(0, b"k", v1, |slot| {
        slot.set(Some(Entry::new(Value::Str("a".into()), None)));
    });
    assert_eq!(s.read(0, b"k", |e| e.map(|e| e.version)), Some(v1));
    s.write(0, b"k", v2, |slot| {
        if let Some(e) = slot.as_mut() {
            e.value = Value::Str("b".into());
        }
    });
    assert_eq!(s.read(0, b"k", |e| e.map(|e| e.version)), Some(v2));
}

#[test]
fn reads_count_hits_and_misses() {
    let clock = Arc::new(ManualClock::new(1_000));
    let s = store(&clock);
    s.write(0, b"k", Version::zero(), |slot| {
        slot.set(Some(Entry::new(Value::Str("v".into()), Some(2_000))));
    });
    s.read(0, b"k", |_| ());
    s.read(0, b"k", |_| ());
    s.read(0, b"absent", |_| ());
    assert_eq!((s.hits(), s.misses()), (2, 1));
    // An expired entry reads as a miss.
    clock.advance(5_000);
    s.read(0, b"k", |_| ());
    assert_eq!((s.hits(), s.misses()), (2, 2));
}

/// Deletes, rewrites, and inserts that grow the map between steps must not hide a key that is
/// present for the whole walk (the `SCAN` guarantee, which handoff relies on too).
#[test]
fn a_walk_sees_every_key_that_stays_despite_deletes_and_growth() {
    let clock = Arc::new(ManualClock::new(0));
    let s = Store::new(
        StoreConfig {
            shards: 2,
            ..StoreConfig::default()
        },
        clock,
    );
    let put = |k: &str| {
        s.write(0, k.as_bytes(), Version::zero(), |slot| {
            slot.set(Some(Entry::new(Value::Str("v".into()), None)));
        })
    };
    let stays: Vec<String> = (0..500).map(|i| format!("stay{i}")).collect();
    let goes: Vec<String> = (0..500).map(|i| format!("go{i}")).collect();
    for k in stays.iter().chain(&goes) {
        put(k);
    }
    let mut seen = std::collections::HashSet::new();
    let mut cursor = 0;
    let mut round = 0;
    loop {
        let (next, keys) = s.scan(0, cursor, 40, None);
        seen.extend(keys.iter().map(|k| String::from_utf8(k.to_vec()).unwrap()));
        if next == 0 {
            break;
        }
        cursor = next;
        for k in goes.iter().skip(round * 5).take(5) {
            s.write(0, k.as_bytes(), Version::zero(), |slot| slot.take());
        }
        for i in 0..10 {
            put(&format!("new{round}:{i}"));
        }
        put(&stays[round % stays.len()]);
        round += 1;
    }
    let missed: Vec<&String> = stays.iter().filter(|k| !seen.contains(*k)).collect();
    assert!(
        missed.is_empty(),
        "missed {} keys: {missed:?}",
        missed.len()
    );
}

#[test]
fn a_write_that_changes_nothing_keeps_the_version() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let v1 = Version::new(discus_store::NTP64(1), discus_store::NodeId::from_u128(1));
    let v2 = Version::new(discus_store::NTP64(2), discus_store::NodeId::from_u128(1));
    s.write(0, b"k", v1, |slot| {
        slot.set(Some(Entry::new(Value::Str("a".into()), None)));
    });
    let (seen, changed) = s.write_changed(0, b"k", v2, |slot| slot.is_some());
    assert!(seen && !changed);
    assert_eq!(s.version(0, b"k"), Some(v1));
    let (_, changed) = s.write_changed(0, b"gone", v2, |slot| slot.take());
    assert!(!changed);
    assert_eq!(s.version(0, b"gone"), None);
    let (_, changed) = s.write_changed(0, b"k", v2, |slot| {
        if let Some(e) = slot.as_mut() {
            e.expires_at_ms = Some(10);
        }
    });
    assert!(changed);
    assert_eq!(s.version(0, b"k"), Some(v2));
}
