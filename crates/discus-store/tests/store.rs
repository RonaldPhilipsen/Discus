use discus_store::{Clock, Entry, ManualClock, Store, StoreConfig, Value, HASH_SEED};
use std::sync::Arc;

fn store(clock: &Arc<ManualClock>) -> Store {
    let cfg = StoreConfig {
        shards: 16,
        max_memory: 1 << 30,
        seed: HASH_SEED,
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
    s.write(b"k", |slot| {
        *slot = Some(Entry {
            value: Value::Str("v".into()),
            expires_at_ms: Some(2_000),
            last_access_ms: clock.now_ms(),
        });
    });
    assert!(s.read(b"k", |e| e.is_some()));
    clock.advance(1_000);
    assert!(s.read(b"k", |e| e.is_none()));
    assert_eq!(s.len(), 1);
    assert!(!s.write(b"k", |slot| slot.is_some()));
    assert_eq!(s.len(), 0);
}

#[test]
fn used_memory_tracks_inserts_and_removes() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let before = s.used_memory();
    s.write(b"big", |slot| {
        *slot = Some(Entry {
            value: Value::Str(vec![0u8; 4096].into()),
            expires_at_ms: None,
            last_access_ms: 0,
        });
    });
    assert!(s.used_memory() >= before + 4096);
    assert!(s.remove(b"big"));
    assert_eq!(s.used_memory(), before);
}

#[test]
fn reads_count_hits_and_misses() {
    let clock = Arc::new(ManualClock::new(1_000));
    let s = store(&clock);
    s.write(b"k", |slot| {
        *slot = Some(Entry {
            value: Value::Str("v".into()),
            expires_at_ms: Some(2_000),
            last_access_ms: 0,
        });
    });
    s.read(b"k", |_| ());
    s.read(b"k", |_| ());
    s.read(b"absent", |_| ());
    assert_eq!((s.hits(), s.misses()), (2, 1));
    // An expired entry reads as a miss.
    clock.advance(5_000);
    s.read(b"k", |_| ());
    assert_eq!((s.hits(), s.misses()), (2, 2));
}
