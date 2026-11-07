use discus_store::Version;
use discus_store::{Entry, ManualClock, Store, StoreConfig, Value, HASH_SEED};
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
    s.write(b"k", Version::ZERO, |slot| {
        *slot = Some(Entry::new(Value::Str("v".into()), Some(2_000)));
    });
    assert!(s.read(b"k", |e| e.is_some()));
    clock.advance(1_000);
    assert!(s.read(b"k", |e| e.is_none()));
    assert_eq!(s.len(), 1);
    assert!(!s.write(b"k", Version::ZERO, |slot| slot.is_some()));
    assert_eq!(s.len(), 0);
}

#[test]
fn used_memory_tracks_inserts_and_removes() {
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let before = s.used_memory();
    s.write(b"big", Version::ZERO, |slot| {
        *slot = Some(Entry::new(Value::Str(vec![0u8; 4096].into()), None));
    });
    assert!(s.used_memory() >= before + 4096);
    assert!(s.remove(b"big"));
    assert_eq!(s.used_memory(), before);
}

#[test]
fn write_stamps_the_entry() {
    use discus_store::{NodeId, Timestamp, Version};
    let clock = Arc::new(ManualClock::new(0));
    let s = store(&clock);
    let origin = NodeId::from_u128(3);
    let v1 = Version {
        ts: Timestamp::new(10, 0),
        origin,
    };
    let v2 = Version {
        ts: Timestamp::new(10, 1),
        origin,
    };
    s.write(b"k", v1, |slot| {
        *slot = Some(Entry::new(Value::Str("a".into()), None));
    });
    assert_eq!(s.read(b"k", |e| e.map(|e| e.version)), Some(v1));
    s.write(b"k", v2, |slot| {
        if let Some(e) = slot {
            e.value = Value::Str("b".into());
        }
    });
    assert_eq!(s.read(b"k", |e| e.map(|e| e.version)), Some(v2));
}
