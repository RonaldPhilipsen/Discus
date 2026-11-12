use discus_placement::ManualClock;
use discus_store::Version;
use discus_store::{Admit, Entry, Store, StoreConfig, Value};
use std::sync::Arc;

fn entry(size: usize) -> Entry {
    Entry::new(Value::Str(vec![0u8; size].into()), None)
}

#[test]
fn reserve_evicts_oldest_first_and_stays_under_budget() {
    let clock = Arc::new(ManualClock::new(0));
    let cfg = StoreConfig {
        shards: 1,
        max_memory: 10_000,
        ..StoreConfig::default()
    };
    let s = Store::new(cfg, clock.clone());
    for i in 0..8u64 {
        // Each write stamps the access time with the clock, so k0 is oldest and k7 is newest.
        clock.advance(1);
        s.write(format!("k{i}").as_bytes(), Version::zero(), |slot| {
            slot.set(Some(entry(1_000)))
        });
    }
    assert_eq!(s.reserve(3_000), Admit::Ok);
    assert!(s.used_memory() + 3_000 <= 10_000);
    assert!(s.read(b"k7", |e| e.is_some()));
    assert!(s.evictions() >= 2);
}

#[test]
fn a_value_larger_than_maxmemory_is_rejected_not_evicted_for() {
    let clock = Arc::new(ManualClock::new(0));
    let cfg = StoreConfig {
        shards: 1,
        max_memory: 10_000,
        ..StoreConfig::default()
    };
    let s = Store::new(cfg, clock.clone());
    s.write(b"keep", Version::zero(), |slot| slot.set(Some(entry(100))));
    assert_eq!(s.reserve(20_000), Admit::TooLarge);
    assert!(s.read(b"keep", |e| e.is_some()));
    assert_eq!(s.evictions(), 0);
}

#[test]
fn reserve_on_an_empty_store_does_not_loop() {
    let clock = Arc::new(ManualClock::new(0));
    let cfg = StoreConfig {
        shards: 4,
        max_memory: 10_000,
        ..StoreConfig::default()
    };
    let s = Store::new(cfg, clock);
    assert_eq!(s.reserve(9_000), Admit::Ok);
}
