use discus_store::Version;
use discus_store::{Entry, ManualClock, Store, StoreConfig, Value, HASH_SEED};
use std::sync::Arc;

#[test]
fn sweep_removes_a_share_of_expired_entries_without_touching_live_ones() {
    let clock = Arc::new(ManualClock::new(0));
    let cfg = StoreConfig {
        shards: 4,
        max_memory: 1 << 30,
        seed: HASH_SEED,
    };
    let s = Store::new(cfg, clock.clone());
    for i in 0..400 {
        let key = format!("k{i}");
        let expires = if i % 2 == 0 { Some(10) } else { None };
        s.write(key.as_bytes(), Version::zero(), |slot| {
            *slot = Some(Entry::new(Value::Str("v".into()), expires));
        });
    }
    clock.advance(20);
    let removed = s.sweep_expired(50);
    assert!(removed > 0 && removed <= 200);
    assert_eq!(s.len(), 400 - removed);
    for i in (1..400).step_by(2) {
        assert!(s.read(format!("k{i}").as_bytes(), |e| e.is_some()));
    }
}

#[test]
fn repeated_sweeps_eventually_cover_every_expired_entry() {
    let clock = Arc::new(ManualClock::new(0));
    let cfg = StoreConfig {
        shards: 2,
        max_memory: 1 << 30,
        seed: HASH_SEED,
    };
    let s = Store::new(cfg, clock.clone());
    for i in 0..300 {
        s.write(format!("k{i}").as_bytes(), Version::zero(), |slot| {
            *slot = Some(Entry::new(Value::Str("v".into()), Some(5)));
        });
    }
    clock.advance(10);
    for _ in 0..20 {
        s.sweep_expired(25);
    }
    assert_eq!(s.len(), 0);
    assert_eq!(s.used_memory(), 0);
}
