#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use discus_placement::{Hlc, HlcError, ManualClock, NodeId, Timestamp, Version, MAX_DRIFT_MS};

const START: u64 = 1_000_000;

fn hlc() -> (Hlc, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(START));
    (Hlc::new(NodeId::from_u128(7), clock.clone()), clock)
}

#[test]
fn timestamps_order_like_wall_then_logical() {
    assert!(Timestamp::new(5, 9) < Timestamp::new(6, 0));
    assert!(Timestamp::new(5, 1) < Timestamp::new(5, 2));
    let t = Timestamp::new(123_456, 789);
    assert_eq!((t.wall_ms(), t.logical()), (123_456, 789));
    assert_eq!(Timestamp::from_raw(t.raw()), t);
    assert!(Timestamp::ZERO < t);
}

#[test]
fn versions_break_ties_by_origin() {
    let ts = Timestamp::new(10, 0);
    let a = Version {
        ts,
        origin: NodeId::from_u128(1),
    };
    let b = Version {
        ts,
        origin: NodeId::from_u128(2),
    };
    assert!(a < b);
    let later = Version {
        ts: Timestamp::new(11, 0),
        origin: NodeId::from_u128(1),
    };
    assert!(b < later);
    assert!(Version::ZERO < a);
}

#[test]
fn ticks_follow_the_wall_clock() {
    let (h, clock) = hlc();
    let v = h.tick().unwrap();
    assert_eq!(v.origin, NodeId::from_u128(7));
    assert_eq!(h.origin(), NodeId::from_u128(7));
    assert_eq!((v.ts.wall_ms(), v.ts.logical()), (START, 0));
    clock.advance(5);
    let w = h.tick().unwrap();
    assert_eq!((w.ts.wall_ms(), w.ts.logical()), (START + 5, 0));
    assert_eq!(h.last(), w.ts);
}

#[test]
fn ticks_in_one_millisecond_increase() {
    let (h, _clock) = hlc();
    let mut prev = h.tick().unwrap();
    for _ in 0..70_000 {
        let next = h.tick().unwrap();
        assert!(next > prev);
        assert!(next.ts.wall_ms() < START + MAX_DRIFT_MS);
        prev = next;
    }
    assert_eq!(prev.ts.wall_ms(), START + 1);
}

#[test]
fn observe_moves_the_clock_forward() {
    let (h, _clock) = hlc();
    let remote = Timestamp::new(START + 500, 3);
    h.observe(remote).unwrap();
    assert!(h.tick().unwrap().ts > remote);
}

#[test]
fn observe_rejects_more_than_one_second_ahead() {
    assert_eq!(MAX_DRIFT_MS, 1_000);
    let (h, _clock) = hlc();
    let before = h.last();
    assert_eq!(
        h.observe(Timestamp::new(START + 1_001, 0)),
        Err(HlcError::TooFarAhead { ahead_ms: 1_001 })
    );
    assert_eq!(h.last(), before);
    h.observe(Timestamp::new(START + 1_000, 0)).unwrap();
}

#[test]
fn at_the_bound_the_clock_is_pinned() {
    let (h, clock) = hlc();
    assert!(!h.is_pinned());
    h.observe(Timestamp::new(START + 1_000, 0)).unwrap();
    assert!(h.is_pinned());
    assert_eq!(h.tick(), Err(HlcError::Pinned));
    assert_eq!(h.refusals(), 1);
    clock.advance(1);
    assert!(!h.is_pinned());
    let v = h.tick().unwrap();
    assert!(v.ts > Timestamp::new(START + 1_000, 0));
    assert!(v.ts.wall_ms() < START + 1 + MAX_DRIFT_MS);
}

#[test]
fn a_backward_wall_step_pins_the_clock() {
    let (h, clock) = hlc();
    h.tick().unwrap();
    clock.set(START - 2_000);
    assert!(h.is_pinned());
    assert_eq!(h.tick(), Err(HlcError::Pinned));
    clock.set(START - 999);
    assert!(!h.is_pinned());
    assert!(h.tick().unwrap().ts > Timestamp::new(START, 0));
}

#[test]
fn set_floor_raises_the_clock() {
    let (h, _clock) = hlc();
    let floor = Timestamp::new(START + 800, 0);
    h.set_floor(floor).unwrap();
    assert!(h.tick().unwrap().ts > floor);
    assert_eq!(
        h.set_floor(Timestamp::new(START + 1_500, 0)),
        Err(HlcError::TooFarAhead { ahead_ms: 1_500 })
    );
}

#[test]
fn an_old_observation_changes_nothing() {
    let (h, _clock) = hlc();
    let v = h.tick().unwrap();
    h.observe(Timestamp::new(START - 10, 0)).unwrap();
    assert_eq!(h.last(), v.ts);
}

#[test]
fn concurrent_ticks_are_unique() {
    let (h, _clock) = hlc();
    let h = Arc::new(h);
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let h = h.clone();
            std::thread::spawn(move || (0..5_000).map(|_| h.tick().unwrap()).collect::<Vec<_>>())
        })
        .collect();
    let mut all: Vec<Version> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    let n = all.len();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), n);
}
