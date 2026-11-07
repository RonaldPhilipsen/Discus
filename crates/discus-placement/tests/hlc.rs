#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use discus_placement::mock::{mock_clock, MockClock};
use discus_placement::{Clock, Hlc, HlcError, NodeId, Version, MAX_DRIFT, NTP64};

const START: u64 = 1_700_000_000_000;

fn ms(n: u64) -> NTP64 {
    NTP64::from(Duration::from_millis(n))
}

/// uhlc clears the low 4 bits of the physical time for its counter, so a fresh timestamp sits up
/// to 15 NTP64 units below the wall clock reading.
fn assert_near(t: NTP64, wall: NTP64) {
    assert!(
        t <= wall && wall.as_u64() - t.as_u64() < 16,
        "{t:?} vs {wall:?}"
    );
}

/// A fresh HLC on clock slot `SLOT`. Every test uses its own slot, because tests run in
/// parallel and the slots are process-wide.
fn hlc<const SLOT: usize>(origin: u128) -> Hlc {
    MockClock::<SLOT>.set(START);
    Hlc::new(NodeId::from_u128(origin), mock_clock::<SLOT>)
}

#[test]
fn versions_order_by_time_then_origin() {
    let a = Version::new(ms(10), NodeId::from_u128(1));
    let b = Version::new(ms(10), NodeId::from_u128(2));
    assert_ne!(a, b);
    assert!(a < b || b < a);
    let later = Version::new(ms(11), NodeId::from_u128(1));
    assert!(a < later && b < later);
    assert_eq!(a.origin(), NodeId::from_u128(1));
    assert_eq!(a.time(), ms(10));
    assert!(Version::zero() < a);
}

#[test]
fn the_origin_round_trips_through_the_uhlc_id() {
    let id = NodeId::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    assert_eq!(Version::new(ms(1), id).origin(), id);
    assert_eq!(
        Version::new(ms(1), id).as_uhlc().get_id().to_le_bytes(),
        id.as_u128().to_le_bytes()
    );
}

#[test]
fn ticks_follow_the_wall_clock() {
    let h = hlc::<0>(7);
    let v = h.tick().unwrap();
    assert_eq!(v.origin(), NodeId::from_u128(7));
    assert_eq!(h.origin(), NodeId::from_u128(7));
    assert_near(v.time(), ms(START));
    MockClock::<0>.advance(5);
    let w = h.tick().unwrap();
    assert_near(w.time(), ms(START + 5));
    assert_eq!(h.last(), w.time());
}

#[test]
fn ticks_in_one_millisecond_increase() {
    let h = hlc::<1>(7);
    let mut prev = h.tick().unwrap();
    for _ in 0..70_000 {
        let next = h.tick().unwrap();
        assert!(next > prev);
        prev = next;
    }
    assert!(prev.time() < ms(START) + MAX_DRIFT);
}

#[test]
fn observe_moves_the_clock_forward() {
    let h = hlc::<2>(7);
    let remote = Version::new(ms(START + 500), NodeId::from_u128(9));
    h.observe(remote).unwrap();
    assert!(h.tick().unwrap().time() > remote.time());
}

#[test]
fn observe_rejects_more_than_one_second_ahead() {
    assert_eq!(MAX_DRIFT, ms(1_000));
    let h = hlc::<3>(7);
    let before = h.last();
    let far = Version::new(ms(START + 1_001), NodeId::from_u128(9));
    assert_eq!(
        h.observe(far),
        Err(HlcError::TooFarAhead { ahead_ms: 1_001 })
    );
    assert_eq!(h.last(), before);
    h.observe(Version::new(ms(START + 1_000), NodeId::from_u128(9)))
        .unwrap();
}

#[test]
fn at_the_bound_the_clock_is_pinned() {
    let h = hlc::<4>(7);
    assert!(!h.is_pinned());
    h.observe(Version::new(ms(START + 1_000), NodeId::from_u128(9)))
        .unwrap();
    assert!(h.is_pinned());
    assert_eq!(h.tick(), Err(HlcError::Pinned));
    assert_eq!(h.refusals(), 1);
    MockClock::<4>.advance(1);
    assert!(!h.is_pinned());
    let v = h.tick().unwrap();
    assert!(v.time() > ms(START + 1_000));
    assert!(v.time() < ms(START + 1) + MAX_DRIFT);
}

#[test]
fn a_backward_wall_step_pins_the_clock() {
    let h = hlc::<5>(7);
    h.tick().unwrap();
    MockClock::<5>.set(START - 2_000);
    assert!(h.is_pinned());
    assert_eq!(h.tick(), Err(HlcError::Pinned));
    MockClock::<5>.set(START - 999);
    assert!(!h.is_pinned());
    assert!(h.tick().unwrap().time() > ms(START));
}

#[test]
fn set_floor_raises_the_clock() {
    let h = hlc::<6>(7);
    let floor = ms(START + 800);
    h.set_floor(floor).unwrap();
    assert!(h.tick().unwrap().time() > floor);
    assert_eq!(
        h.set_floor(ms(START + 1_500)),
        Err(HlcError::TooFarAhead { ahead_ms: 1_500 })
    );
}

#[test]
fn an_old_observation_changes_nothing() {
    let h = hlc::<7>(7);
    let v = h.tick().unwrap();
    h.observe(Version::new(ms(START - 10), NodeId::from_u128(9)))
        .unwrap();
    assert_eq!(h.last(), v.time());
}

#[test]
fn concurrent_ticks_are_unique() {
    let h = Arc::new(hlc::<8>(7));
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

#[test]
fn two_nodes_with_skewed_clocks_converge() {
    // Per-node clocks in one process go through separate slots.
    MockClock::<9>.set(START);
    MockClock::<10>.set(START + 300);
    let a = Hlc::new(NodeId::from_u128(1), mock_clock::<9>);
    let b = Hlc::new(NodeId::from_u128(2), mock_clock::<10>);
    let from_b = b.tick().unwrap();
    a.observe(from_b).unwrap();
    assert!(a.tick().unwrap() > from_b);
    assert_eq!(MockClock::<9>.now_ms(), START);
}

#[test]
fn the_system_clock_is_near_now() {
    let h = Hlc::new(NodeId::from_u128(1), discus_placement::hlc::system_clock);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let t = h.tick().unwrap().time().to_duration();
    assert!(t.abs_diff(now) < Duration::from_secs(5));
}
