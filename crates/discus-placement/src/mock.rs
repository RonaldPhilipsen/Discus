//! Process-wide mock clocks for tests, behind the `mock-clock` feature.
//!
//! `uhlc` takes its clock as a plain `fn() -> NTP64`, which cannot capture state. Each slot is a
//! static atomic millisecond counter, and [`mock_clock::<SLOT>`] reads it. A single-node test sets
//! the time of its slot. An in-process multi-node test gives each node its own slot, so each node
//! has its own, possibly skewed, clock. [`MockClock<SLOT>`] reads the same slot through the store
//! [`Clock`] trait, so the TTL clock and the HLC agree.
//!
//! Tests in one binary run in parallel and share the slots, so each test uses its own slot.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use uhlc::NTP64;

use crate::Clock;

pub const MOCK_SLOTS: usize = 32;

static SLOTS: [AtomicU64; MOCK_SLOTS] = [const { AtomicU64::new(0) }; MOCK_SLOTS];

/// The time of slot `SLOT` as NTP64, for `Hlc::new`.
pub fn mock_clock<const SLOT: usize>() -> NTP64 {
    NTP64::from(Duration::from_millis(MockClock::<SLOT>.now_ms()))
}

/// A handle on slot `SLOT`. It implements the store [`Clock`] trait.
#[derive(Debug, Clone, Copy, Default)]
pub struct MockClock<const SLOT: usize>;

impl<const SLOT: usize> MockClock<SLOT> {
    const CHECK: () = assert!(SLOT < MOCK_SLOTS, "mock clock slot out of range");

    fn slot() -> &'static AtomicU64 {
        #[allow(clippy::let_unit_value)]
        let () = Self::CHECK;
        &SLOTS[SLOT]
    }

    /// Jump to `ms` Unix milliseconds. This can move backwards, to model an NTP step.
    pub fn set(&self, ms: u64) {
        Self::slot().store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, ms: u64) {
        Self::slot().fetch_add(ms, Ordering::SeqCst);
    }

    /// The HLC clock function of this slot.
    pub fn hlc_clock(&self) -> fn() -> NTP64 {
        mock_clock::<SLOT>
    }
}

impl<const SLOT: usize> Clock for MockClock<SLOT> {
    fn now_ms(&self) -> u64 {
        Self::slot().load(Ordering::SeqCst)
    }
}

/// A clock that only moves when a test advances it.
#[derive(Debug)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(start_ms: u64) -> ManualClock {
        ManualClock(AtomicU64::new(start_ms))
    }

    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }

    /// Jump to `ms`. Unlike a real clock source, this can move backwards, to model an NTP step.
    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
