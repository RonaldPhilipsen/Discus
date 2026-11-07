//! Time source, replaceable in tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of Unix time in milliseconds.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
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
