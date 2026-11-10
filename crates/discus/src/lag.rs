//! A probe for the lag of the data runtime.
//!
//! A task on the data runtime wakes every [`PERIOD`] and records when it ran and how late it
//! was. A blocked runtime cannot publish anything, so the age of the last beat shows a stall
//! while it lasts. The lateness of the last tick shows a stall that has just ended.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const PERIOD: Duration = Duration::from_millis(10);
const NO_BEAT: u64 = u64::MAX;

struct Inner {
    start: Instant,
    last_beat_ms: AtomicU64,
    last_late_ms: AtomicU64,
}

#[derive(Clone)]
pub struct LagProbe(Arc<Inner>);

impl Default for LagProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl LagProbe {
    pub fn new() -> LagProbe {
        LagProbe(Arc::new(Inner {
            start: Instant::now(),
            last_beat_ms: AtomicU64::new(NO_BEAT),
            last_late_ms: AtomicU64::new(0),
        }))
    }

    /// Start the probe task on the current runtime.
    pub fn spawn(&self, stop: CancellationToken) {
        let inner = self.0.clone();
        tokio::spawn(async move {
            let mut ticker = interval(PERIOD);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                let deadline = tokio::select! {
                    _ = stop.cancelled() => return,
                    deadline = ticker.tick() => deadline.into_std(),
                };
                let now = Instant::now();
                let late = now.saturating_duration_since(deadline).as_millis() as u64;
                inner.last_late_ms.store(late, Ordering::Release);
                let beat = now.saturating_duration_since(inner.start).as_millis() as u64;
                inner.last_beat_ms.store(beat, Ordering::Release);
            }
        });
    }

    /// The larger of the age of the last beat beyond one period, and the lateness of the last
    /// tick. It is 0 before the first tick.
    pub fn lag_ms(&self) -> u64 {
        let beat = self.0.last_beat_ms.load(Ordering::Acquire);
        if beat == NO_BEAT {
            return 0;
        }
        let late = self.0.last_late_ms.load(Ordering::Acquire);
        let now = Instant::now()
            .saturating_duration_since(self.0.start)
            .as_millis() as u64;
        let age = now
            .saturating_sub(beat)
            .saturating_sub(PERIOD.as_millis() as u64);
        age.max(late)
    }
}
