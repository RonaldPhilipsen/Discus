//! The failure detector of one member: a phi-accrual detector over heartbeats plus decaying
//! evidence (spec section 6.1).
//!
//! The phi part comes from `phi-accrual-failure-detector`. A heartbeat is any message from the
//! member and any direct or indirect ack. One node hears from one given member only about every
//! `n / 2` periods in a cluster of `n`, so the expected interval and the acceptable pause scale
//! with `n`. Evidence adds weight for a failed probe and, with a larger weight, for a data-path
//! failure. It halves every `evidence_half_life_ms`. Evidence alone stays below the death
//! threshold, so a death always needs silence or the suspicion timeout too.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use phi_accrual_failure_detector::{Builder, Clock, Detector as _, FailureDetector, UnsyncState};

use crate::{Millis, SwimConfig};

/// A clock that reads the time the caller last set. It makes the detector run on the caller's
/// time, which is virtual in the simulator.
#[derive(Clone)]
struct CallerClock(Arc<AtomicU64>);

impl Clock for CallerClock {
    type Timestamp = Millis;

    fn timestamp(&self) -> Millis {
        self.0.load(Ordering::Relaxed)
    }

    fn elapsed(before: &Millis, after: &Millis) -> Duration {
        Duration::from_millis(after.saturating_sub(*before))
    }
}

/// The share of `phi_dead` that evidence can reach on its own.
const EVIDENCE_CAP: f64 = 0.75;

pub struct Detector {
    now: Arc<AtomicU64>,
    phi: FailureDetector<UnsyncState<CallerClock>>,
    evidence: Vec<(Millis, f64)>,
    half_life: f64,
    cap: f64,
}

impl Detector {
    /// A detector for a cluster of `n` members that starts monitoring with a heartbeat at `now`.
    pub fn new(cfg: &SwimConfig, now: Millis, n: usize) -> Detector {
        let clock = Arc::new(AtomicU64::new(now));
        let period = Duration::from_millis(cfg.period_ms.max(1));
        let n = u32::try_from(n).unwrap_or(u32::MAX).max(1);
        let phi = Builder::<UnsyncState<_>>::new()
            .threshold(cfg.phi_suspect.max(f64::MIN_POSITIVE))
            .max_sample_size(100)
            .min_std_deviation((period / 2).max(Duration::from_millis(1)))
            .acceptable_heartbeat_pause(period * n.max(2))
            .first_heartbeat_estimate(period * (n / 2).max(1))
            .clock(CallerClock(clock.clone()))
            .build()
            .unwrap_or_else(|_| {
                Builder::<UnsyncState<_>>::new()
                    .clock(CallerClock(clock.clone()))
                    .build()
                    .unwrap_or_else(|e| panic!("the default detector settings are valid: {e}"))
            });
        let mut d = Detector {
            now: clock,
            phi,
            evidence: Vec::new(),
            half_life: cfg.evidence_half_life_ms.max(1) as f64,
            cap: cfg.phi_dead * EVIDENCE_CAP,
        };
        d.heartbeat(now);
        d
    }

    fn set(&self, now: Millis) {
        self.now.store(now, Ordering::Relaxed);
    }

    pub fn heartbeat(&mut self, now: Millis) {
        self.set(now);
        self.phi.heartbeat();
    }

    /// Add evidence of `weight` at `now`.
    pub fn evidence(&mut self, now: Millis, weight: f64) {
        let horizon = (self.half_life * 16.0) as Millis;
        self.evidence
            .retain(|(at, _)| now.saturating_sub(*at) < horizon);
        self.evidence.push((now, weight));
    }

    /// The decayed evidence at `now`, capped below the death threshold.
    pub fn evidence_at(&self, now: Millis) -> f64 {
        let sum: f64 = self
            .evidence
            .iter()
            .map(|(at, w)| w * 0.5f64.powf(now.saturating_sub(*at) as f64 / self.half_life))
            .sum();
        sum.min(self.cap)
    }

    pub fn phi(&self, now: Millis) -> f64 {
        self.set(now);
        let phi = self.phi.phi();
        if phi.is_finite() {
            phi
        } else {
            f64::MAX
        }
    }

    /// `phi + evidence`.
    pub fn score(&self, now: Millis) -> f64 {
        self.phi(now) + self.evidence_at(now)
    }
}
