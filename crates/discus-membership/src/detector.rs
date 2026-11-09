//! The failure detector of one member: a phi-accrual detector over heartbeats plus decaying
//! evidence (spec section 6.1).
//!
//! The phi part follows Hayashibara et al., "The phi accrual failure detector". A heartbeat is any message from the
//! member and any direct or indirect ack. One node hears from one given member only about every
//! `n / 2` periods in a cluster of `n`, so the expected interval and the acceptable pause scale
//! with `n`. Evidence adds weight for a failed probe and, with a larger weight, for a data-path
//! failure. It halves every `evidence_half_life_ms`. Evidence alone stays below the death
//! threshold, so a death always needs silence or the suspicion timeout too.

use std::collections::VecDeque;

use crate::{Millis, SwimConfig};

/// The number of inter-arrival samples that the detector keeps.
const MAX_SAMPLES: usize = 100;

/// The share of `phi_dead` that evidence can reach on its own.
const EVIDENCE_CAP: f64 = 0.75;

/// Heartbeat inter-arrival times, in milliseconds.
struct Intervals {
    window: VecDeque<f64>,
    capacity: usize,
    sum: f64,
    squares: f64,
}

impl Intervals {
    fn new(capacity: usize) -> Intervals {
        Intervals {
            window: VecDeque::with_capacity(capacity),
            capacity,
            sum: 0.0,
            squares: 0.0,
        }
    }

    fn add(&mut self, interval: f64) {
        if self.window.len() == self.capacity {
            if let Some(oldest) = self.window.pop_front() {
                self.sum -= oldest;
                self.squares -= oldest * oldest;
            }
        }
        self.window.push_back(interval);
        self.sum += interval;
        self.squares += interval * interval;
    }

    fn mean(&self) -> f64 {
        self.sum / self.window.len() as f64
    }

    fn std_deviation(&self) -> f64 {
        let mean = self.mean();
        (self.squares / self.window.len() as f64 - mean * mean).sqrt()
    }
}

pub struct Detector {
    threshold: f64,
    acceptable_pause: f64,
    min_std_deviation: f64,
    intervals: Intervals,
    last_heartbeat: Millis,
    evidence: Vec<(Millis, f64)>,
    half_life: f64,
    cap: f64,
}

impl Detector {
    /// A detector for a cluster of `n` members that starts monitoring with a heartbeat at `now`.
    pub fn new(cfg: &SwimConfig, now: Millis, n: usize) -> Detector {
        let period = cfg.period_ms.max(1);
        let n = u64::try_from(n).unwrap_or(u64::MAX).max(1);
        // The unknown environment at the start gets a wide spread around the first estimate.
        let first_estimate = period.saturating_mul((n / 2).max(1)) as f64;
        let mut intervals = Intervals::new(MAX_SAMPLES);
        intervals.add(first_estimate * 0.75);
        intervals.add(first_estimate * 1.25);
        Detector {
            threshold: cfg.phi_suspect.max(f64::MIN_POSITIVE),
            acceptable_pause: period.saturating_mul(n.max(2)) as f64,
            min_std_deviation: (period / 2).max(1) as f64,
            intervals,
            last_heartbeat: now,
            evidence: Vec::new(),
            half_life: cfg.evidence_half_life_ms.max(1) as f64,
            cap: cfg.phi_dead * EVIDENCE_CAP,
        }
    }

    pub fn heartbeat(&mut self, now: Millis) {
        // An interval that was already suspicious is an outage, and must not widen the normal
        // distribution.
        if self.raw_phi(now) < self.threshold {
            let elapsed = now.saturating_sub(self.last_heartbeat);
            self.intervals.add(elapsed as f64);
        }
        self.last_heartbeat = now;
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
        let phi = self.raw_phi(now);
        if phi.is_finite() {
            phi
        } else {
            f64::MAX
        }
    }

    /// Infinite when the silence is so long that the tail probability underflows.
    fn raw_phi(&self, now: Millis) -> f64 {
        let elapsed = now.saturating_sub(self.last_heartbeat) as f64;
        let mean = self.intervals.mean() + self.acceptable_pause;
        // `f64::max` also replaces the NaN that rounding can give for a variance just below zero.
        let std_deviation = self.intervals.std_deviation().max(self.min_std_deviation);
        let y = (elapsed - mean) / std_deviation;
        // The logistic approximation of the normal CDF that Akka's detector uses.
        let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
        if elapsed > mean {
            -(e / (1.0 + e)).log10()
        } else {
            -(1.0 - 1.0 / (1.0 + e)).log10()
        }
    }

    /// `phi + evidence`.
    pub fn score(&self, now: Millis) -> f64 {
        self.phi(now) + self.evidence_at(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SwimConfig {
        SwimConfig {
            period_ms: 1000,
            phi_suspect: 8.0,
            ..SwimConfig::default()
        }
    }

    fn close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "{a} != {b}");
    }

    #[test]
    fn the_history_starts_with_two_samples_around_the_first_estimate() {
        let d = Detector::new(&cfg(), 0, 8);
        close(d.intervals.mean(), 4000.0);
        close(d.intervals.std_deviation(), 1000.0);
        assert_eq!(d.intervals.window.len(), 2);
    }

    #[test]
    fn the_pause_and_the_cluster_size_scale_the_expected_interval() {
        let d = Detector::new(&cfg(), 0, 8);
        close(d.acceptable_pause, 8000.0);
        close(d.min_std_deviation, 500.0);
        // At exactly the expected time y is 0, so the tail probability is one half.
        close(d.phi(12_000), -(0.5f64).log10());
        assert!(d.phi(11_000) < d.phi(12_000));
        assert!(d.phi(12_000) < d.phi(16_000));
    }

    #[test]
    fn the_standard_deviation_never_goes_below_the_minimum() {
        let mut d = Detector::new(&cfg(), 0, 1);
        let mut now = 0;
        for _ in 0..50 {
            now += 1000;
            d.heartbeat(now);
        }
        assert!(d.intervals.std_deviation() < d.min_std_deviation);
        close(d.intervals.mean(), 1000.0);
        // Mean 1000 plus pause 2000 plus one minimum deviation of 500, so y is exactly 1.
        let e = (-(1.5976 + 0.070566f64)).exp();
        close(d.phi(now + 3500), -(e / (1.0 + e)).log10());
    }

    #[test]
    fn the_mean_and_the_deviation_cover_only_the_window() {
        let mut h = Intervals::new(3);
        for sample in [1.0, 2.0, 3.0, 4.0, 5.0] {
            h.add(sample);
        }
        assert_eq!(Vec::from(h.window.clone()), vec![3.0, 4.0, 5.0]);
        close(h.sum, 12.0);
        close(h.squares, 50.0);
        close(h.mean(), 4.0);
        close(h.std_deviation(), (2.0f64 / 3.0).sqrt());
    }

    #[test]
    fn the_expected_interval_does_not_depend_on_the_uptime() {
        let mut d = Detector::new(&cfg(), 0, 2);
        let mut now = 0;
        for _ in 0..MAX_SAMPLES {
            now += 1000;
            d.heartbeat(now);
        }
        let settled = d.phi(now + 4000);
        for _ in 0..10 * MAX_SAMPLES {
            now += 1000;
            d.heartbeat(now);
        }
        close(d.intervals.mean(), 1000.0);
        close(d.phi(now + 4000), settled);
    }

    #[test]
    fn a_heartbeat_after_a_suspicious_silence_is_not_a_sample() {
        let mut d = Detector::new(&cfg(), 0, 1);
        d.heartbeat(1000);
        let before = d.intervals.window.len();
        let silent = 1000 + 3_600_000;
        assert!(d.phi(silent) >= d.threshold);
        d.heartbeat(silent);
        assert_eq!(d.intervals.window.len(), before);
        assert_eq!(d.last_heartbeat, silent);
    }

    #[test]
    fn an_endless_silence_gives_the_largest_finite_phi() {
        let d = Detector::new(&cfg(), 0, 1);
        assert!(d.raw_phi(Millis::MAX).is_infinite());
        assert_eq!(d.phi(Millis::MAX), f64::MAX);
    }

    #[test]
    fn a_clock_that_goes_back_counts_as_no_silence() {
        let d = Detector::new(&cfg(), 5000, 1);
        assert_eq!(d.phi(1000), d.phi(5000));
    }
}
