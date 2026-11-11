use crate::config::SwimConfig;

/// A Holt forecast of the tick lag of this node, used to tell a slow node from a dead peer.
///
/// The level follows the lag fast when it rises and slowly when it falls. The trend never goes
/// below zero, so a falling lag keeps the score at or above the level and recovery is slow.
#[derive(Debug, Clone)]
pub struct LocalHealth {
    alpha_up: f64,
    alpha_down: f64,
    beta: f64,
    horizon: f64,
    deadband_ms: f64,
    unit_ms: f64,
    max_multiplier: f64,
    level: f64,
    trend: f64,
}

impl LocalHealth {
    pub fn new(cfg: &SwimConfig) -> LocalHealth {
        LocalHealth {
            alpha_up: cfg.health_alpha_up,
            alpha_down: cfg.health_alpha_down,
            beta: cfg.health_beta,
            horizon: f64::from(cfg.health_horizon_ticks),
            deadband_ms: cfg.health_deadband_ms as f64,
            unit_ms: cfg.health_unit_ms as f64,
            max_multiplier: cfg.health_max_multiplier,
            level: 0.0,
            trend: 0.0,
        }
    }

    /// Feed one tick lag sample.
    pub fn observe(&mut self, lag_ms: u64) {
        let x = lag_ms as f64;
        let a = if x >= self.level {
            self.alpha_up
        } else {
            self.alpha_down
        };
        let level = (a * x + (1.0 - a) * (self.level + self.trend)).max(0.0);
        self.trend = (self.beta * (level - self.level) + (1.0 - self.beta) * self.trend).max(0.0);
        self.level = level;
    }

    /// The forecast lag: the level plus the trend projected over the horizon.
    pub fn score_ms(&self) -> f64 {
        self.level + self.horizon * self.trend
    }

    /// The factor that stretches this node's timeouts. It is 1.0 while the score is in the deadband.
    pub fn multiplier(&self) -> f64 {
        let excess = (self.score_ms() - self.deadband_ms).max(0.0);
        (1.0 + excess / self.unit_ms).clamp(1.0, self.max_multiplier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(h: &mut LocalHealth, lag: u64, n: usize) {
        for _ in 0..n {
            h.observe(lag);
        }
    }

    #[test]
    fn step_reaches_cutoff_within_five_samples() {
        let cfg = SwimConfig::default();
        let mut h = LocalHealth::new(&cfg);
        feed(&mut h, 200, 5);
        assert!(h.multiplier() >= cfg.health_evidence_cutoff);
    }

    #[test]
    fn recovery_is_slow_but_bounded() {
        let cfg = SwimConfig::default();
        let mut h = LocalHealth::new(&cfg);
        feed(&mut h, 200, 50);
        feed(&mut h, 0, 50);
        assert!(h.multiplier() > 1.0);
        feed(&mut h, 0, 450);
        assert_eq!(h.multiplier(), 1.0);
    }

    #[test]
    fn jitter_below_deadband_keeps_multiplier_at_one() {
        let mut h = LocalHealth::new(&SwimConfig::default());
        for i in 0..1000 {
            h.observe(i % 6);
        }
        assert_eq!(h.multiplier(), 1.0);
    }

    #[test]
    fn falling_lag_never_scores_below_level() {
        // The score is level + horizon * trend with trend >= 0, so it can only sit at or above
        // the plain asymmetric EWMA of the same samples.
        let cfg = SwimConfig::default();
        let mut h = LocalHealth::new(&cfg);
        let mut ewma = 0.0f64;
        for x in (0..=300)
            .rev()
            .step_by(3)
            .chain(std::iter::repeat_n(0, 200))
        {
            h.observe(x);
            let a = if x as f64 >= ewma {
                cfg.health_alpha_up
            } else {
                cfg.health_alpha_down
            };
            ewma = a * x as f64 + (1.0 - a) * ewma;
            assert!(h.score_ms() + 1e-9 >= ewma);
        }
    }

    #[test]
    fn multiplier_is_capped() {
        let cfg = SwimConfig::default();
        let mut h = LocalHealth::new(&cfg);
        feed(&mut h, 100_000, 100);
        assert_eq!(h.multiplier(), cfg.health_max_multiplier);
    }
}
