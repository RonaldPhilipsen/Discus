//! The SWIM and failure detector settings (spec section 6.1). Every value is tunable.

/// Milliseconds on a monotonic clock that the caller supplies. The simulator uses virtual time.
pub type Millis = u64;

/// Settings of the SWIM core.
#[derive(Debug, Clone, PartialEq)]
pub struct SwimConfig {
    /// The protocol period: one probe per period. It does not scale with the health multiplier.
    /// A probe that is still in flight when the next one is due delays the next one.
    pub period_ms: Millis,
    /// Time to wait for a direct ack before the indirect probes start. The core scales it by the
    /// health multiplier, so the scaled value can be longer than the period. Unscaled, it must be
    /// below the period.
    pub probe_timeout_ms: Millis,
    /// Members asked to probe a target that missed a direct ping.
    pub indirect_probes: usize,
    /// A rumor is sent `retransmit_mult * ceil(log2(n + 1))` times.
    pub retransmit_mult: u32,
    /// Rumors per message.
    pub max_rumors: usize,
    /// Anti-entropy: a full state exchange with one random member at this interval.
    pub sync_interval_ms: Millis,
    /// A sync with a dead member, a seed, or a foreign lease holder at this interval.
    pub reconnect_interval_ms: Millis,
    /// Dead records are kept this long, for refutation and for partition heal.
    pub dead_retention_ms: Millis,
    /// At most this many dead records are kept.
    pub max_dead: usize,
    /// A score at or above this makes a member suspect.
    pub phi_suspect: f64,
    /// A suspect member with a score at or above this is dead once `suspicion_min_ms` passed.
    pub phi_dead: f64,
    /// Evidence added by one data-path failure (forward timeout, reset, failed handshake).
    pub data_path_weight: f64,
    /// Evidence added by one failed probe (direct and indirect). The default equals
    /// `phi_suspect`, so one failed probe makes the member suspect, as in plain SWIM.
    pub missed_ping_weight: f64,
    /// Evidence halves after this time.
    pub evidence_half_life_ms: Millis,
    /// The suspicion timeout with `suspicion_k` confirmations or more. No suspect becomes dead
    /// sooner.
    pub suspicion_min_ms: Millis,
    /// The suspicion timeout without confirmations.
    pub suspicion_max_ms: Millis,
    /// The number of other accusers that brings the timeout down to the minimum.
    pub suspicion_k: usize,
    /// A node waits a random time up to this before it creates an epoch (spec section 5.1).
    pub epoch_jitter_ms: Millis,
    /// A starting node looks for a NORMAL peer this long before it tries the lease.
    pub bootstrap_deadline_ms: Millis,
    /// Level smoothing of the local health score for a rising tick lag.
    pub health_alpha_up: f64,
    /// Level smoothing of the local health score for a falling tick lag.
    pub health_alpha_down: f64,
    /// Trend smoothing of the local health score.
    pub health_beta: f64,
    /// How many ticks ahead the trend projects.
    pub health_horizon_ticks: u32,
    /// A tick lag below this is scheduling noise.
    pub health_deadband_ms: Millis,
    /// The score above the deadband that adds one step to the health multiplier.
    pub health_unit_ms: Millis,
    /// The upper bound of the health multiplier.
    pub health_max_multiplier: f64,
    /// At or above this multiplier, data-path evidence is dropped.
    pub health_evidence_cutoff: f64,
    /// The replication factor, for the ownership-gained event.
    pub replication_factor: usize,
    /// The seed of the random source. Production uses a random seed.
    pub seed: u64,
}

impl Default for SwimConfig {
    fn default() -> SwimConfig {
        SwimConfig {
            period_ms: 1000,
            probe_timeout_ms: 500,
            indirect_probes: 3,
            retransmit_mult: 4,
            max_rumors: 16,
            sync_interval_ms: 10_000,
            reconnect_interval_ms: 5_000,
            dead_retention_ms: 3_600_000,
            max_dead: 1024,
            phi_suspect: 8.0,
            phi_dead: 16.0,
            data_path_weight: 10.0,
            missed_ping_weight: 8.0,
            evidence_half_life_ms: 10_000,
            suspicion_min_ms: 3_000,
            suspicion_max_ms: 10_000,
            suspicion_k: 3,
            epoch_jitter_ms: 50,
            bootstrap_deadline_ms: 10_000,
            health_alpha_up: 0.2,
            health_alpha_down: 0.01,
            health_beta: 0.3,
            health_horizon_ticks: 5,
            health_deadband_ms: 10,
            health_unit_ms: 10,
            health_max_multiplier: 9.0,
            health_evidence_cutoff: 1.5,
            replication_factor: discus_placement::DEFAULT_RF,
            seed: fastrand::u64(..),
        }
    }
}

impl SwimConfig {
    /// Fast timings for tests: a 100 ms period and a 300 to 1000 ms suspicion timeout.
    pub fn fast() -> SwimConfig {
        SwimConfig {
            period_ms: 100,
            probe_timeout_ms: 50,
            sync_interval_ms: 1_000,
            reconnect_interval_ms: 500,
            suspicion_min_ms: 300,
            suspicion_max_ms: 1_000,
            evidence_half_life_ms: 1_000,
            bootstrap_deadline_ms: 500,
            ..SwimConfig::default()
        }
    }

    /// Check the values that the detector needs.
    pub fn validate(&self) -> Result<(), String> {
        if self.period_ms == 0
            || self.probe_timeout_ms == 0
            || self.probe_timeout_ms >= self.period_ms
        {
            return Err("the probe timeout must be above 0 and below the gossip interval".into());
        }
        if !(self.phi_suspect > 0.0 && self.phi_dead > self.phi_suspect) {
            return Err("the phi thresholds need 0 < suspect < dead".into());
        }
        if self.suspicion_min_ms > self.suspicion_max_ms {
            return Err("the suspicion minimum must not exceed the maximum".into());
        }
        if self.data_path_weight < 0.0 || self.missed_ping_weight < 0.0 {
            return Err("the evidence weights must not be negative".into());
        }
        let unit_range = |v: f64| v > 0.0 && v <= 1.0;
        if !(unit_range(self.health_alpha_up)
            && unit_range(self.health_alpha_down)
            && unit_range(self.health_beta))
        {
            return Err("the health smoothing factors need 0 < value <= 1".into());
        }
        if self.health_unit_ms == 0 {
            return Err("the health unit must be above 0".into());
        }
        if self.health_max_multiplier.is_nan() || self.health_max_multiplier < 1.0 {
            return Err("the health maximum multiplier must be at least 1".into());
        }
        if self.health_evidence_cutoff.is_nan() || self.health_evidence_cutoff < 1.0 {
            return Err("the health evidence cutoff must be at least 1".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_bad_health_values() {
        let bad: [fn(&mut SwimConfig); 5] = [
            |c| c.health_alpha_up = 0.0,
            |c| c.health_beta = 1.5,
            |c| c.health_unit_ms = 0,
            |c| c.health_max_multiplier = 0.5,
            |c| c.health_evidence_cutoff = 0.9,
        ];
        for f in bad {
            let mut c = SwimConfig::default();
            f(&mut c);
            assert!(c.validate().is_err());
        }
        assert!(SwimConfig::default().validate().is_ok());
    }
}
