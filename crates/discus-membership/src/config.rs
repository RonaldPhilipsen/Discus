//! The SWIM and failure detector settings (spec section 6.1). Every value is tunable.

/// Milliseconds on a monotonic clock that the caller supplies. The simulator uses virtual time.
pub type Millis = u64;

/// Settings of the SWIM core.
#[derive(Debug, Clone, PartialEq)]
pub struct SwimConfig {
    /// The protocol period: one probe per period.
    pub period_ms: Millis,
    /// Time to wait for a direct ack before the indirect probes start.
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
    /// A suspect member with a score at or above this is dead at once.
    pub phi_dead: f64,
    /// Evidence added by one data-path failure (forward timeout, reset, failed handshake).
    pub data_path_weight: f64,
    /// Evidence added by one failed probe (direct and indirect). The default equals
    /// `phi_suspect`, so one failed probe makes the member suspect, as in plain SWIM.
    pub missed_ping_weight: f64,
    /// Evidence halves after this time.
    pub evidence_half_life_ms: Millis,
    /// The suspicion timeout with `suspicion_k` confirmations or more.
    pub suspicion_min_ms: Millis,
    /// The suspicion timeout without confirmations.
    pub suspicion_max_ms: Millis,
    /// The number of other accusers that brings the timeout down to the minimum.
    pub suspicion_k: usize,
    /// A node waits a random time up to this before it creates an epoch (spec section 5.1).
    pub epoch_jitter_ms: Millis,
    /// A starting node looks for a NORMAL peer this long before it tries the lease.
    pub bootstrap_deadline_ms: Millis,
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
        Ok(())
    }
}
