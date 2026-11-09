#![allow(clippy::unwrap_used)]

use discus_membership::{Detector, SwimConfig};

#[test]
fn phi_grows_with_silence() {
    let cfg = SwimConfig {
        seed: 1,
        ..SwimConfig::default()
    };
    let mut d = Detector::new(&cfg, 0, 1);
    for beat in 1..=10 {
        d.heartbeat(beat * 1000);
    }
    // The next regular beat is not suspicious.
    assert!(d.score(11_000) < 1.0, "{}", d.score(11_000));
    // Seven seconds of silence is.
    assert!(d.score(17_000) > cfg.phi_suspect, "{}", d.score(17_000));
}

#[test]
fn a_member_without_history_starts_monitored() {
    let cfg = SwimConfig::default();
    let d = Detector::new(&cfg, 0, 1);
    assert!(d.score(0) < 1.0);
    assert!(d.score(60_000) > cfg.phi_dead);
}

#[test]
fn data_path_events_weigh_more_than_missed_pings() {
    let cfg = SwimConfig::default();
    assert!(cfg.data_path_weight > cfg.missed_ping_weight);
    let mut d = Detector::new(&cfg, 0, 1);
    d.evidence(0, cfg.data_path_weight);
    assert!((d.evidence_at(0) - cfg.data_path_weight).abs() < 1e-9);
    let half = d.evidence_at(cfg.evidence_half_life_ms);
    assert!((half - cfg.data_path_weight / 2.0).abs() < 1e-6, "{half}");

    let mut m = Detector::new(&cfg, 0, 1);
    m.evidence(0, cfg.missed_ping_weight);
    assert!(m.evidence_at(0) < d.evidence_at(0));
}

#[test]
fn evidence_alone_never_reaches_death() {
    let cfg = SwimConfig::default();
    let mut d = Detector::new(&cfg, 0, 1);
    for _ in 0..100 {
        d.evidence(0, cfg.data_path_weight);
    }
    assert!(d.evidence_at(0) < cfg.phi_dead);
    assert!(d.score(0) >= cfg.phi_suspect);
}
