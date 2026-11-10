#![allow(clippy::unwrap_used)]

use discus_membership::sim::Sim;
use discus_membership::SwimConfig;
use discus_wire::Liveness;

#[test]
fn lagging_node_keeps_healthy_peers_alive() {
    let mut sim = Sim::new(11, SwimConfig::fast());
    let ids = sim.cluster(5);
    sim.run_ms(1_000);
    sim.lag(ids[0], 200);
    sim.run_ms(500);
    assert!(sim.node(ids[0]).swim.health_multiplier() >= SwimConfig::fast().health_evidence_cutoff);
    for _ in 0..(10_000 / 50) {
        for &p in &ids[1..] {
            sim.data_path_failure(ids[0], p);
        }
        sim.run_ms(50);
    }
    for &p in &ids[1..] {
        assert_eq!(sim.liveness(ids[0], p), Some(Liveness::Alive), "{p}");
    }
    assert!(sim.node(ids[0]).swim.evidence_dropped() > 0);
}

// Two nodes, so that node 0 cannot learn the death from a healthy peer's rumor.
#[test]
fn lagging_node_still_detects_a_dead_peer() {
    let cfg = SwimConfig::fast();
    let cutoff = cfg.health_evidence_cutoff;
    let bound = 9 * (cfg.suspicion_max_ms + cfg.period_ms * 2);
    let mut sim = Sim::new(12, cfg);
    let ids = sim.cluster(2);
    sim.run_ms(1_000);
    sim.lag(ids[0], 200);
    sim.run_ms(500);
    assert!(sim.node(ids[0]).swim.health_multiplier() >= cutoff);
    sim.kill(ids[1]);
    sim.run_until(bound, |s| {
        s.liveness(ids[0], ids[1]) == Some(Liveness::Dead)
    });
    assert!(sim.node(ids[0]).swim.health_multiplier() >= cutoff);
}

#[test]
fn lagging_node_stretches_the_suspicion_timeout() {
    let cfg = SwimConfig::fast();
    let mut sim = Sim::new(14, cfg.clone());
    let ids = sim.cluster(2);
    sim.run_ms(1_000);
    sim.lag(ids[0], 200);
    sim.run_ms(500);
    sim.kill(ids[1]);
    let limit = 20 * cfg.suspicion_max_ms;
    sim.run_until(limit, |s| {
        s.liveness(ids[0], ids[1]) == Some(Liveness::Suspect)
    });
    let suspect_at = sim.now();
    sim.run_until(limit, |s| {
        s.liveness(ids[0], ids[1]) == Some(Liveness::Dead)
    });
    let m = sim.node(ids[0]).swim.health_multiplier();
    assert!(m > 2.0, "{m}");
    // Unscaled, the suspicion ends at suspicion_max_ms at the latest.
    let held = sim.now() - suspect_at;
    assert!(
        held >= (cfg.suspicion_min_ms as f64 * m).round() as u64 - 10,
        "{held}"
    );
    assert!(held > cfg.suspicion_max_ms, "{held}");
}

#[test]
fn lagging_node_probes_less_often() {
    let mut healthy = Sim::new(15, SwimConfig::fast());
    let hi = healthy.cluster(2);
    let mut lagging = Sim::new(15, SwimConfig::fast());
    let li = lagging.cluster(2);
    healthy.run_ms(1_000);
    lagging.run_ms(1_000);
    lagging.lag(li[0], 200);
    lagging.run_ms(500);
    healthy.run_ms(500);
    let (h0, l0) = (healthy.messages_sent(hi[0]), lagging.messages_sent(li[0]));
    healthy.run_ms(5_000);
    lagging.run_ms(5_000);
    let h = healthy.messages_sent(hi[0]) - h0;
    let l = lagging.messages_sent(li[0]) - l0;
    assert!(l * 3 < h, "lagging sent {l}, healthy sent {h}");
}

#[test]
fn healthy_node_still_suspects_after_one_data_path_failure() {
    let mut sim = Sim::new(13, SwimConfig::fast());
    let ids = sim.cluster(5);
    sim.run_ms(1_000);
    sim.data_path_failure(ids[0], ids[1]);
    sim.run_ms(10);
    assert_eq!(sim.liveness(ids[0], ids[1]), Some(Liveness::Suspect));
}
