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

#[test]
fn lagging_node_still_detects_a_dead_peer() {
    let cfg = SwimConfig::fast();
    let bound = 9 * (cfg.suspicion_max_ms + cfg.period_ms * 5);
    let mut sim = Sim::new(12, cfg);
    let ids = sim.cluster(5);
    sim.run_ms(1_000);
    sim.lag(ids[0], 200);
    sim.run_ms(500);
    sim.kill(ids[4]);
    sim.run_until(bound, |s| {
        s.liveness(ids[0], ids[4]) == Some(Liveness::Dead)
    });
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
