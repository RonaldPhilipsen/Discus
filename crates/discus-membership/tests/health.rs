#![allow(clippy::unwrap_used)]

use discus_membership::sim::{Emitted, Sim};
use discus_membership::SwimConfig;
use discus_placement::NodeId;
use discus_wire::{GossipKind, Liveness};

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
    assert!(sim.node(ids[0]).swim.health_multiplier() > 2.0);
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

fn direct_pings(sim: &Sim, id: NodeId) -> Vec<Emitted> {
    sim.node(id)
        .emitted
        .iter()
        .filter(|e| e.kind == GossipKind::Ping && !e.relay)
        .copied()
        .collect()
}

// Answered probes end long before the next one is due, so lag must not slow them down: a lower
// probe rate is a lower heartbeat rate at every peer.
#[test]
fn lagging_node_keeps_its_probe_rate() {
    let cfg = SwimConfig::fast();
    let mut healthy = Sim::new(15, cfg.clone());
    let hi = healthy.cluster(2);
    let mut lagging = Sim::new(15, cfg.clone());
    let li = lagging.cluster(2);
    healthy.run_ms(1_000);
    lagging.run_ms(1_000);
    lagging.lag(li[0], 200);
    lagging.run_ms(500);
    healthy.run_ms(500);
    assert_eq!(
        lagging.node(li[0]).swim.health_multiplier(),
        cfg.health_max_multiplier
    );
    healthy.node_mut(hi[0]).emitted.clear();
    lagging.node_mut(li[0]).emitted.clear();
    healthy.run_ms(5_000);
    lagging.run_ms(5_000);
    let h = direct_pings(&healthy, hi[0]).len();
    let l = direct_pings(&lagging, li[0]).len();
    assert!(h >= 45, "healthy sent {h}");
    assert!(l * 10 >= h * 9, "lagging sent {l}, healthy sent {h}");
}

// The scaled timeouts of a probe of a dead peer add up to more than the period. The next probe
// waits for it instead of replacing it, so its missed-probe evidence still lands.
#[test]
fn timed_out_probe_does_not_overlap_the_next() {
    let cfg = SwimConfig::fast();
    let mut sim = Sim::new(17, cfg.clone());
    let ids = sim.cluster(3);
    sim.run_ms(1_000);
    // The helper lags too, so that its rumor does not end the probes of the dead peer early.
    for &id in &ids {
        sim.lag(id, 200);
    }
    sim.run_ms(1_000);
    let m = sim.node(ids[0]).swim.health_multiplier();
    assert_eq!(m, cfg.health_max_multiplier);
    let scale = |ms: u64| (ms as f64 * m).round() as u64;
    let direct_ms = scale(cfg.probe_timeout_ms);
    let indirect_ms = scale(cfg.period_ms - cfg.probe_timeout_ms);
    assert!(direct_ms + indirect_ms > cfg.period_ms);
    let dead = sim.node(ids[2]).swim.me().addr;
    sim.node_mut(ids[0]).emitted.clear();
    sim.kill(ids[2]);
    sim.run_ms(10_000);
    let emitted = sim.node(ids[0]).emitted.clone();
    let pings = direct_pings(&sim, ids[0]);
    let mut checked = 0;
    for pair in pings.windows(2) {
        let (probe, next) = (pair[0], pair[1]);
        if probe.to != dead {
            continue;
        }
        let indirect = emitted
            .iter()
            .any(|e| e.kind == GossipKind::PingReq && e.at >= probe.at && e.at < next.at);
        let end = probe.at + direct_ms + if indirect { indirect_ms } else { 0 };
        assert!(
            next.at >= end,
            "a probe at {} ended at {end}, the next started at {}",
            probe.at,
            next.at
        );
        checked += 1;
    }
    assert!(checked > 0);
}

#[test]
fn all_nodes_lagging_keep_every_peer_alive() {
    let cfg = SwimConfig::default();
    let mut sim = Sim::new(18, cfg.clone());
    let ids = sim.cluster(10);
    sim.run_ms(1_000);
    for &id in &ids {
        sim.lag(id, 200);
    }
    let mut bad = Vec::new();
    let mut reached_max = false;
    for _ in 0..(120_000 / 100) {
        sim.run_ms(100);
        for &o in &ids {
            reached_max |= sim.node(o).swim.health_multiplier() == cfg.health_max_multiplier;
            for &p in ids.iter().filter(|p| **p != o) {
                let l = sim.liveness(o, p);
                if l != Some(Liveness::Alive) {
                    bad.push((sim.now(), o, p, l));
                }
            }
        }
    }
    assert!(reached_max);
    assert!(
        bad.is_empty(),
        "{} non-Alive observations, first {:?}",
        bad.len(),
        bad.first()
    );
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

#[test]
fn lagging_nodes_stretch_the_probe_timeouts() {
    let cfg = SwimConfig::fast();
    let mut sim = Sim::new(16, cfg.clone());
    let ids = sim.cluster(4);
    sim.run_ms(1_000);
    for &id in &ids {
        sim.lag(id, 200);
    }
    sim.run_ms(500);
    for &id in &ids {
        sim.node_mut(id).emitted.clear();
    }
    sim.kill(ids[3]);
    sim.run_ms(10_000);
    let (mut direct, mut indirect, mut relayed) = (0, 0, 0);
    for &id in &ids[..3] {
        let m = sim.node(id).swim.health_multiplier();
        assert!(m > 2.0, "{m}");
        let scale = |ms: u64| (ms as f64 * m).round() as u64;
        let indirect_ms = scale(cfg.period_ms - cfg.probe_timeout_ms).max(1);
        for e in &sim.node(id).emitted {
            match (e.kind, e.relay) {
                (GossipKind::Ping, false) => {
                    direct += 1;
                    assert_eq!(e.timeout_ms, scale(cfg.probe_timeout_ms));
                }
                (GossipKind::Ping, true) => {
                    relayed += 1;
                    assert_eq!(e.timeout_ms, scale(cfg.probe_timeout_ms));
                }
                (GossipKind::PingReq, _) => {
                    indirect += 1;
                    assert_eq!(e.timeout_ms, indirect_ms);
                }
                _ => {}
            }
        }
    }
    assert!(
        direct > 0 && indirect > 0 && relayed > 0,
        "{direct} {indirect} {relayed}"
    );
}
