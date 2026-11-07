#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use discus_membership::sim::Sim;
use discus_membership::SwimConfig;
use discus_wire::Liveness;

fn cfg() -> SwimConfig {
    SwimConfig {
        seed: 7,
        ..SwimConfig::fast()
    }
}

/// Run until `done` holds, in 10 ms steps, and return the virtual time it took. Panics after
/// `limit_ms`.
fn run_until(sim: &mut Sim, limit_ms: u64, mut done: impl FnMut(&Sim) -> bool) -> u64 {
    let start = sim.now();
    while !done(sim) {
        assert!(sim.now() - start < limit_ms, "not done after {limit_ms} ms");
        sim.run_ms(10);
    }
    sim.now() - start
}

#[test]
fn a_dead_node_is_detected() {
    let mut sim = Sim::new(1, cfg());
    let ids = sim.cluster(5);
    sim.run_ms(1_000);
    sim.kill(ids[4]);
    run_until(&mut sim, 3_000, |s| {
        ids[..4]
            .iter()
            .all(|&o| s.liveness(o, ids[4]) == Some(Liveness::Dead))
    });
}

#[test]
fn a_healthy_cluster_stays_alive() {
    let mut sim = Sim::new(2, cfg());
    let ids = sim.cluster(6);
    sim.run_ms(20_000);
    for &o in &ids {
        for &s in &ids {
            if o != s {
                assert_eq!(sim.liveness(o, s), Some(Liveness::Alive), "{o} sees {s}");
            }
        }
    }
}

#[test]
fn suspicion_is_refuted() {
    let mut sim = Sim::new(3, cfg());
    let ids = sim.cluster(4);
    sim.run_ms(1_000);
    let before = sim.incarnation(ids[2]);
    sim.isolate(ids[2], 300);
    // Some node suspects it during the isolation.
    run_until(&mut sim, 300, |s| {
        ids.iter()
            .any(|&o| s.liveness(o, ids[2]) == Some(Liveness::Suspect))
    });
    sim.run_ms(2_000);
    for &o in &ids[..2] {
        assert_eq!(sim.liveness(o, ids[2]), Some(Liveness::Alive));
    }
    assert!(sim.incarnation(ids[2]) > before);
}

#[test]
fn rumors_spread_in_log_rounds() {
    let mut sim = Sim::new(4, cfg());
    let ids = sim.cluster(16);
    sim.run_ms(1_000);
    sim.set_leader_hints(ids[0], Bytes::from_static(b"g0"));
    let want = sim.incarnation(ids[0]);
    let took = run_until(&mut sim, 10 * 5 * 100, |s| {
        ids[1..]
            .iter()
            .all(|&o| s.known_incarnation(o, ids[0]) == Some(want))
    });
    assert!(took <= 10 * 5 * 100, "{took}");
}

fn time_to_dead(evidence: bool) -> u64 {
    let mut sim = Sim::new(5, cfg());
    let ids = sim.cluster(6);
    sim.run_ms(1_000);
    sim.kill(ids[5]);
    if evidence {
        for &entry in &ids[..3] {
            for _ in 0..3 {
                sim.data_path_failure(entry, ids[5]);
            }
        }
    }
    run_until(&mut sim, 10_000, |s| {
        ids[..5]
            .iter()
            .any(|&o| s.liveness(o, ids[5]) == Some(Liveness::Dead))
    })
}

#[test]
fn data_path_evidence_speeds_up_dead() {
    let plain = time_to_dead(false);
    let fast = time_to_dead(true);
    assert!(fast < plain, "with evidence {fast} ms, without {plain} ms");
}

fn rate(n: usize) -> f64 {
    let mut sim = Sim::new(6, cfg());
    let ids = sim.cluster(n);
    sim.run_ms(2_000);
    let before: Vec<u64> = ids.iter().map(|&i| sim.messages_sent(i)).collect();
    sim.run_ms(10_000);
    let max = ids
        .iter()
        .zip(before)
        .map(|(&i, b)| sim.messages_sent(i) - b)
        .max()
        .unwrap();
    max as f64 / 10.0
}

#[test]
fn the_message_rate_is_constant() {
    let c = cfg();
    let bound = 1000.0 / c.period_ms as f64 * (1 + c.indirect_probes) as f64 + 2.0;
    for n in [4, 16] {
        let r = rate(n);
        assert!(
            r <= bound,
            "{n} nodes: {r} messages per second, bound {bound}"
        );
    }
}
