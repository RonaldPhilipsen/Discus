#![allow(clippy::unwrap_used)]

use discus_membership::sim::{Emitted, Sim};
use discus_membership::{Action, Millis, Outcome, SwimConfig};
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
// starts on time anyway, so a lagging node keeps its heartbeat rate at every healthy peer.
#[test]
fn an_unanswered_probe_does_not_delay_the_next() {
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
    assert!(scale(cfg.probe_timeout_ms) > cfg.period_ms);
    let dead = sim.node(ids[2]).swim.me().addr;
    sim.node_mut(ids[0]).emitted.clear();
    sim.kill(ids[2]);
    sim.run_ms(10_000);
    let pings = direct_pings(&sim, ids[0]);
    let mut checked = 0;
    for pair in pings.windows(2) {
        assert_eq!(pair[1].at - pair[0].at, cfg.period_ms, "{pair:?}");
        if pair[0].to == dead {
            checked += 1;
        }
    }
    assert!(checked > 0);
}

/// A cluster of `n` with every node at the maximum multiplier. The tests below drive node
/// `ids[0]` by hand from the returned time on, so that they choose when each outcome arrives.
fn lagging_cluster(seed: u64, n: usize) -> (Sim, Vec<NodeId>, Millis) {
    let cfg = SwimConfig::fast();
    let mut sim = Sim::new(seed, cfg.clone());
    let ids = sim.cluster(n);
    sim.run_ms(1_000);
    for _ in 0..100 {
        sim.node_mut(ids[0]).swim.observe_lag(200);
    }
    assert_eq!(
        sim.node(ids[0]).swim.health_multiplier(),
        cfg.health_max_multiplier
    );
    // Past the slack of the probes that the simulator left in flight.
    let start = sim.now() + 2 * cfg.period_ms;
    (sim, ids, start)
}

fn tick(sim: &mut Sim, id: NodeId, now: Millis) -> Vec<Action> {
    let node = sim.node_mut(id);
    node.swim.tick(now, &mut node.host)
}

fn pings(actions: &[Action], kind: GossipKind) -> Vec<Action> {
    actions
        .iter()
        .filter(|a| a.msg.kind == kind)
        .cloned()
        .collect()
}

fn outcome(sim: &mut Sim, id: NodeId, now: Millis, token: u64, o: Outcome) -> Vec<Action> {
    let node = sim.node_mut(id);
    node.swim.outcome(now, token, o, &mut node.host)
}

/// The reply of the target of `a`, sent by `from`.
fn answer(sim: &mut Sim, from: NodeId, now: Millis, a: &Action) -> Outcome {
    let src = sim.node(from).swim.me().addr;
    let node = sim.node_mut(a.expected.unwrap());
    let handled = node.swim.handle(now, src, a.msg.clone(), &mut node.host);
    Outcome::Reply(Box::new(handled.reply))
}

fn score(sim: &Sim, id: NodeId, now: Millis, peer: NodeId) -> f64 {
    sim.node(id).swim.score(now, peer)
}

#[test]
fn an_unanswered_probe_still_counts_a_miss() {
    let (mut sim, ids, t) = lagging_cluster(19, 3);
    let me = ids[0];
    let weight = SwimConfig::fast().missed_ping_weight;
    let period = SwimConfig::fast().period_ms;
    let first = pings(&tick(&mut sim, me, t), GossipKind::Ping);
    assert_eq!(first.len(), 1);
    let second = pings(&tick(&mut sim, me, t + period), GossipKind::Ping);
    assert_eq!(second.len(), 1);
    let (a, b) = (first[0].expected.unwrap(), second[0].expected.unwrap());
    assert_ne!(a, b);
    let now = t + period + 10;
    let reqs = pings(
        &outcome(&mut sim, me, now, first[0].token, Outcome::Unreachable),
        GossipKind::PingReq,
    );
    assert!(!reqs.is_empty());
    let (before_a, before_b) = (score(&sim, me, now, a), score(&sim, me, now, b));
    for r in &reqs {
        outcome(&mut sim, me, now, r.token, Outcome::Unreachable);
    }
    let gained = score(&sim, me, now, a) - before_a;
    assert!((gained - weight).abs() < 1e-9, "{gained}");
    assert_eq!(score(&sim, me, now, b), before_b);
}

// Each probe keeps its own state: the outcomes of an older probe end that probe only.
#[test]
fn a_late_outcome_counts_only_for_its_probe() {
    let (mut sim, ids, t) = lagging_cluster(20, 4);
    let me = ids[0];
    let weight = SwimConfig::fast().missed_ping_weight;
    let period = SwimConfig::fast().period_ms;
    let first = pings(&tick(&mut sim, me, t), GossipKind::Ping).remove(0);
    let second = pings(&tick(&mut sim, me, t + period), GossipKind::Ping).remove(0);
    let (a, b) = (first.expected.unwrap(), second.expected.unwrap());
    assert_ne!(a, b);
    let now = t + period + 10;
    let reqs_b = pings(
        &outcome(&mut sim, me, now, second.token, Outcome::Unreachable),
        GossipKind::PingReq,
    );
    let reqs_a = pings(
        &outcome(&mut sim, me, now, first.token, Outcome::Unreachable),
        GossipKind::PingReq,
    );
    assert!(!reqs_a.is_empty() && !reqs_b.is_empty());
    let (before_a, before_b) = (score(&sim, me, now, a), score(&sim, me, now, b));
    for r in &reqs_a {
        outcome(&mut sim, me, now, r.token, Outcome::Unreachable);
    }
    assert!((score(&sim, me, now, a) - before_a - weight).abs() < 1e-9);
    assert_eq!(score(&sim, me, now, b), before_b);
    for r in &reqs_b {
        outcome(&mut sim, me, now, r.token, Outcome::Unreachable);
    }
    assert!((score(&sim, me, now, b) - before_b - weight).abs() < 1e-9);
}

#[test]
fn a_target_with_a_probe_in_flight_is_skipped() {
    let (mut sim, ids, t) = lagging_cluster(21, 4);
    let me = ids[0];
    let period = SwimConfig::fast().period_ms;
    let first = pings(&tick(&mut sim, me, t), GossipKind::Ping).remove(0);
    let pending = first.expected.unwrap();
    // The first probe stays in flight through these periods. The rest of the round that the
    // simulator left, then one full round over the three targets, reaches `pending` again.
    assert!(first.timeout_ms + period > 5 * period);
    let mut probed = Vec::new();
    for i in 1..=5 {
        let now = t + i * period;
        let sent = pings(&tick(&mut sim, me, now), GossipKind::Ping);
        assert_eq!(sent.len(), 1, "period {i}");
        let target = sent[0].expected.unwrap();
        assert_ne!(target, pending, "period {i}");
        probed.push(target);
        let reply = answer(&mut sim, me, now, &sent[0]);
        outcome(&mut sim, me, now + 5, sent[0].token, reply);
    }
    assert!(probed.iter().all(|p| ids[1..].contains(p)));
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

// Every node at the maximum multiplier and one dead peer: the probes of the dead peer outlast
// the period, and they must not starve the heartbeats of the healthy peers.
#[test]
fn all_nodes_lagging_with_a_dead_peer_keep_healthy_peers_alive() {
    let cfg = SwimConfig::default();
    let bound = 9 * (cfg.suspicion_max_ms + cfg.period_ms * 2);
    for seed in 18..22 {
        let mut sim = Sim::new(seed, cfg.clone());
        let ids = sim.cluster(10);
        sim.run_ms(1_000);
        for &id in &ids {
            sim.lag(id, 200);
        }
        sim.run_ms(1_000);
        for &id in &ids {
            assert_eq!(
                sim.node(id).swim.health_multiplier(),
                cfg.health_max_multiplier
            );
        }
        let dead = ids[9];
        let live = &ids[..9];
        sim.kill(dead);
        let killed = sim.now();
        let mut bad = Vec::new();
        let mut dead_at = vec![None; live.len()];
        for _ in 0..(240_000 / 100) {
            sim.run_ms(100);
            for (i, &o) in live.iter().enumerate() {
                if dead_at[i].is_none() && sim.liveness(o, dead) == Some(Liveness::Dead) {
                    dead_at[i] = Some(sim.now() - killed);
                }
                for &p in live.iter().filter(|p| **p != o) {
                    let l = sim.liveness(o, p);
                    if l != Some(Liveness::Alive) {
                        bad.push((sim.now(), o, p, l));
                    }
                }
            }
        }
        assert!(
            bad.is_empty(),
            "seed {seed}: {} non-Alive observations, first {:?}",
            bad.len(),
            bad.first()
        );
        for (i, at) in dead_at.iter().enumerate() {
            assert!(
                at.is_some_and(|ms| ms <= bound),
                "seed {seed}: {} saw the dead peer as Dead after {at:?}",
                live[i]
            );
        }
    }
}
