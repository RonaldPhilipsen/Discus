//! A node that was cut off long enough to mark its peers dead, and then comes back (spec section
//! 6.3).

#![allow(clippy::unwrap_used)]

use discus_membership::sim::Sim;
use discus_membership::{Event, SwimConfig};
use discus_placement::{MemberState, NodeId};
use discus_wire::Liveness;

/// Long enough for the isolated node to mark every peer dead with the fast timings.
const ISOLATION_MS: u64 = 15_000;

/// Every node holds the same epoch, and that epoch lists every node as NORMAL.
fn converged(sim: &Sim, ids: &[NodeId]) -> bool {
    let e = sim.epoch(ids[0]);
    ids.iter().all(|&id| {
        sim.epoch(id).id() == e.id()
            && sim.is_normal(id)
            && e.member(id).map(|m| m.state) == Some(MemberState::Normal)
    })
}

fn rejoined(sim: &Sim, id: NodeId) -> bool {
    sim.events(id)
        .iter()
        .any(|e| matches!(e, Event::Rejoining { .. }))
}

/// Isolate one node of `n` until it has marked every peer dead and built epochs alone, then heal.
/// True when the isolated node held the higher epoch at the heal.
fn isolated_node_rejoins(n: usize, seed: u64) -> bool {
    let mut sim = Sim::new(seed, SwimConfig::fast());
    let ids = sim.cluster(n);
    sim.run_ms(1_000);
    let lone = ids[0];
    let healthy: Vec<NodeId> = ids[1..].to_vec();
    let start = sim.now();
    sim.isolate(lone, ISOLATION_MS);
    sim.run_until(ISOLATION_MS - 1_000, |s| {
        let e = s.epoch(lone);
        e.normal().count() == 1
            && healthy
                .iter()
                .all(|&h| s.epoch(h).member(lone).is_none() && s.is_normal(h))
    });
    let higher = sim.epoch(lone).id() > sim.epoch(healthy[0]).id();
    sim.run_ms(start + ISOLATION_MS - sim.now());
    let now = sim.now();
    let deaths: Vec<usize> = healthy
        .iter()
        .map(|&h| dead_peers(&sim, h, &healthy))
        .collect();
    sim.run_until(30_000, |s| {
        for &h in &healthy {
            assert!(
                s.is_normal(h),
                "seed {seed}: {h} left NORMAL at {} for epoch {}",
                s.now(),
                s.epoch(h).id()
            );
        }
        converged(s, &ids)
    });
    sim.run_ms(2_000);
    assert!(converged(&sim, &ids), "seed {seed}: no lasting agreement");
    for (i, &h) in healthy.iter().enumerate() {
        assert!(!rejoined(&sim, h), "seed {seed}: healthy node {h} rejoined");
        assert_eq!(
            dead_peers(&sim, h, &healthy),
            deaths[i],
            "seed {seed}: {h} marked a healthy peer dead after {now}"
        );
    }
    assert!(
        rejoined(&sim, lone),
        "seed {seed}: the isolated node did not rejoin: higher {higher} {:?} healthy {:?}",
        sim.events(lone)
            .iter()
            .filter(|e| matches!(e, Event::EpochAdopted { .. } | Event::StateChanged { .. }))
            .collect::<Vec<_>>(),
        sim.events(healthy[0])
            .iter()
            .filter(|e| matches!(e, Event::EpochAdopted { .. }))
            .collect::<Vec<_>>()
    );
    higher
}

/// How often `observer` saw a member of `peers` become dead.
fn dead_peers(sim: &Sim, observer: NodeId, peers: &[NodeId]) -> usize {
    sim.events(observer)
        .iter()
        .filter(|e| {
            matches!(e, Event::MemberChanged { id, liveness: Liveness::Dead, .. } if peers.contains(id))
        })
        .count()
}

#[test]
fn an_isolated_node_of_five_rejoins_the_four() {
    let higher = (0..10)
        .filter(|seed| isolated_node_rejoins(5, 500 + seed))
        .count();
    assert!(
        higher > 0,
        "no seed gave the isolated node the higher epoch"
    );
}

#[test]
fn an_isolated_node_of_three_rejoins_the_two() {
    let higher = (0..10)
        .filter(|seed| isolated_node_rejoins(3, 600 + seed))
        .count();
    assert!(
        higher > 0,
        "no seed gave the isolated node the higher epoch"
    );
}
