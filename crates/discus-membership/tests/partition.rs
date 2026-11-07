#![allow(clippy::unwrap_used)]

use discus_membership::merge::winner;
use discus_membership::sim::Sim;
use discus_membership::{Event, SwimConfig};
use discus_placement::{ClusterId, MemberState, NodeId};
use proptest::prelude::*;

fn cfg(seed: u64) -> SwimConfig {
    SwimConfig {
        seed,
        ..SwimConfig::fast()
    }
}

/// Every live node holds the same epoch, lists every live node as NORMAL, and has no other
/// member.
fn converged(sim: &Sim) -> bool {
    let live = sim.alive_ids();
    let e = sim.epoch(live[0]);
    let mut members: Vec<(NodeId, MemberState)> =
        e.members().iter().map(|m| (m.id, m.state)).collect();
    members.sort_by_key(|m| m.0);
    let mut want: Vec<(NodeId, MemberState)> =
        live.iter().map(|&id| (id, MemberState::Normal)).collect();
    want.sort_by_key(|m| m.0);
    live.iter()
        .all(|&id| sim.epoch(id).id() == e.id() && sim.is_normal(id))
        && members == want
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn epochs_converge_after_a_partition(
        seed in 0u64..1_000_000,
        n in 3usize..=6,
        split in 1usize..=5,
        at in 1_000u64..5_000,
        heal_after in 5_000u64..20_000,
    ) {
        let mut sim = Sim::new(seed, cfg(seed));
        let ids = sim.cluster(n);
        let split = split.min(n - 1);
        sim.run_ms(at);
        sim.partition(&[&ids[..split], &ids[split..]]);
        sim.run_ms(heal_after);
        sim.heal();
        let mut quiet = 0;
        let mut waited = 0;
        while quiet < 30_000 {
            sim.run_ms(100);
            waited += 100;
            quiet = if converged(&sim) { quiet + 100 } else { 0 };
            prop_assert!(waited < 120_000, "no convergence after 120 s");
        }
    }
}

#[test]
fn the_lease_cluster_wins_a_merge() {
    let (a, b) = (ClusterId::from_u128(1), ClusterId::from_u128(2));
    assert_eq!(winner((a, 1), (b, 5), Some(a)), a);
    assert_eq!(winner((b, 5), (a, 1), Some(a)), a);
}

#[test]
fn the_larger_cluster_wins_a_merge() {
    let (a, b) = (ClusterId::from_u128(9), ClusterId::from_u128(2));
    assert_eq!(winner((a, 2), (b, 3), None), b);
    assert_eq!(winner((a, 2), (b, 3), Some(ClusterId::from_u128(77))), b);
}

#[test]
fn the_higher_cluster_id_breaks_a_tie() {
    let (a, b) = (ClusterId::from_u128(9), ClusterId::from_u128(2));
    assert_eq!(winner((a, 2), (b, 2), None), a);
    assert_eq!(winner((b, 2), (a, 2), None), a);
}

#[test]
fn two_clusters_merge_into_the_larger() {
    merge(false);
}

#[test]
fn the_none_lease_does_not_decide_a_merge() {
    merge(true);
}

fn merge(none_lease: bool) {
    let mut sim = Sim::new(8, cfg(8));
    if none_lease {
        sim.with_none_lease();
    }
    let big = sim.cluster(3);
    let small = sim.cluster(2);
    sim.run_ms(1_000);
    let winner = sim.cluster_id(big[0]);
    let addr = sim.node(big[0]).swim.me().addr;
    sim.node_mut(small[0]).swim.add_seed(addr);
    let mut done = false;
    for _ in 0..300 {
        sim.run_ms(100);
        if converged(&sim) {
            done = true;
            break;
        }
    }
    assert!(done, "the clusters did not merge");
    for &id in big.iter().chain(&small) {
        assert_eq!(sim.cluster_id(id), winner);
    }
    for &id in &small {
        assert!(sim
            .events(id)
            .iter()
            .any(|e| matches!(e, Event::ClusterLost { .. })));
    }
}
