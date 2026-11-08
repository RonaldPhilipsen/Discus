#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;

use discus_membership::sim::Sim;
use discus_membership::{gained_owners, Event, SwimConfig};
use discus_placement::{ClusterId, Epoch, EpochId, Member, MemberState, NodeId, Weight};

fn cfg() -> SwimConfig {
    SwimConfig {
        seed: 11,
        ..SwimConfig::fast()
    }
}

/// Every live node holds the same epoch, and its NORMAL members are exactly `want`.
fn agreed(sim: &Sim, want: &[NodeId]) -> bool {
    let live = sim.alive_ids();
    let first = sim.epoch(live[0]);
    let mut normal: Vec<NodeId> = first
        .members()
        .iter()
        .filter(|m| m.state == MemberState::Normal)
        .map(|m| m.id)
        .collect();
    normal.sort();
    let mut want = want.to_vec();
    want.sort();
    live.iter().all(|&id| sim.epoch(id).id() == first.id()) && normal == want
}

#[test]
fn a_death_creates_one_new_epoch() {
    let mut sim = Sim::new(1, cfg());
    let ids = sim.cluster(4);
    sim.run_ms(500);
    let before = sim.epoch(ids[0]).id().generation;
    sim.kill(ids[3]);
    sim.run_until(3_000, |s| agreed(s, &ids[..3]));
    let after = sim.epoch(ids[0]).id().generation;
    assert!(after > before && after <= before + 2, "{before} -> {after}");
}

#[test]
fn a_joiner_becomes_normal_in_an_epoch() {
    let mut sim = Sim::new(2, cfg());
    let ids = sim.cluster(4);
    // Node 0 runs 800 ms ahead, so the cluster maximum is above the joiner's wall clock.
    sim.node_mut(ids[0]).host.clock_offset = 800;
    sim.run_ms(500);
    let j = sim.add_node();
    sim.start(j, &[ids[1]]);
    sim.run_until(3_000, |s| s.node(j).swim.me().state == MemberState::Normal);
    let floor = sim.node(j).host.hlc;
    assert!(floor >= sim.now() + 600, "floor {floor} at {}", sim.now());
    let mut all = ids.clone();
    all.push(j);
    sim.run_until(5_000, |s| agreed(s, &all));
    assert!(sim.is_normal(j));
}

#[test]
fn racing_creators_converge() {
    for seed in 0..10 {
        let mut sim = Sim::new(100 + seed, cfg());
        let ids = sim.cluster(5);
        sim.run_ms(500);
        let before = sim.epoch(ids[0]).id().generation;
        sim.kill(ids[4]);
        // Two entry nodes see the death at the same time.
        for _ in 0..3 {
            sim.data_path_failure(ids[0], ids[4]);
            sim.data_path_failure(ids[1], ids[4]);
        }
        sim.run_until(3_000, |s| agreed(s, &ids[..4]));
        sim.run_ms(1_000);
        assert!(agreed(&sim, &ids[..4]));
        let after = sim.epoch(ids[0]).id().generation;
        assert!(after <= before + 3, "seed {seed}: {before} -> {after}");
    }
}

#[test]
fn an_unseen_change_is_never_undone() {
    let mut sim = Sim::new(3, cfg());
    let ids = sim.cluster(5);
    sim.run_ms(500);
    let x = ids[4];
    sim.kill(x);
    // Only node 0 has strong evidence, so it removes X before the others see the death.
    for _ in 0..4 {
        sim.data_path_failure(ids[0], x);
    }
    let mut removed_at: Option<u64> = None;
    for _ in 0..300 {
        sim.run_ms(10);
        for &id in &ids[..4] {
            let e = sim.epoch(id);
            let has_x = e.member(x).is_some();
            match removed_at {
                None if !has_x => removed_at = Some(e.id().generation),
                Some(g) if e.id().generation >= g => {
                    assert!(!has_x, "epoch {} on {id} adds X back", e.id())
                }
                _ => {}
            }
        }
    }
    assert!(removed_at.is_some());
}

/// An epoch from `ids[0]`'s history with the next generation and `members`.
fn forced(sim: &Sim, from: NodeId, members: Vec<Member>) -> Epoch {
    let e = sim.epoch(from);
    Epoch::new(
        EpochId {
            generation: e.id().generation + 1,
            origin: from,
        },
        e.cluster_id(),
        members,
    )
    .unwrap()
}

#[test]
fn a_dropped_member_rejoins_with_a_new_incarnation() {
    let mut sim = Sim::new(4, cfg());
    let ids = sim.cluster(4);
    sim.run_ms(500);
    let c = ids[2];
    let before = sim.incarnation(c);
    let without: Vec<Member> = sim
        .epoch(ids[0])
        .members()
        .iter()
        .filter(|m| m.id != c)
        .cloned()
        .collect();
    let e = forced(&sim, ids[0], without);
    for &id in &ids {
        sim.adopt(id, e.clone());
    }
    sim.run_until(5_000, |s| agreed(s, &ids));
    assert!(sim
        .events(c)
        .iter()
        .any(|e| matches!(e, Event::Rejoining { .. })));
    assert!(sim.incarnation(c) > before);
}

#[test]
fn a_joiner_does_not_rejoin_before_its_epoch() {
    let mut sim = Sim::new(5, cfg());
    let ids = sim.cluster(3);
    sim.run_ms(500);
    let j = sim.add_node();
    sim.start(j, &[ids[0]]);
    sim.run_until(3_000, |s| s.node(j).swim.me().state == MemberState::Normal);
    // An epoch that does not list the joiner yet arrives right after its join.
    let members = sim.epoch(j).members().to_vec();
    if members.iter().all(|m| m.id != j) {
        let e = forced(&sim, ids[0], members);
        sim.adopt(j, e);
    }
    let mut all = ids.clone();
    all.push(j);
    sim.run_until(5_000, |s| agreed(s, &all));
    assert!(!sim
        .events(j)
        .iter()
        .any(|e| matches!(e, Event::Rejoining { .. })));
}

fn member(n: u128) -> Member {
    Member {
        id: NodeId::from_u128(n),
        addr: SocketAddr::from(([127, 0, 0, 1], 7000 + n as u16)),
        state: MemberState::Normal,
        weight: Weight::default(),
    }
}

fn epoch(generation: u64, ids: &[u128]) -> Epoch {
    Epoch::new(
        EpochId {
            generation,
            origin: NodeId::from_u128(1),
        },
        ClusterId::from_u128(5),
        ids.iter().map(|&n| member(n)).collect(),
    )
    .unwrap()
}

#[test]
fn ownership_gain_lists_the_new_owners() {
    let id = NodeId::from_u128;
    assert_eq!(
        gained_owners(&epoch(1, &[1, 2, 3]), &epoch(2, &[1, 2, 3, 4]), 3),
        vec![id(4)]
    );
    assert!(gained_owners(&epoch(1, &[1, 2, 3]), &epoch(2, &[1, 2]), 3).is_empty());
    assert_eq!(
        gained_owners(&epoch(1, &[1, 2, 3, 4]), &epoch(2, &[1, 2, 3]), 3),
        vec![id(1), id(2), id(3)]
    );
}

#[test]
fn a_drain_during_a_join_stays_leaving() {
    let mut sim = Sim::new(6, cfg());
    let ids = sim.cluster(3);
    sim.run_ms(500);
    let j = sim.add_node();
    sim.start(j, &[ids[0]]);
    // The join request leaves at the next tick. The drain comes before its reply.
    sim.run_ms(10);
    let now = sim.now();
    sim.node_mut(j).swim.leave(now);
    for _ in 0..200 {
        sim.run_ms(10);
        for &id in &ids {
            assert!(sim.epoch(id).member(j).is_none(), "{j} is in an epoch");
        }
    }
    assert_eq!(sim.node(j).swim.phase(), discus_membership::Phase::Leaving);
    assert!(!sim.is_normal(j));
}
