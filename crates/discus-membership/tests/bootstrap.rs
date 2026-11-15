#![allow(clippy::unwrap_used)]

use std::net::{IpAddr, Ipv4Addr};

use discus_membership::merge::JoinLimiter;
use discus_membership::sim::Sim;
use discus_membership::{MemoryLease, Phase, SwimConfig};
use discus_placement::{MemberState, NodeId};

const LEASE_MS: u64 = 2_000;

fn all_normal_in_one_cluster(sim: &Sim, ids: &[NodeId]) -> bool {
    let cluster = sim.cluster_id(ids[0]);
    let epoch = sim.epoch(ids[0]);
    ids.iter().all(|&id| {
        sim.is_normal(id)
            && sim.cluster_id(id) == cluster
            && sim.epoch(id).id() == epoch.id()
            && epoch.member(id).map(|m| m.state) == Some(MemberState::Normal)
    })
}

/// Start `n` new nodes at once, each seeded with all the others.
fn start_all(sim: &mut Sim, n: usize) -> Vec<NodeId> {
    let ids: Vec<NodeId> = (0..n).map(|_| sim.add_node()).collect();
    for &id in &ids {
        let seeds: Vec<NodeId> = ids.iter().copied().filter(|&s| s != id).collect();
        sim.start(id, &seeds);
    }
    ids
}

#[test]
fn exactly_one_node_forms_a_cluster() {
    for seed in 0..20 {
        let mut sim = Sim::new(seed, SwimConfig::fast());
        sim.with_lease(LEASE_MS);
        let ids = start_all(&mut sim, 5);
        sim.run_until(10_000, |s| all_normal_in_one_cluster(s, &ids));
        assert_eq!(sim.formed(), 1, "seed {seed}");
        let holder = sim.lease().holder().unwrap();
        assert_eq!(holder.cluster_id, sim.cluster_id(ids[0]));
    }
}

#[test]
fn lease_takeover() {
    let mut sim = Sim::new(1, SwimConfig::fast());
    sim.with_lease(LEASE_MS);
    let ids = start_all(&mut sim, 3);
    sim.run_until(10_000, |s| all_normal_in_one_cluster(s, &ids));
    let cluster = sim.cluster_id(ids[0]);
    let holder = sim.lease().holder().unwrap();
    let dead = *ids
        .iter()
        .find(|id| id.to_string() == holder.identity)
        .unwrap();
    sim.kill(dead);
    let rest: Vec<NodeId> = ids.iter().copied().filter(|&i| i != dead).collect();
    sim.run_until(LEASE_MS * 4, |s| {
        s.lease()
            .holder()
            .is_some_and(|h| h.identity != holder.identity && h.cluster_id == cluster)
    });
    assert!(rest.iter().all(|&id| sim.is_normal(id)));
}

#[test]
fn an_api_server_outage_keeps_the_cluster_serving() {
    let mut sim = Sim::new(2, SwimConfig::fast());
    sim.with_lease(LEASE_MS);
    let ids = start_all(&mut sim, 3);
    sim.run_until(10_000, |s| all_normal_in_one_cluster(s, &ids));
    sim.lease().set_available(false);
    let lonely = sim.add_node();
    sim.start(lonely, &[]);
    for _ in 0..60 {
        sim.run_ms(1_000);
        assert!(ids.iter().all(|&id| sim.is_normal(id)));
        assert!(!sim.is_normal(lonely));
    }
    sim.lease().set_available(true);
}

#[test]
fn a_full_cold_restart_forms_a_new_cluster() {
    let mut sim = Sim::new(3, SwimConfig::fast());
    sim.with_lease(LEASE_MS);
    let old = start_all(&mut sim, 3);
    sim.run_until(10_000, |s| all_normal_in_one_cluster(s, &old));
    let old_cluster = sim.cluster_id(old[0]);
    for &id in &old {
        sim.kill(id);
    }
    let fresh = start_all(&mut sim, 3);
    // The lease of the dead cluster still runs, so nobody forms at once.
    sim.run_ms(LEASE_MS / 2);
    assert!(fresh.iter().all(|&id| !sim.is_normal(id)));
    sim.run_until(10_000, |s| all_normal_in_one_cluster(s, &fresh));
    assert_ne!(sim.cluster_id(fresh[0]), old_cluster);
    assert_eq!(sim.formed(), 2);
}

#[test]
fn a_silent_normal_peer_blocks_formation() {
    let mut sim = Sim::new(4, SwimConfig::fast());
    sim.with_lease(LEASE_MS);
    let ids = start_all(&mut sim, 1);
    sim.run_until(5_000, |s| s.is_normal(ids[0]));
    sim.silence(ids[0]);
    let joiner = sim.add_node();
    sim.start(joiner, &[ids[0]]);
    sim.run_ms(5_000);
    assert!(!sim.is_normal(joiner));
    assert_eq!(sim.lease_requests(joiner), 0);
    assert_eq!(sim.formed(), 1);
}

#[test]
fn the_bootstrap_node_keeps_the_wall_clock() {
    let mut sim = Sim::new(5, SwimConfig::fast());
    sim.with_lease(LEASE_MS);
    let ids = start_all(&mut sim, 1);
    sim.run_until(5_000, |s| s.is_normal(ids[0]));
    // No floor raised the clock: it is still the wall clock.
    assert_eq!(sim.node(ids[0]).host.hlc, 0);
}

#[test]
fn the_none_provider_forms_after_the_deadline() {
    let mut sim = Sim::new(6, SwimConfig::fast());
    sim.with_none_lease();
    let ids = start_all(&mut sim, 1);
    let took = sim.run_until(5_000, |s| s.is_normal(ids[0]));
    assert!(took >= SwimConfig::fast().bootstrap_deadline_ms, "{took}");
}

#[test]
fn joins_from_one_address_back_off() {
    let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9));
    let other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10));
    let mut limiter = JoinLimiter::default();
    assert_eq!(limiter.admit(ip, 0), Ok(()));
    let wait = limiter.admit(ip, 100).unwrap_err();
    assert!((800..=1000).contains(&wait), "{wait}");
    assert_eq!(limiter.admit(other, 100), Ok(()));
    assert_eq!(limiter.admit(ip, 1_000), Ok(()));
    let wait = limiter.admit(ip, 1_100).unwrap_err();
    assert!((1_800..=2_000).contains(&wait), "{wait}");
    // Five quiet minutes reset the backoff.
    assert_eq!(limiter.admit(ip, 400_000), Ok(()));
    assert!(limiter.admit(ip, 400_100).unwrap_err() <= 1_000);
}

#[test]
fn memory_lease_semantics() {
    use discus_membership::{LeaseHolder, LeaseProvider, LeaseState};
    use discus_placement::ClusterId;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    let now = Arc::new(AtomicU64::new(0));
    let clock = now.clone();
    let lease = MemoryLease::new(Arc::new(move || clock.load(Ordering::Relaxed)), 1_000);
    let a = LeaseHolder {
        identity: "a".into(),
        address: "10.0.0.1:7379".parse().unwrap(),
        cluster_id: ClusterId::from_u128(1),
    };
    let b = LeaseHolder {
        identity: "b".into(),
        address: "10.0.0.2:7379".parse().unwrap(),
        cluster_id: ClusterId::from_u128(2),
    };
    let mut b_same = b.clone();
    b_same.cluster_id = a.cluster_id;
    let call = |h: &LeaseHolder| lease.acquire_or_renew(h).now_or_never().unwrap();
    assert_eq!(call(&a), Ok(LeaseState::Held(a.clone())));
    assert_eq!(call(&b), Ok(LeaseState::HeldByOther(a.clone())));
    assert_eq!(call(&b_same), Ok(LeaseState::Held(a.clone())));
    now.store(999, Ordering::Relaxed);
    assert_eq!(call(&b), Ok(LeaseState::HeldByOther(a.clone())));
    now.store(1_000, Ordering::Relaxed);
    assert_eq!(call(&b), Ok(LeaseState::Held(b.clone())));
    lease.set_available(false);
    assert!(call(&a).is_err());
}

/// A contender judges expiry on its own clock from its first read of the record, as client-go
/// does. It never trusts the renewal time of the holder.
#[test]
fn memory_lease_waits_a_full_duration_from_the_first_read() {
    use discus_membership::{LeaseHolder, LeaseProvider, LeaseState};
    use discus_placement::ClusterId;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    let now = Arc::new(AtomicU64::new(0));
    let clock = now.clone();
    let lease = MemoryLease::new(Arc::new(move || clock.load(Ordering::Relaxed)), 1_000);
    let holder = |id: &str, c: u128| LeaseHolder {
        identity: id.into(),
        address: "10.0.0.1:7379".parse().unwrap(),
        cluster_id: ClusterId::from_u128(c),
    };
    let (a, b, c) = (holder("a", 1), holder("b", 2), holder("c", 3));
    let call = |h: &LeaseHolder| lease.acquire_or_renew(h).now_or_never().unwrap();
    assert_eq!(call(&a), Ok(LeaseState::Held(a.clone())));
    now.store(900, Ordering::Relaxed);
    assert_eq!(call(&b), Ok(LeaseState::HeldByOther(a.clone())));
    now.store(1_500, Ordering::Relaxed);
    assert_eq!(call(&b), Ok(LeaseState::HeldByOther(a.clone())));
    assert_eq!(call(&c), Ok(LeaseState::HeldByOther(a.clone())));
    now.store(1_900, Ordering::Relaxed);
    assert_eq!(call(&b), Ok(LeaseState::Held(b.clone())));
    // c saw the record of a, not the one of b: the new record restarts its wait.
    now.store(2_500, Ordering::Relaxed);
    assert_eq!(call(&c), Ok(LeaseState::HeldByOther(b.clone())));
    now.store(3_400, Ordering::Relaxed);
    assert_eq!(call(&c), Ok(LeaseState::HeldByOther(b.clone())));
    now.store(3_500, Ordering::Relaxed);
    assert_eq!(call(&c), Ok(LeaseState::Held(c.clone())));
}

#[test]
fn a_discovery_refresh_keeps_learned_seeds() {
    use discus_membership::Swim;
    use discus_wire::{Capacity, MemberInfo};
    let me = MemberInfo {
        id: NodeId::from_u128(1),
        addr: "10.0.0.1:7379".parse().unwrap(),
        rpc_addr: "10.0.0.1:7379".parse().unwrap(),
        client_addr: None,
        client_host: None,
        state: MemberState::Joining,
        incarnation: 0,
        weight: Default::default(),
        epoch: discus_membership::provisional_epoch(NodeId::from_u128(1)).id(),
        capabilities: 0,
        capacity: Capacity::default(),
        leader_hints: Default::default(),
        databases: 1,
    };
    let mut swim = Swim::new(SwimConfig::fast(), me);
    let holder = "10.0.0.9:7379".parse().unwrap();
    swim.add_seed(holder);
    swim.set_seeds(vec!["10.0.0.2:7379".parse().unwrap()]);
    assert!(swim.seeds().contains(&holder));
    assert_eq!(swim.seeds().len(), 2);
}

/// Drop `dropped` from the epoch of every node of `ids`, so they rejoin with the cluster ID.
fn drop_from_epoch(sim: &mut Sim, ids: &[NodeId], dropped: &[NodeId]) {
    let e = sim.epoch(ids[0]);
    let members: Vec<_> = e
        .members()
        .iter()
        .filter(|m| !dropped.contains(&m.id))
        .cloned()
        .collect();
    let next = discus_placement::Epoch::new(
        discus_placement::EpochId {
            generation: e.id().generation + 1,
            origin: ids[0],
        },
        e.cluster_id(),
        members,
    )
    .unwrap();
    for &id in ids {
        sim.adopt(id, next.clone());
    }
}

#[test]
fn rejoiners_of_a_dead_cluster_form_a_new_one() {
    let mut sim = Sim::new(7, SwimConfig::fast());
    sim.with_lease(LEASE_MS);
    let ids = sim.cluster(3);
    sim.run_ms(1_000);
    let old = sim.cluster_id(ids[0]);
    sim.kill(ids[0]);
    let rest = &ids[1..];
    drop_from_epoch(&mut sim, rest, rest);
    sim.run_ms(20);
    assert!(rest
        .iter()
        .all(|&id| sim.node(id).swim.phase() == Phase::Seeking));
    assert!(rest.iter().all(|&id| !sim.is_normal(id)));
    sim.run_until(20_000, |s| all_normal_in_one_cluster(s, rest));
    assert_ne!(sim.cluster_id(rest[0]), old);
}

#[test]
fn a_cut_off_rejoiner_returns_to_the_live_cluster() {
    let mut sim = Sim::new(8, SwimConfig::fast());
    sim.with_lease(LEASE_MS);
    let ids = sim.cluster(3);
    sim.run_ms(1_000);
    let cluster = sim.cluster_id(ids[0]);
    // The lease holder drops out, so a renewal of its old hold is the risk.
    let holder = sim.lease().holder().unwrap().identity;
    let r = *ids.iter().find(|id| id.to_string() == holder).unwrap();
    let rest: Vec<NodeId> = ids.iter().copied().filter(|&id| id != r).collect();
    // Longer than the bootstrap deadline, so the rejoiner gives up the cluster ID, but shorter
    // than the lease, so its old hold still runs.
    let cfg = SwimConfig::fast();
    assert!(3 * cfg.bootstrap_deadline_ms < cfg.lease_duration_ms);
    sim.isolate(r, 3 * cfg.bootstrap_deadline_ms);
    drop_from_epoch(&mut sim, &ids, &[r]);
    sim.run_ms(20);
    assert_eq!(sim.node(r).swim.phase(), Phase::Seeking);
    sim.run_until(20_000, |s| all_normal_in_one_cluster(s, &ids));
    for &id in &ids {
        assert_eq!(sim.cluster_id(id), cluster);
    }
    assert_eq!(sim.formed(), 0);
    for &id in &rest {
        assert!(!sim
            .events(id)
            .iter()
            .any(|e| matches!(e, discus_membership::Event::ClusterLost { .. })));
    }
}

#[test]
fn a_seeker_of_another_cluster_does_not_win_a_merge() {
    use discus_wire::{Gossip, GossipKind};
    let mut sim = Sim::new(9, SwimConfig::fast());
    let ids = sim.cluster(2);
    let seeker = sim.add_node();
    sim.run_ms(500);
    let cluster = sim.cluster_id(ids[0]);
    let mut msg = Gossip::new(GossipKind::Ping, sim.node(seeker).swim.me().clone());
    msg.cluster_id = discus_placement::ClusterId::from_u128(u128::MAX);
    msg.cluster_size = 100;
    let src = sim.node(seeker).swim.me().addr;
    let now = sim.now();
    let node = sim.node_mut(ids[0]);
    node.swim.handle(now, src, msg, &mut node.host);
    sim.run_ms(100);
    assert_eq!(sim.cluster_id(ids[0]), cluster);
    assert!(sim.is_normal(ids[0]));
}
