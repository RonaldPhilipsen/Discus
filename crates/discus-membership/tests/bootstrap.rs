#![allow(clippy::unwrap_used)]

use std::net::{IpAddr, Ipv4Addr};

use discus_membership::merge::JoinLimiter;
use discus_membership::sim::Sim;
use discus_membership::{MemoryLease, SwimConfig};
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

#[test]
fn a_discovery_refresh_keeps_learned_seeds() {
    use discus_membership::Swim;
    use discus_wire::{Capacity, MemberInfo};
    let me = MemberInfo {
        id: NodeId::from_u128(1),
        addr: "10.0.0.1:7379".parse().unwrap(),
        rpc_addr: "10.0.0.1:7379".parse().unwrap(),
        state: MemberState::Joining,
        incarnation: 0,
        weight: Default::default(),
        epoch: discus_membership::provisional_epoch(NodeId::from_u128(1)).id(),
        capabilities: 0,
        capacity: Capacity::default(),
        leader_hints: Default::default(),
    };
    let mut swim = Swim::new(SwimConfig::fast(), me);
    let holder = "10.0.0.9:7379".parse().unwrap();
    swim.add_seed(holder);
    swim.set_seeds(vec!["10.0.0.2:7379".parse().unwrap()]);
    assert!(swim.seeds().contains(&holder));
    assert_eq!(swim.seeds().len(), 2);
}
