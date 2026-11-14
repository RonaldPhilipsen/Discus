#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use discus_membership::sim::Sim;
use discus_membership::{SwimConfig, DATABASES_DWELL_MS};
use discus_placement::NodeId;
use discus_wire::{caps, Gossip, GossipKind, Liveness, MemberInfo};

#[test]
fn a_dead_node_is_detected() {
    let mut sim = Sim::new(1, SwimConfig::fast());
    let ids = sim.cluster(5);
    sim.run_ms(1_000);
    sim.kill(ids[4]);
    sim.run_until(3_000, |s| {
        ids[..4]
            .iter()
            .all(|&o| s.liveness(o, ids[4]) == Some(Liveness::Dead))
    });
}

#[test]
fn a_healthy_cluster_stays_alive() {
    let mut sim = Sim::new(2, SwimConfig::fast());
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
    let mut sim = Sim::new(3, SwimConfig::fast());
    let ids = sim.cluster(4);
    sim.run_ms(1_000);
    let before = sim.incarnation(ids[2]);
    sim.isolate(ids[2], 300);
    // Some node suspects it during the isolation.
    sim.run_until(300, |s| {
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
    let mut sim = Sim::new(4, SwimConfig::fast());
    let ids = sim.cluster(16);
    sim.run_ms(1_000);
    sim.set_leader_hints(ids[0], Bytes::from_static(b"g0"));
    let want = sim.incarnation(ids[0]);
    let took = sim.run_until(10 * 5 * 100, |s| {
        ids[1..]
            .iter()
            .all(|&o| s.known_incarnation(o, ids[0]) == Some(want))
    });
    assert!(took <= 10 * 5 * 100, "{took}");
}

fn time_to_dead(evidence: bool) -> u64 {
    let mut sim = Sim::new(5, SwimConfig::fast());
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
    sim.run_until(10_000, |s| {
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
    let mut sim = Sim::new(6, SwimConfig::fast());
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
    let c = SwimConfig::fast();
    let bound = 1000.0 / c.period_ms as f64 * (1 + c.indirect_probes) as f64 + 2.0;
    for n in [4, 16] {
        let r = rate(n);
        assert!(
            r <= bound,
            "{n} nodes: {r} messages per second, bound {bound}"
        );
    }
}

#[test]
fn a_direct_rumor_at_the_same_incarnation_refreshes_the_rpc_address() {
    use discus_membership::{provisional_epoch, Swim};
    use discus_placement::{MemberState, NodeId};
    use discus_wire::{Capacity, MemberInfo, Rumor};

    let info = |n: u128, rpc_port: u16| MemberInfo {
        id: NodeId::from_u128(n),
        addr: "10.0.0.1:7379".parse().unwrap(),
        rpc_addr: format!("10.0.0.1:{rpc_port}").parse().unwrap(),
        client_addr: None,
        client_host: None,
        state: MemberState::Normal,
        incarnation: 5,
        weight: Default::default(),
        epoch: provisional_epoch(NodeId::from_u128(n)).id(),
        capabilities: 0,
        capacity: Capacity::default(),
        leader_hints: Bytes::new(),
        databases: 1,
    };
    let rumor = |info| Rumor {
        info,
        liveness: Liveness::Alive,
        accuser: None,
    };
    let mut swim = Swim::new(SwimConfig::fast(), info(1, 7380));
    swim.learn(0, rumor(info(2, 7380)), true);
    swim.learn(10, rumor(info(2, 9000)), true);
    let peer = swim.member(NodeId::from_u128(2)).unwrap();
    assert_eq!(peer.rpc_addr, "10.0.0.1:9000".parse().unwrap());
}

fn multi_db(databases: u32) -> impl FnMut(usize, &mut MemberInfo, &mut SwimConfig) {
    move |_, info, _| {
        info.databases = databases;
        info.capabilities = caps::KNOWN;
    }
}

/// A joiner of a build without numbered databases.
fn without_multi_db(info: &mut MemberInfo, _: &mut SwimConfig) {
    info.capabilities = caps::KNOWN & !caps::MULTI_DB;
}

/// Every node of `ids` lists `j` as `NORMAL` in its epoch.
fn placed(sim: &Sim, ids: &[NodeId], j: NodeId) -> bool {
    ids.iter().all(|&o| sim.epoch(o).is_normal(j))
}

fn no_drops(sim: &Sim, ids: &[NodeId]) {
    for &o in ids {
        assert!(
            sim.database_drops(o).is_empty(),
            "{o} dropped {:?}",
            sim.database_drops(o)
        );
    }
}

#[test]
fn the_cluster_count_is_the_smallest_normal_count() {
    let mut sim = Sim::new(7, SwimConfig::fast());
    let counts = [3, 3, 1];
    let ids = sim.cluster_with(3, |i, info, _| {
        info.databases = counts[i];
        info.capabilities = caps::KNOWN;
    });
    sim.run_ms(1_000);
    for &o in &ids {
        assert_eq!(sim.cluster_databases(o), Some(1), "{o}");
        // A node never serves its own count before the cluster does: a write to database 2
        // there would be deleted when the lower count applies.
        assert_eq!(sim.served_databases(o), 1, "{o}");
    }
    sim.set_databases(ids[2], 3);
    assert_eq!(sim.node(ids[2]).swim.me().databases, 3);
    sim.run_until(10_000, |s| {
        ids.iter()
            .all(|&o| s.cluster_databases(o) == Some(3) && s.served_databases(o) == 3)
    });
    no_drops(&sim, &ids);
}

#[test]
fn a_joiner_takes_the_larger_count() {
    let mut sim = Sim::new(8, SwimConfig::fast());
    let ids = sim.cluster_with(3, multi_db(3));
    sim.run_ms(1_000);
    let j = sim.add_node_with(|info, cfg| multi_db(1)(0, info, cfg));
    sim.start(j, &[ids[1]]);
    let mut lowest = u32::MAX;
    sim.run_until(30_000, |s| {
        lowest = ids
            .iter()
            .filter_map(|&o| s.cluster_databases(o))
            .fold(lowest, u32::min);
        s.is_normal(j) && placed(s, &ids, j)
    });
    assert_eq!(sim.node(j).swim.me().databases, 3);
    assert_eq!(lowest, 3, "the cluster count dropped during the join");
    sim.run_ms(DATABASES_DWELL_MS * 2);
    let all: Vec<NodeId> = ids.iter().copied().chain([j]).collect();
    for &o in &all {
        assert_eq!(sim.cluster_databases(o), Some(3), "{o}");
        assert_eq!(sim.served_databases(o), 3, "{o}");
    }
    no_drops(&sim, &all);
}

#[test]
fn a_lower_databases_joiner_lowers_the_count_after_the_dwell() {
    let mut sim = Sim::new(9, SwimConfig::fast());
    let ids = sim.cluster_with(3, multi_db(3));
    sim.run_ms(1_000);
    let j = sim.add_node_with(|info, cfg| {
        multi_db(1)(0, info, cfg);
        cfg.lower_databases = true;
    });
    sim.start(j, &[ids[1]]);
    let mut first_seen: Vec<Option<u64>> = vec![None; ids.len()];
    let mut see = |s: &Sim| {
        for (i, &o) in ids.iter().enumerate() {
            if first_seen[i].is_none() && s.cluster_databases(o) == Some(1) {
                first_seen[i] = Some(s.now());
            }
        }
    };
    sim.run_until(30_000, |s| {
        see(s);
        s.is_normal(j) && placed(s, &ids, j)
    });
    assert_eq!(sim.node(j).swim.me().databases, 1);
    sim.run_until(DATABASES_DWELL_MS * 3, |s| {
        see(s);
        ids.iter().chain([&j]).all(|&o| s.served_databases(o) == 1)
    });
    for (i, &o) in ids.iter().enumerate() {
        let seen = first_seen[i].expect("the node saw the lower count");
        let drops = sim.database_drops(o);
        assert_eq!(drops.len(), 1, "{o}: {drops:?}");
        let (at, count) = drops[0];
        assert_eq!(count, 1);
        assert!(
            at - seen >= DATABASES_DWELL_MS,
            "{o} dropped {} ms after it saw the lower count",
            at - seen
        );
    }
}

#[test]
fn a_dip_shorter_than_the_dwell_drops_nothing() {
    let mut sim = Sim::new(14, SwimConfig::fast());
    let ids = sim.cluster_with(3, multi_db(3));
    sim.run_ms(1_000);
    let j = sim.add_node_with(|info, cfg| {
        multi_db(1)(0, info, cfg);
        cfg.lower_databases = true;
    });
    sim.start(j, &[ids[1]]);
    sim.run_until(30_000, |s| {
        s.is_normal(j) && ids.iter().any(|&o| s.cluster_databases(o) == Some(1))
    });
    let dip = sim.now();
    sim.kill(j);
    sim.run_until(DATABASES_DWELL_MS, |s| {
        ids.iter().all(|&o| s.cluster_databases(o) == Some(3))
    });
    assert!(sim.now() - dip < DATABASES_DWELL_MS);
    sim.run_ms(DATABASES_DWELL_MS * 2);
    for &o in &ids {
        assert_eq!(sim.served_databases(o), 3, "{o}");
    }
    no_drops(&sim, &ids);
}

#[test]
fn a_member_without_a_record_keeps_the_view() {
    use discus_placement::{Epoch, EpochId, Member, MemberState, Weight};

    let mut sim = Sim::new(15, SwimConfig::fast());
    let ids = sim.cluster_with(3, multi_db(3));
    sim.run_ms(1_000);
    let current = sim.epoch(ids[0]);
    let unknown = NodeId::from_u128(u128::MAX);
    let addr = "10.9.9.9:7379".parse().unwrap();
    let mut members = current.members().to_vec();
    members.push(Member {
        id: unknown,
        addr,
        rpc_addr: addr,
        client_addr: None,
        client_host: None,
        state: MemberState::Normal,
        weight: Weight::default(),
    });
    let id = EpochId {
        generation: current.id().generation + 1,
        origin: ids[0],
    };
    let epoch = Epoch::new(id, current.cluster_id(), members).unwrap();
    sim.adopt(ids[0], epoch);
    assert!(sim.epoch(ids[0]).is_normal(unknown));
    assert_eq!(sim.cluster_databases(ids[0]), None);
    sim.run_ms(DATABASES_DWELL_MS * 2);
    for &o in &ids {
        assert_eq!(sim.served_databases(o), 3, "{o}");
    }
    no_drops(&sim, &ids);
}

/// Ask `member` to admit `j` with one `JOIN`, as the joiner would.
fn join_reply(sim: &mut Sim, member: NodeId, j: NodeId) -> Gossip {
    let from = sim.node(j).swim.me().clone();
    let src = from.addr;
    let now = sim.now();
    let node = sim.node_mut(member);
    let handled = node.swim.handle(
        now,
        src,
        Gossip::new(GossipKind::Join, from),
        &mut node.host,
    );
    handled.reply
}

#[test]
fn a_node_without_multi_db_cannot_join_a_multi_database_cluster() {
    let mut sim = Sim::new(10, SwimConfig::fast());
    let ids = sim.cluster_with(3, multi_db(3));
    sim.run_ms(1_000);
    let j = sim.add_node_with(without_multi_db);
    assert!(join_reply(&mut sim, ids[0], j).retry_after_ms > 0);
    sim.start(j, &[ids[1]]);
    sim.run_ms(20_000);
    assert!(!sim.is_normal(j));
    for &o in &ids {
        assert!(sim.epoch(o).member(j).is_none(), "{o} placed the joiner");
        assert_eq!(sim.cluster_databases(o), Some(3));
    }

    let mut sim = Sim::new(11, SwimConfig::fast());
    let ids = sim.cluster_with(3, multi_db(1));
    sim.run_ms(1_000);
    let j = sim.add_node_with(without_multi_db);
    sim.start(j, &[ids[1]]);
    sim.run_until(30_000, |s| s.is_normal(j) && placed(s, &ids, j));
}

#[test]
fn all_multi_db_needs_every_member() {
    let mut sim = Sim::new(12, SwimConfig::fast());
    let ids = sim.cluster_with(3, multi_db(1));
    sim.run_ms(1_000);
    for &o in &ids {
        assert!(sim.all_multi_db(o), "{o}");
    }

    let mut sim = Sim::new(13, SwimConfig::fast());
    let ids = sim.cluster_with(3, |i, info, cfg| {
        multi_db(1)(i, info, cfg);
        if i == 2 {
            info.capabilities = caps::KNOWN & !caps::MULTI_DB;
        }
    });
    sim.run_ms(1_000);
    for &o in &ids {
        assert!(!sim.all_multi_db(o), "{o}");
    }
}
