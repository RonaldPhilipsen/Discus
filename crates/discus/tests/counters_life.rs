#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::counters::{
    all_hold, flags, group_of, incr_until, join_to, leader_of, query_until, raft_cluster, status,
    targeted, Leases, GROUPS,
};
use common::gossip::{heal, partition, refs, wait_agreed, wait_until, GossipNode, Net};
use common::status as http_status;
use redis::AsyncCommands;
use serial_test::serial;

/// Every group whose targets include `n` has `n` as a voter or the leader.
fn promoted(n: &GossipNode) -> bool {
    targeted(n)
        .iter()
        .all(|g| matches!(status(n, *g).role, "voter" | "leader"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn startupz_waits_for_counter_promotion() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let (fourth, _) = join_to(&net, &leases, &refs(&nodes), &flags(&[])).await;
    fourth
        .node
        .faults
        .stall_counter_ms
        .store(400, Ordering::Relaxed);
    let mut all = refs(&nodes);
    all.push(&fourth);
    wait_agreed(&all, Duration::from_secs(15)).await;
    let mut waited = false;
    let unstall = tokio::time::Instant::now() + Duration::from_secs(3);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() >= unstall {
            fourth
                .node
                .faults
                .stall_counter_ms
                .store(0, Ordering::Relaxed);
        }
        let code = http_status(fourth.admin, "/startupz").await;
        if code == 200 {
            assert!(
                promoted(&fourth),
                "the startup probe passed before the promotion"
            );
            break;
        }
        waited |= fourth.is_normal() && !promoted(&fourth);
        assert!(
            tokio::time::Instant::now() < deadline,
            "the startup probe never passed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(waited, "the startup probe never waited for the counters");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn drain_moves_raft_roles_away() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 4, &["--shutdown-timeout-secs", "15"]).await;
    let refs = refs(&nodes);
    let leaving = nodes
        .iter()
        .find(|n| (0..GROUPS).any(|g| status(n, g).role == "leader"))
        .unwrap();
    let rest: Vec<&GossipNode> = refs
        .iter()
        .copied()
        .filter(|n| n.id != leaving.id)
        .collect();
    let mut c = rest[0].redis().await;
    for g in 0..GROUPS {
        let key = common::counters::key_in(rest[0], g, "d");
        let _: i64 = incr_until(&mut c, &key, Duration::from_secs(5)).await;
    }
    leaving.node.drain_token().cancel();
    let moved = || {
        (0..GROUPS).all(|g| {
            leader_of(&rest, g).is_some_and(|l| {
                let s = status(l, g);
                s.voters.len() == 3 && !s.voters.contains(&leaving.id)
            })
        })
    };
    assert!(
        wait_until(Duration::from_secs(15), moved).await,
        "the draining node kept a voter slot"
    );
    for g in 0..GROUPS {
        let key = common::counters::key_in(rest[0], g, "d");
        assert_eq!(incr_until(&mut c, &key, Duration::from_secs(5)).await, 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn a_rejoined_node_discards_its_counters() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 4, &[]).await;
    let replicas = |n: &GossipNode| -> HashSet<u64> {
        (0..GROUPS).filter_map(|g| status(n, g).replica).collect()
    };
    let old: Vec<HashSet<u64>> = nodes.iter().map(replicas).collect();
    // Either side can win the merge, so record which nodes join again.
    let rejoined: Vec<Arc<AtomicBool>> = nodes
        .iter()
        .map(|n| {
            let flag = Arc::new(AtomicBool::new(false));
            let mut events = n.node.membership().unwrap().subscribe();
            let f = flag.clone();
            tokio::spawn(async move {
                while let Ok(e) = events.recv().await {
                    if matches!(
                        e,
                        discus::membership::Event::Core(discus_membership::Event::Rejoining { .. })
                    ) {
                        f.store(true, Ordering::Relaxed);
                    }
                }
            });
            flag
        })
        .collect();
    let x = &nodes[3];
    partition(x, &refs(&nodes[..3]));
    let x_alone = || x.node.epochs().current().members().len() == 1;
    let rest_alone = || {
        nodes[..3]
            .iter()
            .all(|n| n.node.epochs().current().member(x.id).is_none())
    };
    assert!(
        wait_until(Duration::from_secs(20), || x_alone() && rest_alone()).await,
        "the partition did not split the epochs"
    );
    heal(x, &refs(&nodes[..3]));
    let refs = refs(&nodes);
    wait_agreed(&refs, Duration::from_secs(30)).await;
    let losers: Vec<usize> = (0..nodes.len())
        .filter(|i| rejoined[*i].load(Ordering::Relaxed))
        .collect();
    assert!(!losers.is_empty(), "no node joined again");
    for i in losers {
        let now = replicas(&nodes[i]);
        assert!(
            now.is_disjoint(&old[i]),
            "node {i} kept a replica: {now:?} and {:?}",
            old[i]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn a_false_dead_heals_to_one_value() {
    let net = Net::new();
    let nodes = net.start_all(3, &["--counter-groups", "4"]).await;
    let refs = refs(&nodes);
    wait_agreed(&refs, Duration::from_secs(10)).await;
    let mut c = nodes[0].redis().await;
    assert_eq!(
        incr_until(&mut c, "cnt:fd", Duration::from_secs(10)).await,
        1
    );
    let x = &nodes[2];
    partition(x, &refs[..2]);
    assert!(
        wait_until(Duration::from_secs(20), || x
            .node
            .epochs()
            .current()
            .members()
            .len()
            == 1
            && nodes[0].node.epochs().current().member(x.id).is_none())
        .await
    );
    let mut cx = x.redis().await;
    let on_x = incr_until(&mut cx, "cnt:fd", Duration::from_secs(10)).await;
    let mut on_rest = 0;
    for _ in 0..3 {
        on_rest = incr_until(&mut c, "cnt:fd", Duration::from_secs(10)).await;
    }
    heal(x, &refs[..2]);
    wait_agreed(&refs, Duration::from_secs(30)).await;
    let mut values = Vec::new();
    for n in &nodes {
        let mut client = n.redis().await;
        let get = redis::cmd("GET").arg("cnt:fd").clone();
        let v: Option<i64> = query_until(&mut client, &get, Duration::from_secs(10), |_| true)
            .await
            .unwrap();
        values.push(v);
    }
    assert!(values.iter().all(|v| *v == values[0]), "{values:?}");
    let v = values[0];
    assert!(
        v == Some(on_x) || v == Some(on_rest),
        "{v:?} is neither side: {on_x} or {on_rest}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn growth_from_one_to_three_and_back() {
    let (net, leases) = (Net::new(), Leases::new());
    let extra: Vec<&'static str> =
        flags(&["--replication-factor", "3", "--shutdown-timeout-secs", "15"]);
    let (first, _) = join_to(&net, &leases, &[], &extra).await;
    let mut c = first.redis().await;
    for i in 1..=50 {
        assert_eq!(
            incr_until(&mut c, "cnt:grow", Duration::from_secs(10)).await,
            i
        );
    }
    let _: () = c.set("plain", "v").await.unwrap();
    let holders = |nodes: &[&GossipNode]| {
        nodes
            .iter()
            .filter(|n| n.node.store.entry(b"plain").is_some())
            .count()
    };

    let (second, _) = join_to(&net, &leases, &[&first], &extra).await;
    let two = [&first, &second];
    wait_agreed(&two, Duration::from_secs(10)).await;
    let g = group_of(&first, "cnt:grow");
    assert!(
        all_hold(&two, "cnt:grow", "50", Duration::from_secs(10)).await,
        "the second node got no snapshot"
    );
    assert!(
        wait_until(Duration::from_secs(10), || holders(&two) == 2).await,
        "RF did not reach 2"
    );
    let mut c2 = second.redis().await;
    assert_eq!(
        incr_until(&mut c2, "cnt:grow", Duration::from_secs(5)).await,
        51
    );

    let (third, _) = join_to(&net, &leases, &[&first, &second], &extra).await;
    let three = [&first, &second, &third];
    wait_agreed(&three, Duration::from_secs(10)).await;
    assert!(
        wait_until(Duration::from_secs(20), || common::counters::all_raft(
            &three
        ))
        .await,
        "the groups did not switch to Raft"
    );
    assert!(
        wait_until(Duration::from_secs(10), || holders(&three) == 3).await,
        "RF did not reach 3"
    );
    assert_eq!(
        incr_until(&mut c, "cnt:grow", Duration::from_secs(5)).await,
        52
    );

    third.node.drain_token().cancel();
    wait_agreed(&two, Duration::from_secs(20)).await;
    assert!(
        wait_until(Duration::from_secs(10), || leader_of(&two, g)
            .is_some_and(|l| status(l, g).voters.is_empty()))
        .await,
        "the group did not switch down"
    );
    assert_eq!(
        incr_until(&mut c, "cnt:grow", Duration::from_secs(10)).await,
        53
    );

    second.node.drain_token().cancel();
    wait_agreed(&[&first], Duration::from_secs(20)).await;
    assert_eq!(
        incr_until(&mut c, "cnt:grow", Duration::from_secs(10)).await,
        54
    );
}
