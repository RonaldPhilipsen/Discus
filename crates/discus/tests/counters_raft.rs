#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::counters::{
    all_raft, group_of, held, incr_or_show, incr_until, join, key_in, leader_of, role, start,
    status, voters_of, Leases, GROUPS,
};
use common::gossip::{agreed, socket, wait_until, GossipNode, Net};
use redis::AsyncCommands;

/// Raft flags for tests: a dwell of one second, and a counter timeout that a busy test machine
/// does not run out of. The dwell flag comes first, so a test can replace it.
const RAFT: &[&str] = &[
    "--counter-dwell-ms",
    "1000",
    "--counter-timeout-ms",
    "3000",
    "--raft-voter-timeout-ms",
    "3000",
];

/// Death that takes longer than any test.
const SLOW_DEATH: &[&str] = &[
    "--suspicion-min-ms",
    "60000",
    "--suspicion-max-ms",
    "120000",
];

fn flags(extra: &[&'static str]) -> Vec<&'static str> {
    let mut f: Vec<&'static str> = RAFT.to_vec();
    f.extend_from_slice(extra);
    f
}

async fn raft_cluster(
    net: &Net,
    leases: &Leases,
    n: usize,
    extra: &[&'static str],
) -> Vec<GossipNode> {
    let (nodes, _) = start(net, leases, n, &flags(extra)).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    assert!(
        wait_until(Duration::from_secs(15), || agreed(&refs)).await,
        "no agreement"
    );
    if !wait_until(Duration::from_secs(20), || all_raft(&refs)).await {
        for n in &nodes {
            for g in 0..GROUPS {
                eprintln!("{} g{g}: {:?}", n.id, status(n, g));
            }
        }
        panic!("the groups did not reach Raft mode");
    }
    // Every entry node finds every leader before the test starts.
    for n in &nodes {
        let mut c = n.redis().await;
        for g in 0..GROUPS {
            let key = key_in(n, g, "warm");
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while redis::cmd("GET")
                .arg(&key)
                .query_async::<Option<i64>>(&mut c)
                .await
                .is_err()
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "group {g} has no leader"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    nodes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_nodes_elect_raft_leaders() {
    let _turn = common::counters::turn("three_nodes_elect_raft_leaders").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    for g in 0..GROUPS {
        let leaders = nodes.iter().filter(|n| role(n, g) == "leader").count();
        assert_eq!(leaders, 1, "group {g}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn entries_reach_every_voter() {
    let _turn = common::counters::turn("entries_reach_every_voter").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let mut c = nodes[0].redis().await;
    for i in 1..=100 {
        let n: i64 = c.incr("cnt:a", 1).await.unwrap();
        assert_eq!(n, i);
    }
    assert!(
        wait_until(Duration::from_secs(5), || nodes.iter().all(|n| held(
            n, "cnt:a"
        )
        .as_deref()
            == Some("100")))
        .await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_learner_catches_up_by_snapshot() {
    let _turn = common::counters::turn("a_learner_catches_up_by_snapshot").await;
    let (net, leases) = (Net::new(), Leases::new());
    let extra: [&'static str; 2] = ["--raft-snapshot-entries", "50"];
    let nodes = raft_cluster(&net, &leases, 3, &extra).await;
    let mut c = nodes[0].redis().await;
    let keys: Vec<String> = (0..GROUPS).map(|g| key_in(&nodes[0], g, "s")).collect();
    for _ in 0..300 {
        for k in &keys {
            let _: i64 = c.incr(k, 1).await.unwrap();
        }
    }
    let seeds: Vec<_> = nodes.iter().map(|n| n.cluster_addr).collect();
    let (fourth, _) = join(&net, &leases, socket().0, &seeds, &flags(&extra)).await;
    let mut all: Vec<&GossipNode> = nodes.iter().collect();
    all.push(&fourth);
    assert!(wait_until(Duration::from_secs(15), || agreed(&all)).await);
    let epoch = fourth.node.epochs().current().clone();
    let targeted: Vec<u16> = (0..GROUPS)
        .filter(|g| {
            epoch
                .owners(&discus::counter::group_key(*g), 3)
                .contains(&fourth.id)
        })
        .collect();
    assert!(
        !targeted.is_empty(),
        "the fourth node is a target of no group"
    );
    for g in targeted {
        let key = &keys[usize::from(g)];
        assert!(
            wait_until(Duration::from_secs(20), || role(&fourth, g) == "voter"
                || role(&fourth, g) == "leader")
            .await,
            "the fourth node never became a voter of group {g}"
        );
        assert!(
            wait_until(Duration::from_secs(5), || held(&fourth, key).as_deref()
                == Some("300"))
            .await
        );
        assert!(
            status(&fourth, g).installed >= 1,
            "group {g} sent no snapshot"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_minority_returns_tryagain() {
    let _turn = common::counters::turn("a_minority_returns_tryagain").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 5, SLOW_DEATH).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let g = group_of(&nodes[0], "cnt:min");
    let mut c = nodes[0].redis().await;
    let _: i64 = c.incr("cnt:min", 1).await.unwrap();
    let leader = leader_of(&refs, g).unwrap();
    for n in nodes.iter().filter(|n| n.id != leader.id) {
        n.node.faults.blocked.lock().insert(leader.id);
        leader.node.faults.blocked.lock().insert(n.id);
    }
    let mut lc = leader.redis().await;
    let err = redis::cmd("INCR")
        .arg("cnt:min")
        .query_async::<i64>(&mut lc)
        .await
        .unwrap_err();
    assert!(err.to_string().to_uppercase().contains("TRYAGAIN"), "{err}");
    let other = nodes.iter().find(|n| n.id != leader.id).unwrap();
    let mut oc = other.redis().await;
    let n = incr_or_show(&mut oc, "cnt:min", Duration::from_secs(15), &refs).await;
    assert!(n >= 2, "{n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_restarted_node_enters_as_a_learner() {
    let _turn = common::counters::turn("a_restarted_node_enters_as_a_learner").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 4, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let victim = voters_of(&refs, 0)[0];
    victim.kill();
    let rest: Vec<&GossipNode> = nodes.iter().filter(|n| n.id != victim.id).collect();
    assert!(wait_until(Duration::from_secs(15), || agreed(&rest)).await);
    let seeds: Vec<_> = rest.iter().map(|n| n.cluster_addr).collect();
    let (fresh, _) = join(&net, &leases, socket().0, &seeds, &flags(&[])).await;
    let mut all = rest.clone();
    all.push(&fresh);
    assert!(wait_until(Duration::from_secs(15), || agreed(&all)).await);
    assert!(
        wait_until(Duration::from_secs(20), || (0..GROUPS)
            .any(|g| role(&fresh, g) == "voter"))
        .await,
        "the new node never became a voter"
    );
    for g in 0..GROUPS {
        let s = status(&fresh, g);
        if s.regime.is_some() && matches!(s.role, "voter" | "learner") {
            assert_eq!(s.first_role, Some("learner"), "group {g}: {s:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_of_three_voters_lost() {
    let _turn = common::counters::turn("two_of_three_voters_lost").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 6, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let g = group_of(&nodes[0], "cnt:two");
    let mut c = nodes[0].redis().await;
    for _ in 0..10 {
        let _: i64 = c.incr("cnt:two", 1).await.unwrap();
    }
    let voters = voters_of(&refs, g);
    assert!(
        wait_until(Duration::from_secs(5), || voters.iter().all(|v| held(
            v, "cnt:two"
        )
        .as_deref()
            == Some("10")))
        .await
    );
    voters[0].kill();
    voters[1].kill();
    let survivor = voters[2];
    let mut sc = survivor.redis().await;
    assert_eq!(
        incr_until(&mut sc, "cnt:two", Duration::from_secs(20)).await,
        11
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_voter_lost() {
    let _turn = common::counters::turn("every_voter_lost").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 6, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let g = group_of(&nodes[0], "cnt:all");
    let mut c = nodes[0].redis().await;
    for _ in 0..10 {
        let _: i64 = c.incr("cnt:all", 1).await.unwrap();
    }
    let voters = voters_of(&refs, g);
    for v in &voters {
        v.kill();
    }
    let other = nodes
        .iter()
        .find(|n| !voters.iter().any(|v| v.id == n.id))
        .unwrap();
    let mut oc = other.redis().await;
    let rest: Vec<&GossipNode> = nodes
        .iter()
        .filter(|n| !voters.iter().any(|v| v.id == n.id))
        .collect();
    assert_eq!(
        incr_or_show(&mut oc, "cnt:all", Duration::from_secs(25), &rest).await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn force_reconfigure_acts_before_dead() {
    let _turn = common::counters::turn("force_reconfigure_acts_before_dead").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 5, SLOW_DEATH).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let g = group_of(&nodes[0], "cnt:force");
    let mut c = nodes[0].redis().await;
    for _ in 0..5 {
        let _: i64 = c.incr("cnt:force", 1).await.unwrap();
    }
    let voters = voters_of(&refs, g);
    assert!(
        wait_until(Duration::from_secs(5), || voters.iter().all(|v| held(
            v,
            "cnt:force"
        )
        .as_deref()
            == Some("5")))
        .await
    );
    voters[0].kill();
    voters[1].kill();
    let survivor = voters[2];
    let status = reqwest::Client::new()
        .post(format!(
            "http://{}/admin/raft/force-reconfigure?group={g}",
            survivor.admin
        ))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 401, "the admin route is open without credentials");
    let done = survivor
        .node
        .counters()
        .unwrap()
        .force_reconfigure(Some(g))
        .await;
    assert_eq!(done, vec![g]);
    let mut sc = survivor.redis().await;
    assert_eq!(
        incr_until(&mut sc, "cnt:force", Duration::from_secs(3)).await,
        6
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_up_switch_waits_for_the_dwell_time() {
    let _turn = common::counters::turn("the_up_switch_waits_for_the_dwell_time").await;
    let (net, leases) = (Net::new(), Leases::new());
    let mut f: Vec<&str> = RAFT[2..].to_vec();
    f.extend(["--counter-dwell-ms", "3000"]);
    let (nodes, _) = start(&net, &leases, 2, &f).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    assert!(wait_until(Duration::from_secs(10), || agreed(&refs)).await);
    let mut c = nodes[0].redis().await;
    for _ in 0..5 {
        let _: i64 = incr_until(&mut c, "cnt:up", Duration::from_secs(5)).await;
    }
    let seeds: Vec<_> = nodes.iter().map(|n| n.cluster_addr).collect();
    let (third, _) = join(&net, &leases, socket().0, &seeds, &f).await;
    let mut all = refs.clone();
    all.push(&third);
    assert!(wait_until(Duration::from_secs(10), || agreed(&all)).await);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        (0..GROUPS).all(|g| all.iter().all(|n| status(n, g).voters.is_empty())),
        "a group switched before the dwell time"
    );
    assert!(
        wait_until(Duration::from_secs(12), || all_raft(&all)).await,
        "the groups did not switch up"
    );
    assert_eq!(
        incr_until(&mut c, "cnt:up", Duration::from_secs(5)).await,
        6
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_down_switch_is_at_once() {
    let _turn = common::counters::turn("the_down_switch_is_at_once").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let mut c = nodes[0].redis().await;
    for _ in 0..7 {
        let _: i64 = c.incr("cnt:down", 1).await.unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(5), || nodes.iter().all(|n| held(
            n, "cnt:down"
        )
        .as_deref()
            == Some("7")))
        .await
    );
    nodes[2].kill();
    let rest = [&nodes[0], &nodes[1]];
    assert!(wait_until(Duration::from_secs(10), || agreed(&rest)).await);
    assert!(
        wait_until(Duration::from_secs(8), || (0..GROUPS)
            .all(
                |g| leader_of(&rest, g).is_some_and(|l| status(l, g).voters.is_empty())
            ))
        .await,
        "the groups did not switch down"
    );
    assert_eq!(
        incr_until(&mut c, "cnt:down", Duration::from_secs(5)).await,
        8
    );
}

/// `clients` connections on rotating entry nodes, `each` increments per client. Returns the
/// successes and the `-TRYAGAIN` replies.
async fn load(nodes: &[&GossipNode], key: &str, clients: usize, each: usize) -> (i64, i64) {
    let ok = Arc::new(AtomicI64::new(0));
    let failed = Arc::new(AtomicI64::new(0));
    let mut tasks = Vec::new();
    for i in 0..clients {
        let mut c = nodes[i % nodes.len()].redis().await;
        let (ok, failed, key) = (ok.clone(), failed.clone(), key.to_string());
        tasks.push(tokio::spawn(async move {
            for _ in 0..each {
                let r: redis::RedisResult<i64> =
                    redis::cmd("INCR").arg(&key).query_async(&mut c).await;
                match r {
                    Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(e) => {
                        assert!(e.to_string().to_uppercase().contains("TRYAGAIN"), "{e}");
                        failed.fetch_add(1, Ordering::Relaxed)
                    }
                };
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    (ok.load(Ordering::Relaxed), failed.load(Ordering::Relaxed))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_final_value_counts_every_success() {
    let _turn = common::counters::turn("the_final_value_counts_every_success").await;
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 4, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let (ok, failed) = load(&refs, "cnt:lin", 4, 250).await;
    assert_eq!(failed, 0, "a steady cluster refused increments");
    let mut c = nodes[0].redis().await;
    let total: i64 = c.get("cnt:lin").await.unwrap();
    assert_eq!(total, ok);

    let g = group_of(&nodes[0], "cnt:lin2");
    let leader = leader_of(&refs, g).unwrap();
    let rest: Vec<&GossipNode> = nodes.iter().filter(|n| n.id != leader.id).collect();
    let loaded = load(&rest, "cnt:lin2", 4, 300);
    let killer = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        leader.kill();
    };
    let ((ok, failed), ()) = tokio::join!(loaded, killer);
    let mut c = rest[0].redis().await;
    let total = incr_until(&mut c, "cnt:lin2", Duration::from_secs(10)).await - 1;
    assert!(
        ok <= total && total <= ok + failed,
        "{ok} successes and {failed} failures, but the value is {total}"
    );
}
