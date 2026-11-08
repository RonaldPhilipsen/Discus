#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::counters::{
    all_raft, flags, group_of, held, incr_once, incr_or_show, incr_until, join_to, key_in,
    leader_of, raft_cluster, role, start, status, voters_of, Leases, GROUPS, RAFT, SLOW_DEATH,
};
use common::gossip::{refs, wait_agreed, wait_until, GossipNode, Net};
use redis::AsyncCommands;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn three_nodes_elect_raft_leaders() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    for g in 0..GROUPS {
        let leaders = nodes.iter().filter(|n| role(n, g) == "leader").count();
        assert_eq!(leaders, 1, "group {g}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn entries_reach_every_voter() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let mut c = nodes[0].redis().await;
    for i in 1..=100 {
        let n: i64 = incr_once(&mut c, "cnt:a").await;
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
#[serial]
async fn a_learner_catches_up_by_snapshot() {
    let (net, leases) = (Net::new(), Leases::new());
    let extra: [&'static str; 2] = ["--raft-snapshot-entries", "50"];
    let nodes = raft_cluster(&net, &leases, 3, &extra).await;
    let mut c = nodes[0].redis().await;
    let keys: Vec<String> = (0..GROUPS).map(|g| key_in(&nodes[0], g, "s")).collect();
    for _ in 0..300 {
        for k in &keys {
            let _: i64 = incr_once(&mut c, k).await;
        }
    }
    let (fourth, _) = join_to(&net, &leases, &refs(&nodes), &flags(&extra)).await;
    let mut all = refs(&nodes);
    all.push(&fourth);
    wait_agreed(&all, Duration::from_secs(15)).await;
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
#[serial]
async fn a_minority_returns_tryagain() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 5, SLOW_DEATH).await;
    let refs = refs(&nodes);
    let g = group_of(&nodes[0], "cnt:min");
    let mut c = nodes[0].redis().await;
    let _: i64 = incr_once(&mut c, "cnt:min").await;
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
#[serial]
async fn a_restarted_node_enters_as_a_learner() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 4, &[]).await;
    let refs = refs(&nodes);
    let victim = voters_of(&refs, 0)[0];
    victim.kill();
    let rest: Vec<&GossipNode> = nodes.iter().filter(|n| n.id != victim.id).collect();
    wait_agreed(&rest, Duration::from_secs(15)).await;
    let (fresh, _) = join_to(&net, &leases, &rest, &flags(&[])).await;
    let mut all = rest.clone();
    all.push(&fresh);
    wait_agreed(&all, Duration::from_secs(15)).await;
    let targeted: Vec<u16> = (0..GROUPS)
        .filter(|g| {
            fresh
                .node
                .epochs()
                .current()
                .owners(&discus::counter::group_key(*g), 3)
                .contains(&fresh.id)
        })
        .collect();
    let promoted = || {
        targeted
            .iter()
            .all(|g| matches!(role(&fresh, *g), "voter" | "leader"))
    };
    if !wait_until(Duration::from_secs(30), promoted).await {
        for n in &all {
            for g in 0..GROUPS {
                eprintln!("{} g{g}: {:?}", n.id, status(n, g));
            }
        }
        panic!("the new node did not become a voter of {targeted:?}");
    }
    for g in 0..GROUPS {
        let s = status(&fresh, g);
        if s.regime.is_some() && matches!(s.role, "voter" | "learner") {
            assert_eq!(s.first_role, Some("learner"), "group {g}: {s:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn two_of_three_voters_lost() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 6, &[]).await;
    let refs = refs(&nodes);
    let g = group_of(&nodes[0], "cnt:two");
    let mut c = nodes[0].redis().await;
    for _ in 0..10 {
        let _: i64 = incr_once(&mut c, "cnt:two").await;
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
#[serial]
async fn every_voter_lost() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 6, &[]).await;
    let refs = refs(&nodes);
    let g = group_of(&nodes[0], "cnt:all");
    let mut c = nodes[0].redis().await;
    for _ in 0..10 {
        let _: i64 = incr_once(&mut c, "cnt:all").await;
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
#[serial]
async fn force_reconfigure_acts_before_dead() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 5, SLOW_DEATH).await;
    let refs = refs(&nodes);
    let g = group_of(&nodes[0], "cnt:force");
    let mut c = nodes[0].redis().await;
    for _ in 0..5 {
        let _: i64 = incr_once(&mut c, "cnt:force").await;
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
#[serial]
async fn the_up_switch_waits_for_the_dwell_time() {
    let (net, leases) = (Net::new(), Leases::new());
    let mut f: Vec<&str> = RAFT[2..].to_vec();
    f.extend(["--counter-dwell-ms", "3000"]);
    let (nodes, _) = start(&net, &leases, 2, &f).await;
    let refs = refs(&nodes);
    wait_agreed(&refs, Duration::from_secs(10)).await;
    let mut c = nodes[0].redis().await;
    for _ in 0..5 {
        let _: i64 = incr_until(&mut c, "cnt:up", Duration::from_secs(5)).await;
    }
    let (third, _) = join_to(&net, &leases, &refs, &f).await;
    let mut all = refs.clone();
    all.push(&third);
    wait_agreed(&all, Duration::from_secs(10)).await;
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
#[serial]
async fn the_down_switch_is_at_once() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let mut c = nodes[0].redis().await;
    for _ in 0..7 {
        let _: i64 = incr_once(&mut c, "cnt:down").await;
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
    wait_agreed(&rest, Duration::from_secs(10)).await;
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
#[serial]
async fn the_final_value_counts_every_success() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 4, &[]).await;
    let refs = refs(&nodes);
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

/// The Raft leader drains while the other nodes write to its group. The group switches down,
/// and the counter keeps every increment that a client saw succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn a_draining_raft_leader_keeps_every_increment() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let refs = refs(&nodes);
    let key = "cnt:drain";
    let g = group_of(&nodes[0], key);
    let leader = leader_of(&refs, g).unwrap();
    let rest: Vec<&GossipNode> = nodes.iter().filter(|n| n.id != leader.id).collect();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (ok, ambiguous) = (Arc::new(AtomicI64::new(0)), Arc::new(AtomicI64::new(0)));
    let mut load = Vec::new();
    for i in 0..4 {
        let (mut c, stop, ok, ambiguous) = (
            rest[i % 2].redis().await,
            stop.clone(),
            ok.clone(),
            ambiguous.clone(),
        );
        load.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let r: redis::RedisResult<i64> =
                    redis::cmd("INCR").arg(key).query_async(&mut c).await;
                match r {
                    Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(e) if e.to_string().contains("has no leader") => 0,
                    Err(_) => ambiguous.fetch_add(1, Ordering::Relaxed),
                };
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    leader.node.drain_token().cancel();
    wait_agreed(&rest, Duration::from_secs(20)).await;
    assert!(
        wait_until(Duration::from_secs(15), || leader_of(&rest, g)
            .is_some_and(|l| status(l, g).voters.is_empty()))
        .await,
        "the group did not switch down"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    for task in load {
        task.await.unwrap();
    }

    let (ok, ambiguous) = (
        ok.load(Ordering::Relaxed),
        ambiguous.load(Ordering::Relaxed),
    );
    let mut c = rest[0].redis().await;
    let value: i64 = c.get(key).await.unwrap();
    assert!(ok > 0, "the load did not run");
    assert!(
        ok <= value && value <= ok + ambiguous,
        "the counter is {value}, with {ok} successes and {ambiguous} ambiguous increments"
    );
}
