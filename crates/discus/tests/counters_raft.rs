#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::counters::{
    all_hold, all_raft, dump, flags, group_of, incr_once, incr_or_show, incr_until, is_refused,
    is_try_again, join_to, key_in, leader_of, purge_reaches_every_replica, raft_cluster, role,
    start, status, targeted, targets, voters_of, Leases, GROUPS, RAFT, SLOW_DEATH,
};
use common::gossip::{heal, partition, refs, wait_agreed, wait_until, GossipNode, Net};
use discus::counter::group_key;
use discus_placement::NodeId;
use redis::AsyncCommands;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn entries_reach_every_voter() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let mut c = nodes[0].redis().await;
    for i in 1..=100 {
        let n: i64 = incr_once(&mut c, "cnt:a").await;
        assert_eq!(n, i);
    }
    assert!(all_hold(&refs(&nodes), "cnt:a", "100", Duration::from_secs(5)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
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
    let targeted = targeted(&fourth);
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
        assert!(all_hold(&[&fourth], key, "300", Duration::from_secs(5)).await);
        assert!(
            status(&fourth, g).installed >= 1,
            "group {g} sent no snapshot"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_minority_returns_tryagain() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 4, SLOW_DEATH).await;
    let refs = refs(&nodes);
    let g = group_of(&nodes[0], "cnt:min");
    let mut c = nodes[0].redis().await;
    let _: i64 = incr_once(&mut c, "cnt:min").await;
    let leader = leader_of(&refs, g).unwrap();
    let rest: Vec<&GossipNode> = nodes.iter().filter(|n| n.id != leader.id).collect();
    partition(leader, &rest);
    let mut lc = leader.redis().await;
    let err = redis::cmd("INCR")
        .arg("cnt:min")
        .query_async::<i64>(&mut lc)
        .await
        .unwrap_err();
    assert!(is_try_again(&err), "{err}");
    let mut oc = rest[0].redis().await;
    let n = incr_or_show(&mut oc, "cnt:min", Duration::from_secs(15), &refs).await;
    assert!(n >= 2, "{n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
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
    let targeted = targeted(&fresh);
    let promoted = || {
        targeted
            .iter()
            .all(|g| matches!(role(&fresh, *g), "voter" | "leader"))
    };
    assert!(
        wait_until(Duration::from_secs(30), promoted).await,
        "the new node did not become a voter of {targeted:?}\n{}",
        dump(&all)
    );
    for g in 0..GROUPS {
        let s = status(&fresh, g);
        if s.regime.is_some() && matches!(s.role, "voter" | "learner") {
            assert_eq!(s.first_role, Some("learner"), "group {g}: {s:?}");
        }
    }
}

/// A voter that the leader removes while it hears nothing keeps its old membership: the leader
/// no longer sends it the entries that drop it. Once it hears again, it must leave the group, not
/// start elections that the other voters refuse forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_voter_that_missed_its_removal_leaves() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, SLOW_DEATH).await;
    let old = refs(&nodes);
    // A node ID for the fourth node that takes the place of an old voter that leads nothing.
    let members = old[0].node.epochs().current().members().to_vec();
    let (fresh_id, x, g) = std::iter::repeat_with(NodeId::random)
        .find_map(|id| {
            let mut m = members.clone();
            let mut joined = m[0].clone();
            joined.id = id;
            m.push(joined);
            (0..GROUPS).find_map(|g| {
                let new = discus_placement::hrw::owners(&group_key(g), &m, 3);
                let leader = leader_of(&old, g)?.id;
                old.iter()
                    .find(|n| !new.contains(&n.id) && n.id != leader)
                    .map(|x| (id, *x, g))
            })
        })
        .unwrap();
    let rest: Vec<&GossipNode> = old.iter().copied().filter(|n| n.id != x.id).collect();
    partition(x, &rest);
    x.node.faults.blocked.lock().insert(fresh_id);
    let id_flag: &'static str = Box::leak(fresh_id.to_string().into_boxed_str());
    let mut f = flags(SLOW_DEATH);
    f.extend(["--node-id", id_flag]);
    let (fresh, _) = join_to(&net, &leases, &rest, &f).await;
    fresh.node.faults.blocked.lock().insert(x.id);
    let mut all = old.clone();
    all.push(&fresh);
    // The cut-off node cannot take part in the epoch change, so only the others agree.
    let mut ids: Vec<NodeId> = all.iter().map(|n| n.id).collect();
    ids.sort();
    let joined = || {
        rest.iter().chain([&&fresh]).all(|n| {
            let e = n.node.epochs().current().clone();
            let mut listed: Vec<NodeId> = e.members().iter().map(|m| m.id).collect();
            listed.sort();
            listed == ids && n.is_normal()
        })
    };
    assert!(
        wait_until(Duration::from_secs(20), joined).await,
        "the fourth node did not join"
    );
    let mut want = targets(&fresh, g);
    want.sort();
    let moved = || {
        leader_of(&all, g).is_some_and(|l| {
            let mut v = status(l, g).voters;
            v.sort();
            v == want
        })
    };
    assert!(
        wait_until(Duration::from_secs(30), moved).await,
        "group {g} did not move to its new targets\n{}",
        dump(&all)
    );
    heal(x, &rest);
    x.node.faults.blocked.lock().remove(&fresh.id);
    fresh.node.faults.blocked.lock().remove(&x.id);
    assert!(
        wait_until(Duration::from_secs(30), || role(x, g) == "idle").await,
        "the removed voter still holds group {g}\n{}",
        dump(&all)
    );
    // The promoted fourth node and the other voters stay past the wait that removes a replica.
    tokio::time::sleep(Duration::from_secs(7)).await;
    for n in all.iter().filter(|n| want.contains(&n.id)) {
        assert!(
            matches!(role(n, g), "voter" | "leader"),
            "a target left group {g}\n{}",
            dump(&all)
        );
    }
    let leader = leader_of(&all, g).unwrap();
    let mut c = leader.redis().await;
    let _: i64 = incr_until(&mut c, &key_in(leader, g, "gone"), Duration::from_secs(10)).await;
}

/// `incrs` increments of `key` through `nodes[0]`, then the group of `key` and its voters, once
/// every voter holds the value.
async fn voters_holding<'a>(
    nodes: &'a [GossipNode],
    key: &str,
    incrs: i64,
) -> (u16, Vec<&'a GossipNode>) {
    let g = group_of(&nodes[0], key);
    let mut c = nodes[0].redis().await;
    for _ in 0..incrs {
        incr_once(&mut c, key).await;
    }
    let voters = voters_of(&refs(nodes), g);
    assert!(all_hold(&voters, key, &incrs.to_string(), Duration::from_secs(5)).await);
    (g, voters)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_of_three_voters_lost() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 5, &[]).await;
    let (_, voters) = voters_holding(&nodes, "cnt:two", 10).await;
    voters[0].kill();
    voters[1].kill();
    let mut sc = voters[2].redis().await;
    assert_eq!(
        incr_until(&mut sc, "cnt:two", Duration::from_secs(20)).await,
        11
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_voter_lost() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 6, &[]).await;
    let (_, voters) = voters_holding(&nodes, "cnt:all", 10).await;
    for v in &voters {
        v.kill();
    }
    let rest: Vec<&GossipNode> = nodes
        .iter()
        .filter(|n| !voters.iter().any(|v| v.id == n.id))
        .collect();
    let mut oc = rest[0].redis().await;
    assert_eq!(
        incr_or_show(&mut oc, "cnt:all", Duration::from_secs(25), &rest).await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn force_reconfigure_acts_before_dead() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, SLOW_DEATH).await;
    let (g, voters) = voters_holding(&nodes, "cnt:force", 5).await;
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
async fn the_down_switch_is_at_once() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    let mut c = nodes[0].redis().await;
    for _ in 0..7 {
        let _: i64 = incr_once(&mut c, "cnt:down").await;
    }
    assert!(all_hold(&refs(&nodes), "cnt:down", "7", Duration::from_secs(5)).await);
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
                        assert!(is_try_again(&e), "{e}");
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
                match redis::cmd("INCR").arg(key).query_async::<i64>(&mut c).await {
                    Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(e) if is_refused(&e) => 0,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_purge_removes_expired_keys_from_every_voter() {
    let (net, leases) = (Net::new(), Leases::new());
    let nodes = raft_cluster(&net, &leases, 3, &[]).await;
    purge_reaches_every_replica(&refs(&nodes)).await;
}
