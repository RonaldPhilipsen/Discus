#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::counters::{
    group_of, held, incr_until, leader_of, role, start, status, Leases, FAST, GROUPS,
};
use common::gossip::{agreed, socket, wait_until, GossipNode, Net, Opts};
use discus::counter::Refusal;
use discus_placement::mock::MockClock;
use discus_placement::{Clock, SystemClock};
use redis::AsyncCommands;

/// Every group has one leader and a synced follower among `nodes`.
fn settled(nodes: &[&GossipNode]) -> bool {
    (0..GROUPS).all(|g| {
        leader_of(nodes, g).is_some_and(|l| {
            nodes.iter().filter(|n| n.id != l.id).all(|n| {
                let s = status(n, g);
                s.role == "follower" && s.synced
            })
        })
    })
}

async fn two(
    net: &Net,
    leases: &Leases,
    extra: &[&str],
) -> (Vec<GossipNode>, Vec<Arc<AtomicBool>>) {
    let (nodes, gates) = start(net, leases, 2, extra).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    assert!(wait_until(Duration::from_secs(10), || agreed(&refs)).await);
    assert!(
        wait_until(Duration::from_secs(10), || settled(&refs)).await,
        "the groups did not settle"
    );
    (nodes, gates)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_and_a_follower_per_group() {
    let (net, leases) = (Net::new(), Leases::new());
    let (nodes, _gates) = two(&net, &leases, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    assert!(
        wait_until(Duration::from_secs(5), || (0..GROUPS).all(|g| {
            let l = leader_of(&refs, g).map(|l| l.id);
            nodes.iter().all(|n| status(n, g).leader == l)
        }))
        .await,
        "the nodes disagree on a leader"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_follower_holds_every_increment() {
    let (net, leases) = (Net::new(), Leases::new());
    let (nodes, _gates) = two(&net, &leases, &[]).await;
    let mut c = nodes[0].redis().await;
    for _ in 0..100 {
        let _: i64 = c.incr("cnt:a", 1).await.unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(3), || nodes.iter().all(|n| held(
            n, "cnt:a"
        )
        .as_deref()
            == Some("100")))
        .await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_keeps_the_value() {
    let (net, leases) = (Net::new(), Leases::new());
    let (nodes, _gates) = two(&net, &leases, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let g = group_of(&nodes[0], "cnt:a");
    let mut c = nodes[0].redis().await;
    for _ in 0..100 {
        let _: i64 = c.incr("cnt:a", 1).await.unwrap();
    }
    let leader = leader_of(&refs, g).unwrap();
    let survivor = nodes.iter().find(|n| n.id != leader.id).unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || held(survivor, "cnt:a")
            .as_deref()
            == Some("100"))
        .await
    );
    leader.kill();
    let mut c = survivor.redis().await;
    let n = incr_until(&mut c, "cnt:a", Duration::from_secs(8)).await;
    assert_eq!(n, 101);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lagging_follower_gets_a_snapshot() {
    let (net, leases) = (Net::new(), Leases::new());
    let (nodes, _gates) = two(&net, &leases, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let g = group_of(&nodes[0], "cnt:lag");
    let leader = leader_of(&refs, g).unwrap();
    let follower = nodes.iter().find(|n| n.id != leader.id).unwrap();
    follower
        .node
        .faults
        .stall_counter_ms
        .store(300, Ordering::Relaxed);
    let mut c = leader.redis().await;
    for i in 1..=20 {
        let started = Instant::now();
        let n: i64 = c.incr("cnt:lag", 1).await.unwrap();
        assert_eq!(n, i);
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "a write waited for the follower"
        );
    }
    follower
        .node
        .faults
        .stall_counter_ms
        .store(0, Ordering::Relaxed);
    assert!(
        wait_until(Duration::from_secs(3), || held(follower, "cnt:lag")
            .as_deref()
            == Some("20"))
        .await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_stops_at_the_lease_margin() {
    let (net, leases) = (Net::new(), Leases::new());
    let (nodes, gates) = two(&net, &leases, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    let g = group_of(&nodes[0], "cnt:fence");
    let mut c = nodes[0].redis().await;
    let _: i64 = c.incr("cnt:fence", 1).await.unwrap();
    let leader = leader_of(&refs, g).unwrap();
    let li = nodes.iter().position(|n| n.id == leader.id).unwrap();
    let follower = &nodes[1 - li];
    gates[li].store(false, Ordering::Relaxed);
    // A renewal in flight can still land, so read the lease end once it cannot move any more.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let until = status(leader, g).lease_until.unwrap();
    let stop = until - Duration::from_millis(500);
    let counters = leader.node.counters().unwrap();
    let cmd: Vec<Bytes> = vec![
        Bytes::from_static(b"INCR"),
        Bytes::from_static(b"cnt:fence"),
    ];
    let early = stop.saturating_duration_since(Instant::now());
    if early > Duration::from_millis(100) {
        assert!(
            counters.execute(g, &cmd).await.is_ok(),
            "stopped before the margin"
        );
    }
    tokio::time::sleep_until((stop + Duration::from_millis(50)).into()).await;
    assert_eq!(counters.execute(g, &cmd).await, Err(Refusal::NotLeader));
    assert!(
        wait_until(Duration::from_secs(8), || role(follower, g) == "leader").await,
        "the follower never took over"
    );
    let since = status(follower, g).since.unwrap();
    assert!(since >= until, "two leaders overlapped");
    let mut c = follower.redis().await;
    let n = incr_until(&mut c, "cnt:fence", Duration::from_secs(3)).await;
    assert!(n >= 2, "{n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_follower_acquires_first() {
    let (net, leases) = (Net::new(), Leases::new());
    let (nodes, _gates) = start(&net, &leases, 3, &[]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    assert!(wait_until(Duration::from_secs(10), || agreed(&refs)).await);
    let g = group_of(&nodes[0], "cnt:first");
    assert!(
        wait_until(Duration::from_secs(10), || leader_of(&refs, g)
            .is_some_and(|l| status(l, g).follower.is_some()))
        .await
    );
    let leader = leader_of(&refs, g).unwrap();
    let follower = status(leader, g).follower.unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || nodes
            .iter()
            .any(|n| n.id == follower && status(n, g).synced))
        .await
    );
    leader.kill();
    let rest: Vec<&GossipNode> = nodes.iter().filter(|n| n.id != leader.id).collect();
    assert!(
        wait_until(Duration::from_secs(10), || leader_of(&rest, g).is_some()).await,
        "no new leader"
    );
    assert_eq!(leader_of(&rest, g).unwrap().id, follower);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_epoch_names_the_leader_without_a_lease() {
    let net = Net::new();
    let nodes = net.start_all(2, &["--counter-groups", "4"]).await;
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    assert!(wait_until(Duration::from_secs(10), || agreed(&refs)).await);
    assert!(wait_until(Duration::from_secs(10), || settled(&refs)).await);
    let g = group_of(&nodes[0], "cnt:e");
    let mut c = nodes[0].redis().await;
    for _ in 0..10 {
        let _: i64 = c.incr("cnt:e", 1).await.unwrap();
    }
    let leader = leader_of(&refs, g).unwrap();
    let survivor = nodes.iter().find(|n| n.id != leader.id).unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || held(survivor, "cnt:e")
            .as_deref()
            == Some("10"))
        .await
    );
    leader.kill();
    let mut c = survivor.redis().await;
    assert_eq!(
        incr_until(&mut c, "cnt:e", Duration::from_secs(8)).await,
        11
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_is_identical_under_clock_skew() {
    const SKEW: u64 = 400;
    let (a, b) = (MockClock::<20>, MockClock::<21>);
    let real_ms = || SystemClock.now_ms();
    a.set(real_ms());
    b.set(real_ms() + SKEW);
    let ticking = tokio::spawn(async move {
        loop {
            a.set(real_ms());
            b.set(real_ms() + SKEW);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });
    let (net, leases) = (Net::new(), Leases::new());
    let (sa, aa) = socket();
    let (sb, ab) = socket();
    let gate = Arc::new(AtomicBool::new(true));
    let na = net
        .start_on(
            sa,
            &[ab],
            &[],
            Opts {
                clocks: Some((a.hlc_clock(), Arc::new(a))),
                counter_leases: Some(leases.for_node(gate.clone())),
                defaults: FAST,
                ..Opts::default()
            },
        )
        .await;
    let nb = net
        .start_on(
            sb,
            &[aa],
            &[],
            Opts {
                clocks: Some((b.hlc_clock(), Arc::new(b))),
                counter_leases: Some(leases.for_node(gate)),
                defaults: FAST,
                ..Opts::default()
            },
        )
        .await;
    let refs = [&na, &nb];
    assert!(wait_until(Duration::from_secs(10), || agreed(&refs)).await);
    assert!(wait_until(Duration::from_secs(10), || settled(&refs)).await);
    for (i, entry) in refs.iter().enumerate() {
        let key = format!("cnt:r{i}");
        let g = group_of(&na, &key);
        let leader = leader_of(&refs, g).unwrap();
        let leader_ms = if leader.id == na.id {
            real_ms()
        } else {
            real_ms() + SKEW
        };
        let mut c = entry.redis().await;
        let _: i64 = redis::cmd("INCR")
            .arg(&key)
            .arg("PX")
            .arg(1000)
            .query_async(&mut c)
            .await
            .unwrap();
        let deadline = |n: &GossipNode| {
            let c = n.node.counters().unwrap();
            c.held_state(g)
                .and_then(|s| s.get(key.as_bytes()).and_then(|e| e.expires_at_ms))
        };
        assert!(
            wait_until(Duration::from_secs(3), || deadline(&na).is_some()
                && deadline(&na) == deadline(&nb))
            .await,
            "the replicas disagree on the deadline"
        );
        let at = deadline(&na).unwrap();
        assert!(
            at.abs_diff(leader_ms + 1000) < 150,
            "the deadline {at} does not follow the leader clock {leader_ms}"
        );
    }
    ticking.abort();
}
