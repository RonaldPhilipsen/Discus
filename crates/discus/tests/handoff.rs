#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::gossip::{fill, hits, refs, wait_agreed, wait_until, GossipNode, Net};
use redis::AsyncCommands;

const FORM: Duration = Duration::from_secs(10);

async fn started(n: &GossipNode) -> bool {
    common::wait_status(n.admin, "/startupz", 200, Duration::from_secs(10)).await
}

#[tokio::test]
async fn a_joining_node_receives_its_keys() {
    let net = Net::new();
    let nodes = net.start_agreed(2, &[]).await;
    let keys = fill(&nodes[0], "join", 300).await;
    let joiner = net.start(&[nodes[0].cluster_addr], &[]).await;
    assert!(started(&joiner).await);
    let epoch = joiner.node.epochs().current().clone();
    let mine: Vec<&String> = keys
        .iter()
        .filter(|k| epoch.primary(k.as_bytes()) == Some(joiner.id))
        .collect();
    assert!(!mine.is_empty());
    for k in mine {
        assert!(
            joiner.node.store.read(k.as_bytes(), |e| e.is_some()),
            "{k} missing"
        );
    }
}

#[tokio::test]
async fn a_death_refills_the_new_replica() {
    let net = Net::new();
    let nodes = net.start_agreed(4, &["--replication-factor", "2"]).await;
    let keys = fill(&nodes[0], "death", 200).await;
    nodes[3].kill();
    let rest = refs(&nodes[..3]);
    wait_agreed(&rest, FORM).await;
    assert!(
        wait_until(Duration::from_secs(10), || keys.iter().all(|k| {
            rest.iter()
                .filter(|n| n.node.store.read(k.as_bytes(), |e| e.is_some()))
                .count()
                == 2
        }))
        .await
    );
}

#[tokio::test]
async fn a_drain_hands_off_before_departing() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &["--shutdown-timeout-secs", "5"]).await;
    let keys = fill(&nodes[0], "drain", 300).await;
    nodes[2].shutdown.cancel();
    let rest = refs(&nodes[..2]);
    wait_agreed(&rest, FORM).await;
    assert_eq!(hits(&nodes[0], &keys).await, keys.len());
}

/// A scan holds while the joins arrive. Every epoch that arrives during the scan waits, and one
/// more scan covers them all.
#[tokio::test]
async fn epochs_during_a_scan_wait_for_one_more_scan() {
    let net = Net::new();
    let nodes = net.start_agreed(2, &[]).await;
    fill(&nodes[0], "scan", 2000).await;
    let rep = nodes[0].node.replicator().unwrap();
    assert!(wait_until(Duration::from_secs(10), || !rep.scanning()).await);
    let before = rep.scans();
    let faults = &nodes[0].node.faults;
    faults.hold_scan.store(true, Ordering::Relaxed);
    let mut joined = vec![net.start(&[nodes[0].cluster_addr], &[]).await];
    let held = wait_until(Duration::from_secs(20), || {
        faults.scan_held.load(Ordering::Relaxed)
    });
    assert!(held.await, "the first join started no scan");
    for i in 1..3 {
        joined.push(net.start(&[nodes[i % 2].cluster_addr], &[]).await);
    }
    let all: Vec<&GossipNode> = nodes.iter().chain(joined.iter()).collect();
    wait_agreed(&all, Duration::from_secs(20)).await;
    assert!(rep.scanning());
    assert_eq!(rep.scans(), before);

    faults.hold_scan.store(false, Ordering::Relaxed);
    assert!(wait_until(Duration::from_secs(60), || !rep.scanning()).await);
    assert_eq!(rep.scans() - before, 2);
}

/// Two nodes with `RF = 1`, a few keys, and a joiner whose inbound handoff hangs.
async fn stalled_join(net: &Net, extra: &[&str]) -> (Vec<GossipNode>, GossipNode, Vec<String>) {
    let nodes = net.start_agreed(2, extra).await;
    let keys = fill(&nodes[0], "fb", 100).await;
    let mut c = nodes[0].redis().await;
    for i in 0..100 {
        let _: () = c.set(format!("cnt{i}"), 5).await.unwrap();
    }
    let socket = common::gossip::socket().0;
    let joiner = net
        .start_on(socket, &[nodes[0].cluster_addr], extra, Default::default())
        .await;
    joiner
        .node
        .faults
        .stall_bulk_ms
        .store(60_000, Ordering::Relaxed);
    let all: Vec<&GossipNode> = nodes.iter().chain([&joiner]).collect();
    wait_agreed(&all, FORM).await;
    (nodes, joiner, keys)
}

#[tokio::test]
async fn fallback_serves_a_miss_until_the_handoff_completes() {
    let net = Net::new();
    let (_nodes, joiner, keys) = stalled_join(&net, &[]).await;
    let rep = joiner.node.replicator().unwrap();
    assert!(rep.handoff_pending());
    let epoch = joiner.node.epochs().current().clone();
    let moved: Vec<&String> = keys
        .iter()
        .filter(|k| epoch.primary(k.as_bytes()) == Some(joiner.id))
        .collect();
    assert!(!moved.is_empty());
    let mut c = joiner.redis().await;
    for k in &moved {
        let v: Option<String> = c.get(*k).await.unwrap();
        assert_eq!(v, Some(format!("v-{k}")));
    }
    let counter = (0..100)
        .map(|i| format!("cnt{i}"))
        .find(|k| epoch.primary(k.as_bytes()) == Some(joiner.id))
        .unwrap();
    let n: i64 = c.incr(&counter, 1).await.unwrap();
    assert_eq!(n, 6);
}

#[tokio::test]
async fn fallback_ends_when_the_handoff_is_abandoned() {
    let net = Net::new();
    let (_nodes, joiner, keys) = stalled_join(&net, &["--handoff-timeout-ms", "500"]).await;
    let rep = joiner.node.replicator().unwrap();
    assert!(wait_until(Duration::from_secs(5), || !rep.handoff_pending()).await);
    let epoch = joiner.node.epochs().current().clone();
    let moved = keys
        .iter()
        .find(|k| epoch.primary(k.as_bytes()) == Some(joiner.id))
        .unwrap();
    let mut c = joiner.redis().await;
    let v: Option<String> = c.get(moved).await.unwrap();
    assert_eq!(v, None);
}

#[tokio::test]
async fn a_handoff_ends_when_its_sources_give_up() {
    let net = Net::new();
    let (_nodes, joiner, _) = stalled_join(&net, &["--replication-timeout-ms", "200"]).await;
    let rep = joiner.node.replicator().unwrap();
    assert!(rep.handoff_pending());
    assert!(wait_until(Duration::from_secs(15), || !rep.handoff_pending()).await);
}

#[tokio::test]
async fn a_rolling_restart_keeps_the_data_and_a_short_history() {
    let net = Net::new();
    let args = ["--replication-factor", "2", "--shutdown-timeout-secs", "5"];
    let mut nodes = net.start_agreed(3, &args).await;
    let keys = fill(&nodes[0], "roll", 300).await;
    for _ in 0..3 {
        let seed = nodes.last().unwrap().cluster_addr;
        let fresh = net.start(&[seed], &args).await;
        assert!(started(&fresh).await);
        let old = nodes.remove(0);
        old.shutdown.cancel();
        nodes.push(fresh);
        wait_agreed(&refs(&nodes), Duration::from_secs(15)).await;
    }
    let hit = hits(&nodes[0], &keys).await;
    assert!(hit * 100 >= keys.len() * 99, "{hit} of {} hit", keys.len());
    for n in &nodes {
        let rep = n.node.replicator().unwrap();
        assert!(
            wait_until(Duration::from_secs(10), || !rep.handoff_pending()
                && !rep.scanning())
            .await
        );
        assert!(
            n.node.epochs().ids().len() <= 5,
            "{:?}",
            n.node.epochs().ids()
        );
    }
}

#[tokio::test]
async fn handoff_skips_unchanged_ranges() {
    let net = Net::new();
    let extra = ["--replication-factor", "1"];
    let nodes = net.start_agreed(2, &extra).await;
    let keys = fill(&nodes[0], "skip", 400).await;
    let old = nodes[0].node.epochs().current().clone();
    let joiner = net.start(&[nodes[0].cluster_addr], &extra).await;
    assert!(started(&joiner).await);
    let new = joiner.node.epochs().current().clone();
    let changed = old.changed_ranges(&new, 1);
    let expected: Vec<&String> = keys
        .iter()
        .filter(|k| {
            let range = discus_placement::range_of(discus_placement::slot(k.as_bytes()));
            changed.contains(range) && new.owners(k.as_bytes(), 1).contains(&joiner.id)
        })
        .collect();
    assert!(!expected.is_empty());
    assert!(
        wait_until(FORM, || expected
            .iter()
            .all(|k| joiner.node.store.read(k.as_bytes(), |e| e.is_some())))
        .await
    );
    assert!(wait_until(FORM, || !nodes[0].node.replicator().unwrap().scanning()).await);
    let moved: Vec<&String> = keys
        .iter()
        .filter(|k| joiner.node.store.read(k.as_bytes(), |e| e.is_some()))
        .collect();
    assert_eq!(moved, expected);
}
