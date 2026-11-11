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

/// After a join, the old owners drop the keys they handed off, so each node holds only its own
/// keys and `DBSIZE` counts every key once.
#[tokio::test]
async fn old_owners_drop_the_keys_they_handed_off() {
    let net = Net::new();
    let extra = ["--replication-factor", "3"];
    let nodes = net.start_agreed(3, &extra).await;
    let keys = fill(&nodes[0], "drop", 400).await;
    let joiner = net.start(&[nodes[0].cluster_addr], &extra).await;
    assert!(started(&joiner).await);
    let all: Vec<&GossipNode> = nodes.iter().chain(std::iter::once(&joiner)).collect();
    wait_agreed(&all, FORM).await;
    let only_owned = || {
        all.iter().all(|n| {
            let epoch = n.node.epochs().current().clone();
            n.node.replicator().is_some_and(|r| !r.scanning())
                && keys.iter().all(|k| {
                    let owns = epoch.owners(k.as_bytes(), 3).contains(&n.id);
                    n.node.store.read(k.as_bytes(), |e| e.is_some()) == owns
                })
        })
    };
    assert!(wait_until(Duration::from_secs(60), only_owned).await);
    let mut c = nodes[0].redis().await;
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert_eq!(size, keys.len() as i64);
    assert_eq!(hits(&joiner, &keys).await, keys.len());
}

/// A write that lands on an old owner after its scan passed the key, from a command that routed
/// before the adoption, reaches the current owners before the old owner drops the key.
#[tokio::test]
async fn a_write_behind_the_scan_reaches_the_owners_before_the_drop() {
    let net = Net::new();
    let extra = ["--replication-factor", "3"];
    let nodes = net.start_agreed(3, &extra).await;
    let keys = fill(&nodes[0], "late", 400).await;
    let old = &nodes[0];
    let everywhere = || {
        nodes.iter().all(|n| {
            keys.iter()
                .all(|k| n.node.store.read(k.as_bytes(), |e| e.is_some()))
        })
    };
    assert!(wait_until(FORM, everywhere).await);
    let rep = old.node.replicator().unwrap();
    assert!(wait_until(FORM, || !rep.scanning()).await);
    // Stamped before the scan starts, so the version is below the scan watermark.
    let stamp = old.node.stamp().unwrap();
    old.node.faults.hold_drop.store(true, Ordering::Relaxed);
    let joiner = net.start(&[old.cluster_addr], &extra).await;
    assert!(started(&joiner).await);
    let held = || old.node.faults.drop_held.load(Ordering::Relaxed);
    assert!(wait_until(Duration::from_secs(60), held).await);

    let epoch = old.node.epochs().current().clone();
    let key = keys
        .iter()
        .find(|k| !epoch.owners(k.as_bytes(), 3).contains(&old.id))
        .unwrap();
    let mut late = discus_store::Entry::new(discus_store::Value::Str("late".into()), None);
    late.version = stamp;
    assert!(old.node.store.apply(key.as_bytes(), late));

    old.node.faults.hold_drop.store(false, Ordering::Relaxed);
    assert!(wait_until(Duration::from_secs(60), || !rep.scanning()).await);
    let all: Vec<&GossipNode> = nodes.iter().chain(std::iter::once(&joiner)).collect();
    for n in all {
        let value = n.node.store.entry(key.as_bytes()).map(|e| e.value);
        if epoch.owners(key.as_bytes(), 3).contains(&n.id) {
            assert_eq!(
                value,
                Some(discus_store::Value::Str("late".into())),
                "{}",
                n.id
            );
        } else {
            assert_eq!(value, None, "{}", n.id);
        }
    }
}

/// A write acknowledged on an old owner while the epoch changes reaches the current owners, also
/// when the deferred epoch work runs after the write queued the key.
#[tokio::test]
async fn a_write_queued_during_the_epoch_change_reaches_the_new_owners() {
    let net = Net::new();
    let extra = ["--replication-factor", "3"];
    let nodes = net.start_agreed(3, &extra).await;
    let keys = fill(&nodes[0], "queued", 200).await;
    let joiner = net.start(&[nodes[0].cluster_addr], &extra).await;
    assert!(started(&joiner).await);
    let all: Vec<&GossipNode> = nodes.iter().chain(std::iter::once(&joiner)).collect();
    wait_agreed(&all, FORM).await;
    let old = &nodes[0];
    let rep = old.node.replicator().unwrap();
    assert!(wait_until(Duration::from_secs(60), || !rep.scanning()).await);
    let (prev, new) = {
        let epochs = old.node.epochs();
        (epochs.previous().unwrap().clone(), epochs.current().clone())
    };
    let key = keys
        .iter()
        .find(|k| {
            let owners = new.owners(k.as_bytes(), 3);
            !owners.contains(&old.id) && owners.contains(&joiner.id)
        })
        .unwrap();
    let key = bytes::Bytes::from(key.clone());

    old.node
        .faults
        .hold_replication
        .store(true, Ordering::Relaxed);
    let value = discus_store::Value::Str("queued".into());
    old.node.write(&key, old.node.stamp().unwrap(), |slot| {
        slot.set(Some(discus_store::Entry::new(value.clone(), None)));
    });
    // The deferred work of the adoption runs after the write queued the key.
    rep.on_epoch(&prev, &new, &new);
    old.node
        .faults
        .hold_replication
        .store(false, Ordering::Relaxed);

    assert!(wait_until(FORM, || rep.settled(&key)).await);
    for n in &all {
        if new.owners(&key, 3).contains(&n.id) {
            let got = n.node.store.entry(&key).map(|e| e.value);
            assert_eq!(got, Some(value.clone()), "{}", n.id);
        }
    }
}
