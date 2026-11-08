//! Growth from one node to five and back under steady load (spec section 16, "Scaling and
//! chaos"). The load enters on the first node, which stays for the whole test. Reads see only
//! cache misses, never errors or wrong values, and the counter equals its successful increments.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::counters::{all_raft, flags, join_to, status, Leases, GROUPS};
use common::gossip::{agreed, refs, wait_until, GossipNode, Net};
use discus::counter::group_key;
use redis::AsyncCommands;
use serial_test::serial;

const MAX_NODES: usize = 5;
const RF: usize = 3;
const COUNTER: &str = "cnt:scale";
const PROBE: &str = "scale-probe";
const STEP: Duration = Duration::from_secs(30);
const PACE: Duration = Duration::from_millis(2);

#[derive(Default)]
struct Tally {
    written: Mutex<Vec<String>>,
    write_errors: AtomicU64,
    read_errors: AtomicU64,
    wrong_values: AtomicU64,
    reads: AtomicU64,
    increments: AtomicU64,
    refused: AtomicU64,
    ambiguous: AtomicU64,
}

/// The owners of `key` in the current epoch that hold no copy yet. A node that lost ownership
/// keeps its copy (spec section 5.2), so a count of copies can exceed `RF`.
fn missing(nodes: &[&GossipNode], key: &[u8]) -> Vec<String> {
    let owners = nodes[0].node.owners(key);
    assert_eq!(owners.len(), nodes.len().min(RF));
    nodes
        .iter()
        .filter(|n| owners.contains(&n.id) && n.node.store.entry(key).is_none())
        .map(|n| n.addr.to_string())
        .collect()
}

/// The role, leader, and voters of every group on every node, for a failure message.
fn groups(nodes: &[&GossipNode]) -> String {
    let short =
        |id: &discus_placement::NodeId| format!("{id:?}").chars().take(12).collect::<String>();
    let mut out = String::new();
    for g in 0..GROUPS {
        let targets = nodes[0].node.epochs().current().owners(&group_key(g), 3);
        out += &format!(
            "group {g}, targets {:?}\n",
            targets.iter().map(short).collect::<Vec<_>>()
        );
        for n in nodes {
            let s = status(n, g);
            out += &format!(
                "  {} {:>8} leader {:?} voters {:?} learners {:?}\n",
                short(&n.id),
                s.role,
                s.leader.as_ref().map(short),
                s.voters.iter().map(short).collect::<Vec<_>>(),
                s.learners.iter().map(short).collect::<Vec<_>>(),
            );
        }
    }
    out
}

/// The cluster of `nodes` agrees on one epoch, every owner of the probe key holds it, and, from
/// three nodes, every counter group runs in Raft mode with its target voters.
async fn settled(nodes: &[&GossipNode], phase: &str) {
    assert!(
        wait_until(STEP, || agreed(nodes)).await,
        "{phase}: the epoch did not agree"
    );
    assert!(
        wait_until(STEP, || missing(nodes, PROBE.as_bytes()).is_empty()).await,
        "{phase}: owners without the probe key: {:?}",
        missing(nodes, PROBE.as_bytes())
    );
    if nodes.len() >= 3 {
        assert!(
            wait_until(STEP, || all_raft(nodes)).await,
            "{phase}: the counter groups did not reach Raft mode\n{}",
            groups(nodes)
        );
    }
}

async fn write_load(
    mut c: redis::aio::MultiplexedConnection,
    t: Arc<Tally>,
    stop: Arc<AtomicBool>,
) {
    let mut i = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let key = format!("scale:{i}");
        i += 1;
        let r: redis::RedisResult<()> = c.set(&key, &key).await;
        match r {
            Ok(()) => t.written.lock().unwrap().push(key),
            Err(e) => {
                eprintln!("write error: {e}");
                t.write_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        tokio::time::sleep(PACE).await;
    }
}

/// A value is the key itself, so any other value is a wrong read, not a miss.
async fn read_load(mut c: redis::aio::MultiplexedConnection, t: Arc<Tally>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        let key = {
            let written = t.written.lock().unwrap();
            if written.is_empty() {
                None
            } else {
                Some(written[fastrand::usize(..written.len())].clone())
            }
        };
        if let Some(key) = key {
            let r: redis::RedisResult<Option<String>> = c.get(&key).await;
            t.reads.fetch_add(1, Ordering::Relaxed);
            match r {
                Ok(Some(v)) if v != key => {
                    eprintln!("wrong value for {key}: {v}");
                    t.wrong_values.fetch_add(1, Ordering::Relaxed);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("read error: {e}");
                    t.read_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        tokio::time::sleep(PACE).await;
    }
}

/// A `has no leader` refusal applied nothing. Any other error may have applied.
async fn counter_load(
    mut c: redis::aio::MultiplexedConnection,
    t: Arc<Tally>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let r: redis::RedisResult<i64> = redis::cmd("INCR").arg(COUNTER).query_async(&mut c).await;
        match r {
            Ok(_) => {
                t.increments.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) if e.to_string().contains("has no leader") => {
                t.refused.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("ambiguous increment: {e}");
                t.ambiguous.fetch_add(1, Ordering::Relaxed);
            }
        }
        tokio::time::sleep(PACE).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn growth_from_one_to_five_and_back_under_load() {
    let (net, leases) = (Net::new(), Leases::new());
    let extra = flags(&["--replication-factor", "3", "--shutdown-timeout-secs", "20"]);

    let (first, _) = join_to(&net, &leases, &[], &extra).await;
    assert!(
        wait_until(STEP, || first.is_normal()).await,
        "the first node did not form a cluster"
    );
    let mut setup = first.redis().await;
    let _: () = setup.set(PROBE, PROBE).await.unwrap();
    let mut nodes = vec![first];
    settled(&[&nodes[0]], "one node").await;

    let tally = Arc::new(Tally::default());
    let stop = Arc::new(AtomicBool::new(false));
    let load = vec![
        tokio::spawn(write_load(
            nodes[0].redis().await,
            tally.clone(),
            stop.clone(),
        )),
        tokio::spawn(read_load(
            nodes[0].redis().await,
            tally.clone(),
            stop.clone(),
        )),
        tokio::spawn(counter_load(
            nodes[0].redis().await,
            tally.clone(),
            stop.clone(),
        )),
    ];

    for n in 2..=MAX_NODES {
        let (node, _) = join_to(&net, &leases, &refs(&nodes), &extra).await;
        nodes.push(node);
        let all = refs(&nodes);
        settled(&all, &format!("grow to {n}")).await;
    }

    // Every node serves every key that the load wrote, whichever node owns it.
    let written = tally.written.lock().unwrap().clone();
    for node in &nodes {
        let mut c = node.redis().await;
        for key in written.iter().step_by(7) {
            let v: Option<String> = c.get(key).await.unwrap();
            assert_eq!(
                v.as_deref(),
                Some(key.as_str()),
                "{key} through {}",
                node.addr
            );
        }
    }

    while nodes.len() > 1 {
        let leaving = nodes.pop().unwrap();
        leaving.node.drain_token().cancel();
        let rest = refs(&nodes);
        settled(&rest, &format!("shrink to {}", nodes.len())).await;
    }

    stop.store(true, Ordering::Relaxed);
    for task in load {
        task.await.unwrap();
    }

    let t = &tally;
    let (reads, read_errors, wrong) = (
        t.reads.load(Ordering::Relaxed),
        t.read_errors.load(Ordering::Relaxed),
        t.wrong_values.load(Ordering::Relaxed),
    );
    let written = t.written.lock().unwrap().clone();
    eprintln!(
        "writes {} (errors {}), reads {reads}, increments {} (refused {}, ambiguous {})",
        written.len(),
        t.write_errors.load(Ordering::Relaxed),
        t.increments.load(Ordering::Relaxed),
        t.refused.load(Ordering::Relaxed),
        t.ambiguous.load(Ordering::Relaxed),
    );
    assert!(reads > 0 && !written.is_empty(), "the load did not run");
    assert_eq!(read_errors, 0, "reads failed during the membership changes");
    assert_eq!(wrong, 0, "reads returned values that were never written");

    let ok = t.increments.load(Ordering::Relaxed);
    let ambiguous = t.ambiguous.load(Ordering::Relaxed);
    let value: i64 = setup.get(COUNTER).await.unwrap();
    let value = u64::try_from(value).unwrap();
    assert!(
        ok <= value && value <= ok + ambiguous,
        "the counter is {value}, with {ok} successes and {ambiguous} ambiguous increments"
    );

    // Writes are acknowledged at W = 1 and copied asynchronously, so a write that lands on a node
    // as it leaves can become a miss (spec section 15.3). Anything beyond a few is a handoff bug.
    let mut lost = Vec::new();
    for (i, key) in written.iter().enumerate() {
        let v: Option<String> = setup.get(key).await.unwrap();
        if v.as_deref() != Some(key.as_str()) {
            lost.push(i);
        }
    }
    assert!(
        lost.len() <= written.len() / 1000,
        "lost {} of {} entries while the cluster shrank, at write positions {lost:?}",
        lost.len(),
        written.len()
    );
}
