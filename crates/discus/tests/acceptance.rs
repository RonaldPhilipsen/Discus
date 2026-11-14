#![allow(clippy::unwrap_used)]

//! The acceptance criteria of spec section 16: a node loss causes no read errors, only misses,
//! and a join causes no errors and a smaller hit-rate dip than a cold start.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::gossip::{fill, hits, refs, wait_agreed, wait_until, Net};
use redis::AsyncCommands;

const FORM: Duration = Duration::from_secs(10);

#[tokio::test]
async fn a_node_loss_causes_no_read_errors() {
    let net = Net::new();
    let nodes = net.start_agreed(4, &["--replication-factor", "3"]).await;
    let keys = Arc::new(fill(&nodes[0], "acc", 300).await);
    let owners = |k: &String| nodes[0].node.owners(k.as_bytes()).len();
    assert!(
        wait_until(Duration::from_secs(5), || keys.iter().all(|k| {
            nodes
                .iter()
                .filter(|n| n.node.store.read(0, k.as_bytes(), |e| e.is_some()))
                .count()
                == owners(k)
        }))
        .await
    );
    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::new();
    for n in &nodes[..2] {
        let mut c = n.redis().await;
        let (keys, stop, errors) = (keys.clone(), stop.clone(), errors.clone());
        readers.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                for k in keys.iter() {
                    let r: redis::RedisResult<Option<String>> = c.get(k).await;
                    if let Err(e) = r {
                        eprintln!("read error: {e}");
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    nodes[3].kill();
    let rest = refs(&nodes[..3]);
    wait_agreed(&rest, FORM).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.await.unwrap();
    }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
    for n in &nodes[..3] {
        assert_eq!(hits(n, &keys).await, keys.len());
    }
}

/// The hits over `keys` once a third node joined a cluster of two with `RF = 1`.
async fn hits_after_a_join(extra: &[&str]) -> usize {
    let net = Net::new();
    let nodes = net.start_agreed(2, extra).await;
    let keys = fill(&nodes[0], "acc", 400).await;
    let joiner = net.start(&[nodes[0].cluster_addr], extra).await;
    let mut all = refs(&nodes);
    all.push(&joiner);
    wait_agreed(&all, FORM).await;
    let ten = Duration::from_secs(10);
    assert!(common::wait_status(joiner.admin, "/startupz", 200, ten).await);
    hits(&nodes[0], &keys).await
}

#[tokio::test]
async fn a_join_dips_less_than_a_cold_start() {
    let warm = hits_after_a_join(&[]).await;
    let cold = hits_after_a_join(&["--handoff", "false"]).await;
    assert!(warm * 100 >= 400 * 99, "{warm} of 400 hit with handoff");
    assert!(warm > cold, "{warm} with handoff, {cold} without");
}
