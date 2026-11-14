#![allow(clippy::unwrap_used)]

//! Rolling upgrades with numbered databases. A node with `--test-without-multi-db` stands in for
//! a build without them.

mod common;

use std::time::Duration;

use common::gossip::{wait_agreed, wait_until, GossipNode, Net};
use discus_placement::MemberState;

const DB3: &[&str] = &["--databases", "3"];
const OLD3: &[&str] = &["--databases", "3", "--test-without-multi-db"];
const LONG: Duration = Duration::from_secs(30);
const NEEDS_UPGRADE: &str = "multiple databases need every node to support them";

async fn select(n: &GossipNode, db: i64) -> redis::RedisResult<()> {
    let mut c = n.redis().await;
    redis::cmd("SELECT").arg(db).query_async(&mut c).await
}

/// Poll `SELECT 1` on `n` until it succeeds or `LONG` passes.
async fn select_1_opens(n: &GossipNode) -> bool {
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        if select(n, 1).await.is_ok() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn joining(n: &GossipNode) -> bool {
    n.node.membership().unwrap().local().state == MemberState::Joining
}

/// A first node alone, then `more` nodes seeded with it.
async fn grow(net: &Net, first: &[&str], more: usize, flags: &[&str]) -> Vec<GossipNode> {
    let seed = net.start(&[], first).await;
    assert!(wait_until(LONG, || seed.is_normal()).await);
    let addr = seed.cluster_addr;
    let mut nodes = vec![seed];
    for _ in 0..more {
        nodes.push(net.start(&[addr], flags).await);
    }
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    wait_agreed(&refs, LONG).await;
    nodes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_without_multi_db_closes_the_gate() {
    let net = Net::new();
    let nodes = grow(&net, OLD3, 2, DB3).await;
    assert!(
        wait_until(LONG, || nodes.iter().all(|n| {
            n.node.databases.cluster() == 3 && !n.node.databases.multi_db()
        }))
        .await,
        "the gate did not close on every node"
    );
    for n in &nodes {
        let err = select(n, 1).await.unwrap_err();
        assert!(err.to_string().contains(NEEDS_UPGRADE), "{err}");
        select(n, 0).await.unwrap();
    }

    nodes[0].kill();
    let addr = nodes[1].cluster_addr;
    wait_agreed(&[&nodes[1], &nodes[2]], LONG).await;
    let joiner = net.start(&[addr], DB3).await;
    let rest = [&nodes[1], &nodes[2], &joiner];
    wait_agreed(&rest, LONG).await;
    for n in rest {
        assert!(
            select_1_opens(n).await,
            "SELECT 1 stayed closed on {}",
            n.id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_without_multi_db_cannot_join_more_databases() {
    let net = Net::new();
    let nodes = net.start_agreed(3, DB3).await;
    let old = net.start(&[nodes[0].cluster_addr], OLD3).await;
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < until {
        assert!(joining(&old), "the node without MULTI_DB left JOINING");
        assert!(nodes.iter().all(GossipNode::is_normal));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for n in &nodes {
        assert!(n.node.epochs().current().member(old.id).is_none());
    }

    let net = Net::new();
    let nodes = net.start_agreed(3, &[]).await;
    let old = net
        .start(&[nodes[0].cluster_addr], &["--test-without-multi-db"])
        .await;
    let all = [&nodes[0], &nodes[1], &nodes[2], &old];
    wait_agreed(&all, LONG).await;
}
