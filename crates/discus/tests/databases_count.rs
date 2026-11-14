#![allow(clippy::unwrap_used)]

//! Raising and lowering the database count of a running gossip cluster. The cluster count is the
//! smallest count of the NORMAL members. A lower count applies only after it has held for the
//! dwell time, so every wait on a lower count is a condition wait with a long limit.

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::gossip::{fill, hits, refs, start_each, wait_agreed, wait_until, GossipNode, Net};
use redis::AsyncCommands;

const LONG: Duration = Duration::from_secs(30);
const OUT_OF_RANGE: &str = "DB index is out of range";

/// A rename replaces the file in one step, so the node never reads a half-written file.
fn write_settings(path: &Path, databases: u32) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, format!("version = 1\ndatabases = {databases}\n")).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

/// Three nodes that agree, each with its own settings file at `databases = 1`.
async fn cluster_with_files(net: &Net) -> (Vec<GossipNode>, Vec<PathBuf>, Vec<tempfile::TempDir>) {
    let mut files = Vec::new();
    let mut dirs = Vec::new();
    let nodes = start_each(3, async |socket, seeds| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("discus.toml");
        write_settings(&path, 1);
        let node = net
            .start_on(
                socket,
                seeds,
                &[
                    "--config",
                    path.to_str().unwrap(),
                    "--config-reload-secs",
                    "1",
                ],
                common::gossip::Opts::default(),
            )
            .await;
        files.push(path);
        dirs.push(dir);
        node
    })
    .await;
    wait_agreed(&refs(&nodes), LONG).await;
    (nodes, files, dirs)
}

async fn select(n: &GossipNode, db: i64) -> redis::RedisResult<()> {
    let mut c = n.redis().await;
    redis::cmd("SELECT").arg(db).query_async(&mut c).await
}

async fn selects_out_of_range(n: &GossipNode, db: i64) -> bool {
    select(n, db)
        .await
        .is_err_and(|e| e.to_string().contains(OUT_OF_RANGE))
}

/// Poll until `SELECT db` works on every node of `nodes`, or `LONG` passes.
async fn wait_selectable(nodes: &[&GossipNode], db: i64) -> bool {
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        let mut all = true;
        for n in nodes {
            all &= select(n, db).await.is_ok();
        }
        if all {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_out_of_range(nodes: &[&GossipNode], db: i64) -> bool {
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        let mut all = true;
        for n in nodes {
            all &= selects_out_of_range(n, db).await;
        }
        if all {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// True if `SELECT db` stays out of range on every node of `nodes` for all of `window`. A single
/// poll can pass before the change under test reaches a node, so this keeps asking.
async fn stays_out_of_range(nodes: &[&GossipNode], db: i64, window: Duration) -> bool {
    let end = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < end {
        for n in nodes {
            if !selects_out_of_range(n, db).await {
                return false;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    true
}

/// Every node of `nodes` has the record of each node of `raised` with `databases` count `n`, and
/// each node of `raised` advertises it.
fn advertised(nodes: &[GossipNode], raised: &[&GossipNode], n: u32) -> bool {
    raised.iter().all(|r| {
        let id = r.node.id;
        r.node
            .membership()
            .is_some_and(|m| m.local().databases == n)
            && nodes.iter().filter(|o| o.node.id != id).all(|o| {
                o.node.membership().is_some_and(|m| {
                    m.members()
                        .iter()
                        .any(|(info, _)| info.id == id && info.databases == n)
                })
            })
    })
}

async fn wait_idle(nodes: &[&GossipNode]) {
    let idle = wait_until(LONG, || {
        nodes.iter().all(|n| {
            n.node
                .replicator()
                .is_none_or(|r| !r.handoff_pending() && !r.scanning())
        })
    })
    .await;
    assert!(idle, "a handoff did not finish");
}

/// Raise the count of the three nodes of [`cluster_with_files`] to 3 and wait for every node.
async fn raise_all(nodes: &[GossipNode], files: &[PathBuf]) {
    for f in files {
        write_settings(f, 3);
    }
    assert!(
        wait_until(LONG, || nodes
            .iter()
            .all(|n| n.node.databases.cluster() == 3))
        .await,
        "the cluster did not raise its count to 3"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raise_in_the_file_reaches_every_node_without_a_restart() {
    let net = Net::new();
    let (nodes, files, _dirs) = cluster_with_files(&net).await;
    let started: Vec<_> = nodes.iter().map(|n| n.node.started_at).collect();

    write_settings(&files[0], 3);
    write_settings(&files[1], 3);
    assert!(
        wait_until(LONG, || nodes[..2]
            .iter()
            .all(|n| n.node.databases.local() == 3))
        .await,
        "nodes A and B did not raise their count"
    );
    assert!(
        wait_until(LONG, || advertised(&nodes, &[&nodes[0], &nodes[1]], 3)).await,
        "A and B did not advertise the raised count"
    );
    assert!(
        stays_out_of_range(&refs(&nodes), 2, Duration::from_secs(3)).await,
        "SELECT 2 opened while C is at 1"
    );

    write_settings(&files[2], 3);
    assert!(
        wait_selectable(&refs(&nodes), 2).await,
        "SELECT 2 did not open on every node"
    );
    for (n, at) in nodes.iter().zip(started) {
        assert_eq!(n.node.started_at, at, "a node restarted");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_the_old_flag_keeps_the_raised_count() {
    let net = Net::new();
    let (mut nodes, files, _dirs) = cluster_with_files(&net).await;
    raise_all(&nodes, &files).await;
    assert!(wait_selectable(&refs(&nodes), 2).await);

    let old = nodes.remove(2);
    old.shutdown.cancel();
    wait_agreed(&refs(&nodes), LONG).await;
    let seeds: Vec<_> = nodes.iter().map(|n| n.cluster_addr).collect();
    let fresh = net.start(&seeds, &["--databases", "1"]).await;
    assert!(
        wait_until(LONG, || fresh.is_normal()).await,
        "the restarted node did not become NORMAL"
    );
    assert_eq!(fresh.node.databases.local(), 3);
    nodes.push(fresh);
    wait_agreed(&refs(&nodes), LONG).await;
    assert!(wait_selectable(&refs(&nodes), 2).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolling_replacement_from_one_to_three_keeps_database_zero() {
    let net = Net::new();
    let mut nodes = net.start_agreed(3, &[]).await;
    let keys = fill(&nodes[0], "roll", 100).await;

    for _ in 0..3 {
        let seeds: Vec<_> = nodes.iter().map(|n| n.cluster_addr).collect();
        let fresh = net.start(&seeds, &["--databases", "3"]).await;
        nodes.push(fresh);
        // The fresh node holds 3, but the old nodes hold 1, so the cluster count is 1. A write to
        // database 2 on the fresh node would be lost.
        assert!(
            stays_out_of_range(&refs(&nodes), 2, Duration::from_secs(3)).await,
            "SELECT 2 opened while the fresh node joined"
        );
        wait_agreed(&refs(&nodes), LONG).await;
        wait_idle(&refs(&nodes)).await;
        assert!(
            stays_out_of_range(&refs(&nodes), 2, Duration::from_secs(3)).await,
            "SELECT 2 opened while an old node is a member"
        );
        let old = nodes.remove(0);
        old.shutdown.cancel();
        wait_agreed(&refs(&nodes), LONG).await;
        wait_idle(&refs(&nodes)).await;
        if nodes.iter().any(|n| n.node.databases.local() == 1) {
            assert!(
                stays_out_of_range(&refs(&nodes), 2, Duration::from_secs(3)).await,
                "SELECT 2 opened while an old node is a member"
            );
        }
    }

    let mut got = 0;
    let deadline = tokio::time::Instant::now() + LONG;
    while got < keys.len() && tokio::time::Instant::now() < deadline {
        got = hits(&nodes[0], &keys).await;
        if got < keys.len() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    assert_eq!(got, keys.len(), "keys of database 0 were lost");
    assert!(
        wait_selectable(&refs(&nodes), 2).await,
        "SELECT 2 stayed closed after the last old node left"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_from_three_to_one_refuses_select_at_once() {
    let net = Net::new();
    let mut nodes = net.start_agreed(3, &["--databases", "3"]).await;
    let keys = fill(&nodes[0], "zero", 50).await;
    let mut selected = nodes[0].redis().await;
    redis::cmd("SELECT")
        .arg(2)
        .query_async::<()>(&mut selected)
        .await
        .unwrap();
    for i in 0..50 {
        let _: () = selected.set(format!("two{i}"), "v").await.unwrap();
    }
    assert!(nodes.iter().map(|n| n.node.store.db_len(2)).sum::<usize>() > 0);

    let seeds: Vec<_> = nodes.iter().map(|n| n.cluster_addr).collect();
    let lowered = net
        .start(&seeds, &["--databases", "1", "--lower-databases"])
        .await;
    assert!(
        wait_until(LONG, || lowered.is_normal()).await,
        "the lowering node did not become NORMAL"
    );
    nodes.push(lowered);
    wait_agreed(&refs(&nodes), LONG).await;

    assert!(
        wait_out_of_range(&refs(&nodes), 1).await,
        "SELECT 1 stayed open on a node"
    );
    let err = selected.get::<_, Option<String>>("two0").await.unwrap_err();
    assert!(err.to_string().contains(OUT_OF_RANGE), "{err}");
    redis::cmd("SELECT")
        .arg(0)
        .query_async::<()>(&mut selected)
        .await
        .unwrap();

    assert!(
        wait_until(LONG, || nodes.iter().all(|n| n.node.store.db_len(2) == 0)).await,
        "a node still holds keys of database 2"
    );
    let mut got = 0;
    let deadline = tokio::time::Instant::now() + LONG;
    while got < keys.len() && tokio::time::Instant::now() < deadline {
        got = hits(&nodes[0], &keys).await;
        if got < keys.len() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    assert_eq!(got, keys.len(), "keys of database 0 were lost");
}
