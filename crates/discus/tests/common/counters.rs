//! Helpers for counter tests: shared group leases, a lease that one node can lose, and waits on
//! the role of each group.
//!
//! The multi-node tests of one binary run one at a time (a test group in
//! `.config/nextest.toml`). Many clusters at once starve the Raft timers on a busy machine, and
//! an election in the middle of a test makes a write `-TRYAGAIN`.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus::counter::route::NO_LEADER;
use discus::counter::{group_key, CounterLeases, GroupStatus};
use discus_membership::{LeaseProvider, MemoryLease};
use discus_placement::NodeId;
use redis::aio::MultiplexedConnection;
use redis::{FromRedisValue, RedisError, RedisResult};

use super::gossip::{refs, socket, start_each, wait_agreed, wait_until, GossipNode, Net, Opts};

pub const GROUPS: u16 = 4;

pub const LEASE_MS: u64 = 2_000;

/// Flags for fast counter leases: duration 2000 ms, renew every 300 ms, margin 500 ms.
pub const FAST: &[(&str, &str)] = &[
    ("--counter-groups", "4"),
    ("--counter-lease-duration-ms", "2000"),
    ("--counter-lease-renew-ms", "300"),
    ("--counter-lease-margin-ms", "500"),
];

/// One in-memory lease per group, shared by every node of a test.
#[derive(Clone)]
pub struct Leases(pub Vec<MemoryLease>);

impl Leases {
    pub fn new() -> Leases {
        Leases((0..GROUPS).map(|_| MemoryLease::system(LEASE_MS)).collect())
    }

    /// The leases of one node, behind a gate that the test can close.
    pub fn for_node(&self, gate: Arc<AtomicBool>) -> CounterLeases {
        let leases = self.0.clone();
        Arc::new(move |g: u16| {
            Arc::new(leases[usize::from(g)].gated(gate.clone())) as Arc<dyn LeaseProvider>
        })
    }
}

pub fn group_of(n: &GossipNode, key: &str) -> u16 {
    n.node.counters().unwrap().group_of(key.as_bytes())
}

pub fn status(n: &GossipNode, g: u16) -> GroupStatus {
    n.node.counters().unwrap().status()[usize::from(g)].clone()
}

pub fn role(n: &GossipNode, g: u16) -> &'static str {
    status(n, g).role
}

/// The value that node `n` holds for `key` in its counter state.
pub fn held(n: &GossipNode, key: &str) -> Option<String> {
    let c = n.node.counters().unwrap();
    let state = c.held_state(c.group_of(key.as_bytes()))?;
    state
        .get(key.as_bytes())
        .map(|e| String::from_utf8_lossy(&e.value).into_owned())
}

/// Wait until every node of `nodes` holds `want` for `key`.
pub async fn all_hold(nodes: &[&GossipNode], key: &str, want: &str, timeout: Duration) -> bool {
    wait_until(timeout, || {
        nodes.iter().all(|n| held(n, key).as_deref() == Some(want))
    })
    .await
}

/// The node that leads group `g`, if exactly one of `nodes` does.
pub fn leader_of<'a>(nodes: &[&'a GossipNode], g: u16) -> Option<&'a GossipNode> {
    let leaders: Vec<&GossipNode> = nodes
        .iter()
        .copied()
        .filter(|n| role(n, g) == "leader")
        .collect();
    (leaders.len() == 1).then(|| leaders[0])
}

/// True when `e` is the server error `text`, given as `CODE detail`.
fn says(e: &RedisError, text: &str) -> bool {
    e.code()
        .zip(e.detail())
        .is_some_and(|(code, detail)| format!("{code} {detail}") == text)
}

pub fn is_try_again(e: &RedisError) -> bool {
    e.code() == Some("TRYAGAIN")
}

/// The counter write applied nothing, so it is safe to send again.
pub fn is_refused(e: &RedisError) -> bool {
    says(e, NO_LEADER)
}

/// Send `cmd` until it succeeds or `timeout` passes. An error that `retry` accepts is sent
/// again after 50 ms; any other error is returned at once.
pub async fn query_until<T: FromRedisValue>(
    c: &mut MultiplexedConnection,
    cmd: &redis::Cmd,
    timeout: Duration,
    retry: fn(&RedisError) -> bool,
) -> RedisResult<T> {
    let deadline = Instant::now() + timeout;
    loop {
        match cmd.query_async(c).await {
            Err(e) if retry(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            r => return r,
        }
    }
}

/// Send `INCR key` until it succeeds. It retries only refusals that applied nothing, so the
/// count stays exact.
pub async fn incr_once(c: &mut MultiplexedConnection, key: &str) -> i64 {
    query_until(
        c,
        redis::cmd("INCR").arg(key),
        Duration::from_secs(10),
        is_refused,
    )
    .await
    .unwrap_or_else(|e| panic!("INCR {key} failed: {e}"))
}

/// Send `INCR key` through any `-TRYAGAIN` until it succeeds or `timeout` passes.
pub async fn incr_until(c: &mut MultiplexedConnection, key: &str, timeout: Duration) -> i64 {
    incr_or_show(c, key, timeout, &[]).await
}

/// Like [`incr_until`]. On a timeout it prints the groups of every node of `nodes`.
pub async fn incr_or_show(
    c: &mut MultiplexedConnection,
    key: &str,
    timeout: Duration,
    nodes: &[&GossipNode],
) -> i64 {
    query_until(c, redis::cmd("INCR").arg(key), timeout, is_try_again)
        .await
        .unwrap_or_else(|e| panic!("INCR {key} kept failing: {e}\n{}", dump(nodes)))
}

/// Start `n` nodes on shared counter leases, each seeded with all the others. Every node gets
/// `extra` and the [`FAST`] lease flags that `extra` does not set.
pub async fn start(
    net: &Net,
    leases: &Leases,
    n: usize,
    extra: &[&str],
) -> (Vec<GossipNode>, Vec<Arc<AtomicBool>>) {
    start_each(n, async |s, seeds| join(net, leases, s, seeds, extra).await)
        .await
        .into_iter()
        .unzip()
}

/// Start one node on a new socket, seeded with `seeds`, like [`join`].
pub async fn join_to(
    net: &Net,
    leases: &Leases,
    seeds: &[&GossipNode],
    flags: &[&str],
) -> (GossipNode, Arc<AtomicBool>) {
    let addrs: Vec<_> = seeds.iter().map(|n| n.cluster_addr).collect();
    join(net, leases, socket().0, &addrs, flags).await
}

/// Start one node on `socket` with the shared leases, `flags`, and the [`FAST`] lease flags that
/// `flags` does not set.
pub async fn join(
    net: &Net,
    leases: &Leases,
    socket: std::net::UdpSocket,
    seeds: &[std::net::SocketAddr],
    flags: &[&str],
) -> (GossipNode, Arc<AtomicBool>) {
    let gate = Arc::new(AtomicBool::new(true));
    let opts = Opts {
        counter_leases: Some(leases.for_node(gate.clone())),
        defaults: FAST,
        ..Opts::default()
    };
    (net.start_on(socket, seeds, flags, opts).await, gate)
}

/// A key of group `g`, by trying `cnt:<prefix><i>`.
pub fn key_in(n: &GossipNode, g: u16, prefix: &str) -> String {
    (0..)
        .map(|i| format!("cnt:{prefix}{i}"))
        .find(|k| group_of(n, k) == g)
        .unwrap()
}

/// The target voters of group `g` in the epoch of `n`.
pub fn targets(n: &GossipNode, g: u16) -> Vec<NodeId> {
    discus_placement::hrw::owners(&group_key(g), n.node.epochs().current().members(), 3)
}

/// The groups whose targets include `n`.
pub fn targeted(n: &GossipNode) -> Vec<u16> {
    (0..GROUPS)
        .filter(|g| targets(n, *g).contains(&n.id))
        .collect()
}

/// The targets, and the role, leader, voters, and learners on every node, of every group, for
/// a failure message.
pub fn dump(nodes: &[&GossipNode]) -> String {
    let short = |id: &NodeId| id.to_string()[..12].to_string();
    let mut out = String::new();
    let Some(first) = nodes.first() else {
        return out;
    };
    for g in 0..GROUPS {
        out += &format!(
            "group {g}, targets {:?}\n",
            targets(first, g).iter().map(short).collect::<Vec<_>>()
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

/// Every group runs Raft with its three target voters, no learner, and one leader among
/// `nodes`. The voter moves are then done.
pub fn all_raft(nodes: &[&GossipNode]) -> bool {
    (0..GROUPS).all(|g| {
        leader_of(nodes, g).is_some_and(|l| {
            let s = status(l, g);
            let mut want = targets(l, g);
            want.sort();
            let mut voters = s.voters.clone();
            voters.sort();
            voters == want
                && s.learners.is_empty()
                && s.voters.iter().all(|v| {
                    nodes
                        .iter()
                        .any(|n| n.id == *v && matches!(role(n, g), "leader" | "voter"))
                })
        })
    })
}

/// The voters of group `g` by the leader among `nodes`.
pub fn voters_of<'a>(nodes: &[&'a GossipNode], g: u16) -> Vec<&'a GossipNode> {
    let Some(l) = leader_of(nodes, g) else {
        return Vec::new();
    };
    let voters = status(l, g).voters;
    nodes
        .iter()
        .copied()
        .filter(|n| voters.contains(&n.id))
        .collect()
}

/// Raft flags for tests: a dwell of one second, and a counter timeout that a busy test machine
/// does not run out of. The dwell flag comes first, so a test can replace it.
pub const RAFT: &[&str] = &[
    "--counter-dwell-ms",
    "1000",
    "--counter-timeout-ms",
    "3000",
    "--raft-voter-timeout-ms",
    "3000",
];

/// Death that takes longer than any test.
pub const SLOW_DEATH: &[&str] = &[
    "--suspicion-min-ms",
    "60000",
    "--suspicion-max-ms",
    "120000",
];

pub fn flags(extra: &[&'static str]) -> Vec<&'static str> {
    let mut f: Vec<&'static str> = RAFT.to_vec();
    f.extend_from_slice(extra);
    f
}

pub async fn raft_cluster(
    net: &Net,
    leases: &Leases,
    n: usize,
    extra: &[&'static str],
) -> Vec<GossipNode> {
    let (nodes, _) = start(net, leases, n, &flags(extra)).await;
    let refs = refs(&nodes);
    wait_agreed(&refs, Duration::from_secs(15)).await;
    assert!(
        wait_until(Duration::from_secs(20), || all_raft(&refs)).await,
        "the groups did not reach Raft mode\n{}",
        dump(&refs)
    );
    // Every entry node finds every leader before the test starts.
    for n in &nodes {
        let mut c = n.redis().await;
        for g in 0..GROUPS {
            let get = redis::cmd("GET").arg(key_in(n, g, "warm")).clone();
            query_until::<Option<i64>>(&mut c, &get, Duration::from_secs(10), |_| true)
                .await
                .unwrap_or_else(|e| panic!("group {g} has no leader: {e}"));
        }
    }
    nodes
}

/// Write a counter key with a short TTL, let it expire, purge on its leader, and check that no
/// node of `nodes` still holds it.
pub async fn purge_reaches_every_replica(nodes: &[&GossipNode]) {
    let key = "cnt:purge";
    let mut c = nodes[0].redis().await;
    let _: i64 = redis::cmd("INCR")
        .arg(key)
        .arg("PX")
        .arg(200)
        .query_async(&mut c)
        .await
        .unwrap();
    assert!(
        all_hold(nodes, key, "1", Duration::from_secs(5)).await,
        "the key did not reach every replica\n{}",
        dump(nodes)
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    let leader = leader_of(nodes, group_of(nodes[0], key)).unwrap();
    leader.node.counters().unwrap().purge().await;
    assert!(
        wait_until(Duration::from_secs(5), || nodes
            .iter()
            .all(|n| held(n, key).is_none()))
        .await,
        "a replica still holds the expired key\n{}",
        dump(nodes)
    );
}
