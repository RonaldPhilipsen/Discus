//! Helpers for counter tests: shared group leases, a lease that one node can lose, and waits on
//! the role of each group.
//!
//! The multi-node tests of one binary run one at a time (`#[serial]`). Many clusters at once
//! starve the Raft timers on a busy machine, and an election in the middle of a test makes a
//! write `-TRYAGAIN`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus::counter::{CounterLeases, GroupStatus};
use discus_membership::{LeaseError, LeaseHolder, LeaseProvider, LeaseState, MemoryLease};
use futures::future::BoxFuture;
use redis::aio::MultiplexedConnection;

use super::gossip::{agreed, socket, wait_until, GossipNode, Net, Opts};

pub const GROUPS: u16 = 4;

pub const LEASE_MS: u64 = 2_000;

/// Flags for fast counter leases: duration 2000 ms, renew every 300 ms, margin 500 ms.
pub const FAST: &[&str] = &[
    "--counter-groups",
    "4",
    "--counter-lease-duration-ms",
    "2000",
    "--counter-lease-renew-ms",
    "300",
    "--counter-lease-margin-ms",
    "500",
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
            Arc::new(Gated {
                inner: leases[usize::from(g)].clone(),
                open: gate.clone(),
            }) as Arc<dyn LeaseProvider>
        })
    }
}

/// A lease whose calls fail while the gate is closed, as if this node lost the API server.
struct Gated {
    inner: MemoryLease,
    open: Arc<AtomicBool>,
}

impl LeaseProvider for Gated {
    fn acquire_or_renew<'a>(
        &'a self,
        me: &'a LeaseHolder,
    ) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        if !self.open.load(Ordering::Relaxed) {
            return Box::pin(async { Err(LeaseError("the gate is closed".into())) });
        }
        self.inner.acquire_or_renew(me)
    }

    fn name(&self) -> &'static str {
        "gated"
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

/// The node that leads group `g`, if exactly one of `nodes` does.
pub fn leader_of<'a>(nodes: &[&'a GossipNode], g: u16) -> Option<&'a GossipNode> {
    let leaders: Vec<&GossipNode> = nodes
        .iter()
        .copied()
        .filter(|n| role(n, g) == "leader")
        .collect();
    (leaders.len() == 1).then(|| leaders[0])
}

/// Send `INCR key` until it succeeds. It retries only refusals that applied nothing, so the
/// count stays exact.
pub async fn incr_once(c: &mut MultiplexedConnection, key: &str) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let r: redis::RedisResult<i64> = redis::cmd("INCR").arg(key).query_async(c).await;
        match r {
            Ok(n) => return n,
            Err(e) if e.to_string().contains("has no leader") && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("INCR {key} failed: {e}"),
        }
    }
}

/// Send `INCR key` until it succeeds or `timeout` passes.
pub async fn incr_until(c: &mut MultiplexedConnection, key: &str, timeout: Duration) -> i64 {
    incr_or_show(c, key, timeout, &[]).await
}

/// Like [`incr_until`]. On a timeout it prints the group of `key` on every node of `nodes`.
pub async fn incr_or_show(
    c: &mut MultiplexedConnection,
    key: &str,
    timeout: Duration,
    nodes: &[&GossipNode],
) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let r: redis::RedisResult<i64> = redis::cmd("INCR").arg(key).query_async(c).await;
        match r {
            Ok(n) => return n,
            Err(e) if Instant::now() < deadline => {
                assert!(e.to_string().to_uppercase().contains("TRYAGAIN"), "{e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                for n in nodes {
                    let g = group_of(n, key);
                    let epoch = n.node.epochs().current().clone();
                    eprintln!("{} normal={} {:?}", n.id, n.is_normal(), status(n, g));
                    eprintln!(
                        "  epoch {:?}",
                        epoch
                            .members()
                            .iter()
                            .map(|m| (m.id, m.state))
                            .collect::<Vec<_>>()
                    );
                }
                panic!("INCR {key} kept failing: {e}");
            }
        }
    }
}

/// Start `n` nodes on shared counter leases, each seeded with all the others. Every node gets
/// `extra` and the [`FAST`] lease flags that `extra` does not set.
pub async fn start(
    net: &Net,
    leases: &Leases,
    n: usize,
    extra: &[&str],
) -> (Vec<GossipNode>, Vec<Arc<AtomicBool>>) {
    let sockets: Vec<_> = (0..n).map(|_| socket()).collect();
    let addrs: Vec<_> = sockets.iter().map(|(_, a)| *a).collect();
    let (mut nodes, mut gates) = (Vec::new(), Vec::new());
    for (i, (s, _)) in sockets.into_iter().enumerate() {
        let seeds: Vec<_> = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| *a)
            .collect();
        let (node, gate) = join(net, leases, s, &seeds, extra).await;
        nodes.push(node);
        gates.push(gate);
    }
    (nodes, gates)
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
        ..Opts::default()
    };
    let mut all: Vec<&str> = FAST
        .chunks(2)
        .filter(|pair| !flags.contains(&pair[0]))
        .flatten()
        .copied()
        .collect();
    all.extend_from_slice(flags);
    (net.start_on(socket, seeds, &all, opts).await, gate)
}

/// A key of group `g`, by trying `cnt:<prefix><i>`.
pub fn key_in(n: &GossipNode, g: u16, prefix: &str) -> String {
    (0..)
        .map(|i| format!("cnt:{prefix}{i}"))
        .find(|k| group_of(n, k) == g)
        .unwrap()
}

/// Every group runs Raft with its three target voters, no learner, and one leader among
/// `nodes`. The voter moves are then done.
pub fn all_raft(nodes: &[&GossipNode]) -> bool {
    (0..GROUPS).all(|g| {
        leader_of(nodes, g).is_some_and(|l| {
            let s = status(l, g);
            let mut targets = l
                .node
                .epochs()
                .current()
                .owners(&discus::counter::group_key(g), 3);
            targets.sort();
            let mut voters = s.voters.clone();
            voters.sort();
            voters == targets
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
    let refs: Vec<&GossipNode> = nodes.iter().collect();
    assert!(
        wait_until(Duration::from_secs(15), || agreed(&refs)).await,
        "no agreement"
    );
    if !wait_until(Duration::from_secs(20), || all_raft(&refs)).await {
        for n in &nodes {
            for g in 0..GROUPS {
                eprintln!("{} g{g}: {:?}", n.id, status(n, g));
            }
        }
        panic!("the groups did not reach Raft mode");
    }
    // Every entry node finds every leader before the test starts.
    for n in &nodes {
        let mut c = n.redis().await;
        for g in 0..GROUPS {
            let key = key_in(n, g, "warm");
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while redis::cmd("GET")
                .arg(&key)
                .query_async::<Option<i64>>(&mut c)
                .await
                .is_err()
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "group {g} has no leader"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    nodes
}
