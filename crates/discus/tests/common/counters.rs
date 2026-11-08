//! Helpers for counter tests: shared group leases, a lease that one node can lose, and waits on
//! the role of each group.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus::counter::{CounterLeases, GroupStatus};
use discus_membership::{LeaseError, LeaseHolder, LeaseProvider, LeaseState, MemoryLease};
use futures::future::BoxFuture;
use redis::aio::MultiplexedConnection;

use super::gossip::GossipNode;

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

/// Send `INCR key` until it succeeds or `timeout` passes.
pub async fn incr_until(c: &mut MultiplexedConnection, key: &str, timeout: Duration) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let r: redis::RedisResult<i64> = redis::cmd("INCR").arg(key).query_async(c).await;
        match r {
            Ok(n) => return n,
            Err(e) if Instant::now() < deadline => {
                assert!(e.to_string().to_uppercase().contains("TRYAGAIN"), "{e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("INCR {key} kept failing: {e}"),
        }
    }
}
