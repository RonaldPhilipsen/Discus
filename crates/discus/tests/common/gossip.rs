//! In-process gossip clusters over real QUIC on localhost, with one test CA and one shared
//! in-memory lease. The timings are fast: a 100 ms gossip period and a 500 ms bootstrap deadline.

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use discus::server::BindOptions;
use discus::{Config, Node, Server};
use discus_membership::{LeaseProvider, MemoryLease};
use discus_placement::{Clock, NodeId, NTP64};
use discus_transport::testing::{TestCa, TEST_SAN};
use tokio_util::sync::CancellationToken;

pub struct GossipNode {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub admin: SocketAddr,
    pub cluster_addr: SocketAddr,
    pub node: Arc<Node>,
    pub shutdown: CancellationToken,
}

impl GossipNode {
    /// Stop serving node-to-node traffic and client traffic at once, as a crash would: no
    /// drain, no goodbye.
    pub fn kill(&self) {
        if let Some(link) = self.node.link() {
            link.transport.close();
        }
        self.shutdown.cancel();
    }

    /// True when this node's store holds `key`.
    pub fn holds(&self, key: &[u8]) -> bool {
        self.node.store.read(key, |e| e.is_some())
    }

    /// The primary of `key` in this node's current epoch.
    pub fn primary(&self, key: &[u8]) -> NodeId {
        self.node.epochs().current().primary(key).unwrap()
    }

    pub fn is_normal(&self) -> bool {
        self.node.is_normal()
    }

    /// A connection whose replies time out after 30 s, so a lost reply fails the test instead of
    /// hanging it.
    pub async fn redis(&self) -> redis::aio::MultiplexedConnection {
        let client = redis::Client::open(format!("redis://{}", self.addr)).unwrap();
        let config =
            redis::AsyncConnectionConfig::new().set_response_timeout(Duration::from_secs(30));
        client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .unwrap()
    }
}

/// Settings that a test can override by passing the same flag in `extra`.
const DEFAULTS: &[(&str, &str)] = &[
    ("--gossip-interval-ms", "100"),
    ("--gossip-probe-timeout-ms", "50"),
    ("--bootstrap-deadline-ms", "500"),
    ("--suspicion-min-ms", "300"),
    ("--suspicion-max-ms", "1000"),
    ("--lease-renew-ms", "300"),
    ("--lease-provider", "none"),
    ("--cluster-handshake-timeout-ms", "500"),
    ("--forward-read-timeout-ms", "2000"),
    ("--forward-write-timeout-ms", "2000"),
    ("--replication-factor", "1"),
    ("--shutdown-timeout-secs", "1"),
];

pub struct Net {
    pub ca: TestCa,
    pub dir: tempfile::TempDir,
    pub lease: MemoryLease,
}

pub fn socket() -> (UdpSocket, SocketAddr) {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let a = s.local_addr().unwrap();
    (s, a)
}

/// The HLC clock and the wall clock of one node.
pub type Clocks = (fn() -> NTP64, Arc<dyn Clock>);

/// Extra parts for one node.
#[derive(Default)]
pub struct Opts {
    pub lease: Option<Arc<dyn LeaseProvider>>,
    pub clocks: Option<Clocks>,
    pub counter_leases: Option<discus::counter::CounterLeases>,
    /// More settings that `extra` can override, after [`DEFAULTS`].
    pub defaults: &'static [(&'static str, &'static str)],
}

impl Net {
    pub fn new() -> Net {
        Net::with_lease_ms(2_000)
    }

    pub fn with_lease_ms(ms: u64) -> Net {
        discus::security::tls::install_default();
        let (ca, dir) = ca_files();
        Net {
            ca,
            dir,
            lease: MemoryLease::system(ms),
        }
    }

    /// Start a node on `socket`, seeded with `seeds`.
    pub async fn start_on(
        &self,
        socket: UdpSocket,
        seeds: &[SocketAddr],
        extra: &[&str],
        opts: Opts,
    ) -> GossipNode {
        let cluster_addr = socket.local_addr().unwrap();
        let peers = seeds.iter().map(|s| s.to_string());
        let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let defaults: Vec<(&str, &str)> = DEFAULTS.iter().chain(opts.defaults).copied().collect();
        let argv = argv(&self.dir, cluster_addr, peers, &defaults, &extra);
        let config = Config::parse_from(argv).expect("valid gossip arguments");
        let lease = opts
            .lease
            .unwrap_or_else(|| Arc::new(self.lease.clone()) as Arc<dyn LeaseProvider>);
        let (hlc_clock, wall) = opts.clocks.unzip();
        let server = Server::bind_with(
            config,
            BindOptions {
                cluster_socket: Some(socket),
                lease: Some(lease),
                hlc_clock,
                wall,
                counter_leases: opts.counter_leases,
            },
        )
        .await
        .expect("bind");
        spawn(server, cluster_addr)
    }

    pub async fn start(&self, seeds: &[SocketAddr], extra: &[&str]) -> GossipNode {
        self.start_on(socket().0, seeds, extra, Opts::default())
            .await
    }

    /// Start `n` nodes at once, each seeded with all the others.
    pub async fn start_all(&self, n: usize, extra: &[&str]) -> Vec<GossipNode> {
        start_each(n, async |s, seeds| {
            self.start_on(s, seeds, extra, Opts::default()).await
        })
        .await
    }
}

/// Bind `n` sockets and call `start` with each socket and the addresses of all the others.
pub async fn start_each<T>(
    n: usize,
    mut start: impl AsyncFnMut(UdpSocket, &[SocketAddr]) -> T,
) -> Vec<T> {
    let sockets: Vec<(UdpSocket, SocketAddr)> = (0..n).map(|_| socket()).collect();
    let addrs: Vec<SocketAddr> = sockets.iter().map(|(_, a)| *a).collect();
    let mut out = Vec::new();
    for (i, (s, _)) in sockets.into_iter().enumerate() {
        let seeds: Vec<SocketAddr> = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| *a)
            .collect();
        out.push(start(s, &seeds).await);
    }
    out
}

/// A test CA, and its CA, certificate, and key files in a new temporary directory.
pub fn ca_files() -> (TestCa, tempfile::TempDir) {
    let ca = TestCa::new();
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = ca.issue(TEST_SAN);
    std::fs::write(dir.path().join("ca.pem"), ca.ca_pem()).unwrap();
    std::fs::write(dir.path().join("cert.pem"), cert).unwrap();
    std::fs::write(dir.path().join("key.pem"), key).unwrap();
    (ca, dir)
}

/// The arguments of a node on `cluster_addr` with the files of [`ca_files`] in `dir`, then a
/// `--peer` flag for each of `peers`, the `defaults` that `extra` does not set, and `extra`.
pub fn argv(
    dir: &tempfile::TempDir,
    cluster_addr: SocketAddr,
    peers: impl IntoIterator<Item = String>,
    defaults: &[(&str, &str)],
    extra: &[String],
) -> Vec<String> {
    let path = |name: &str| dir.path().join(name).display().to_string();
    let mut argv: Vec<String> = [
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
        "--cluster-san",
        TEST_SAN,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    argv.extend([
        "--cluster-listen".into(),
        cluster_addr.to_string(),
        "--cluster-ca".into(),
        path("ca.pem"),
        "--cluster-cert".into(),
        path("cert.pem"),
        "--cluster-key".into(),
        path("key.pem"),
    ]);
    for peer in peers {
        argv.push("--peer".into());
        argv.push(peer);
    }
    for (flag, value) in defaults {
        if !extra.iter().any(|e| e == flag) {
            argv.push(flag.to_string());
            argv.push(value.to_string());
        }
    }
    argv.extend(extra.iter().cloned());
    argv
}

/// Run a bound server until its node's shutdown token is cancelled.
pub fn spawn(server: Server, cluster_addr: SocketAddr) -> GossipNode {
    let node = server.node().clone();
    let (addr, admin) = (server.local_addr(), server.admin_addr());
    let shutdown = CancellationToken::new();
    tokio::spawn(server.run(shutdown.clone()));
    GossipNode {
        id: node.id,
        addr,
        admin,
        cluster_addr,
        node,
        shutdown,
    }
}

/// Poll `cond` every 20 ms until it holds or `timeout` passes.
pub async fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn refs(nodes: &[GossipNode]) -> Vec<&GossipNode> {
    nodes.iter().collect()
}

/// Wait until `nodes` are [`agreed`], or fail the test.
pub async fn wait_agreed(nodes: &[&GossipNode], timeout: Duration) {
    assert!(
        wait_until(timeout, || agreed(nodes)).await,
        "the nodes did not agree on one epoch"
    );
}

/// Every node in `nodes` is NORMAL, in one cluster, with one epoch that lists exactly them.
pub fn agreed(nodes: &[&GossipNode]) -> bool {
    let first = nodes[0].node.epochs().current().clone();
    let mut want: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
    want.sort();
    let listed: Vec<NodeId> = first.members().iter().map(|m| m.id).collect();
    nodes.iter().all(|n| {
        n.is_normal()
            && n.node.cluster_id() == first.cluster_id()
            && n.node.epochs().current().id() == first.id()
    }) && listed == want
}
