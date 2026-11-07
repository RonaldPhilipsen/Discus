//! In-process static clusters over real QUIC on localhost with a test CA.

use std::net::SocketAddr;
use std::sync::Arc;

use discus::{Config, Node, Server};
use discus_placement::{ClusterId, NodeId};
use discus_transport::testing::{TestCa, TEST_SAN};
use tokio_util::sync::CancellationToken;

pub struct ClusterNode {
    pub id: NodeId,
    /// The RESP address.
    pub addr: SocketAddr,
    /// The QUIC address.
    pub cluster_addr: SocketAddr,
    pub node: Arc<Node>,
    pub shutdown: CancellationToken,
}

impl ClusterNode {
    /// Stop serving node-to-node traffic and client traffic at once.
    pub fn stop(&self) {
        if let Some(link) = self.node.link() {
            link.transport.close();
        }
        self.shutdown.cancel();
    }

    pub async fn redis(&self) -> redis::aio::MultiplexedConnection {
        let client = redis::Client::open(format!("redis://{}", self.addr)).unwrap();
        client.get_multiplexed_async_connection().await.unwrap()
    }

    /// True when this node's store holds `key`.
    pub fn holds(&self, key: &[u8]) -> bool {
        self.node.store.read(key, |e| e.is_some())
    }

    /// The primary of `key` in this node's current epoch.
    pub fn primary(&self, key: &[u8]) -> NodeId {
        self.node.epochs().current().primary(key).unwrap()
    }
}

pub struct Cluster {
    pub nodes: Vec<ClusterNode>,
    pub ca: TestCa,
    pub cluster_id: ClusterId,
    pub dir: tempfile::TempDir,
}

impl Cluster {
    pub fn by_id(&self, id: NodeId) -> &ClusterNode {
        self.nodes.iter().find(|n| n.id == id).unwrap()
    }

    /// A key whose primary is node `owner`, found by trying `prefix:0`, `prefix:1`, and so on.
    pub fn key_on(&self, owner: usize, prefix: &str) -> String {
        let want = self.nodes[owner].id;
        (0..)
            .map(|i| format!("{prefix}:{i}"))
            .find(|k| self.nodes[0].primary(k.as_bytes()) == want)
            .unwrap()
    }
}

/// Defaults that a test can override by passing the same flag in `extra`.
const DEFAULTS: &[(&str, &str)] = &[
    ("--forward-read-timeout-ms", "2000"),
    ("--forward-write-timeout-ms", "2000"),
    ("--replication-factor", "1"),
    ("--shutdown-timeout-secs", "1"),
];

/// Start `n` nodes that list each other as static peers. `extra(i)` adds flags for node `i`.
pub async fn start_cluster_with(n: usize, extra: impl Fn(usize) -> Vec<String>) -> Cluster {
    let ca = TestCa::new();
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = ca.issue(TEST_SAN);
    let path = |name: &str| dir.path().join(name).display().to_string();
    std::fs::write(path("ca.pem"), ca.ca_pem()).unwrap();
    std::fs::write(path("cert.pem"), cert).unwrap();
    std::fs::write(path("key.pem"), key).unwrap();
    let cluster_id = ClusterId::mint();
    let sockets: Vec<std::net::UdpSocket> = (0..n)
        .map(|_| std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .collect();
    let addrs: Vec<SocketAddr> = sockets.iter().map(|s| s.local_addr().unwrap()).collect();
    let ids: Vec<NodeId> = (0..n).map(|_| NodeId::random()).collect();
    let mut nodes = Vec::new();
    for (i, socket) in sockets.into_iter().enumerate() {
        let extra = extra(i);
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
            "--node-id".into(),
            ids[i].to_string(),
            "--cluster-id".into(),
            cluster_id.to_string(),
            "--cluster-listen".into(),
            addrs[i].to_string(),
            "--cluster-ca".into(),
            path("ca.pem"),
            "--cluster-cert".into(),
            path("cert.pem"),
            "--cluster-key".into(),
            path("key.pem"),
        ]);
        for j in (0..n).filter(|&j| j != i) {
            argv.push("--peer".into());
            argv.push(format!("{}@{}", ids[j], addrs[j]));
        }
        for (flag, value) in DEFAULTS {
            if !extra.iter().any(|e| e == flag) {
                argv.push(flag.to_string());
                argv.push(value.to_string());
            }
        }
        argv.extend(extra);
        let config = Config::parse_from(argv).expect("valid cluster arguments");
        let server = Server::bind_with_cluster_socket(config, socket)
            .await
            .expect("bind");
        let node = server.node().clone();
        let addr = server.local_addr();
        let shutdown = CancellationToken::new();
        tokio::spawn(server.run(shutdown.clone()));
        nodes.push(ClusterNode {
            id: ids[i],
            addr,
            cluster_addr: addrs[i],
            node,
            shutdown,
        });
    }
    Cluster {
        nodes,
        ca,
        cluster_id,
        dir,
    }
}

pub async fn start_cluster(n: usize, extra: &[&str]) -> Cluster {
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    start_cluster_with(n, move |_| extra.clone()).await
}
