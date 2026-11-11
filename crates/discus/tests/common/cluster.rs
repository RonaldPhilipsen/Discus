//! In-process static clusters (`--membership static`) over real QUIC on localhost with a test CA.

use std::net::SocketAddr;

use discus::{Config, Server};
use discus_placement::{ClusterId, NodeId};
use discus_transport::testing::TestCa;

pub use super::gossip::GossipNode as ClusterNode;
use super::gossip::{argv, ca_files, socket, spawn};

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
    start_cluster_clients(n, |_| None, extra).await
}

/// [`start_cluster_with`], where `client(i)` is the client address that the other nodes list for
/// node `i` after the slash of `--peer`.
pub async fn start_cluster_clients(
    n: usize,
    client: impl Fn(usize) -> Option<SocketAddr>,
    extra: impl Fn(usize) -> Vec<String>,
) -> Cluster {
    try_start_cluster_clients(n, client, extra)
        .await
        .expect("bind")
}

/// [`start_cluster_clients`], or the bind error of the first node that fails. The nodes that
/// started before it stop.
async fn try_start_cluster_clients(
    n: usize,
    client: impl Fn(usize) -> Option<SocketAddr>,
    extra: impl Fn(usize) -> Vec<String>,
) -> std::io::Result<Cluster> {
    let (ca, dir) = ca_files();
    let cluster_id = ClusterId::mint();
    let sockets: Vec<_> = (0..n).map(|_| socket()).collect();
    let addrs: Vec<_> = sockets.iter().map(|s| s.1).collect();
    let ids: Vec<NodeId> = (0..n).map(|_| NodeId::random()).collect();
    let mut nodes: Vec<ClusterNode> = Vec::new();
    for (i, (socket, addr)) in sockets.into_iter().enumerate() {
        let peers = (0..n).filter(|&j| j != i).map(|j| match client(j) {
            Some(c) => format!("{}@{}/{c}", ids[j], addrs[j]),
            None => format!("{}@{}", ids[j], addrs[j]),
        });
        let mut flags: Vec<String> = ["--membership", "static", "--node-id"]
            .map(String::from)
            .to_vec();
        flags.extend([
            ids[i].to_string(),
            "--cluster-id".into(),
            cluster_id.to_string(),
        ]);
        flags.extend(extra(i));
        let config = Config::parse_from(argv(&dir, addr, peers, DEFAULTS, &flags))
            .expect("valid cluster arguments");
        match Server::bind_with_cluster_socket(config, socket).await {
            Ok(server) => nodes.push(spawn(server, addr)),
            Err(e) => {
                nodes.iter().for_each(ClusterNode::kill);
                return Err(e);
            }
        }
    }
    Ok(Cluster {
        nodes,
        ca,
        cluster_id,
        dir,
    })
}

/// A free TCP address on localhost. A port that the probe frees can be taken again before the
/// node binds it, so callers retry with fresh ports on `AddrInUse`.
pub fn free_port() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

/// How many times a start with reserved ports is tried before the test fails.
const PORT_ATTEMPTS: usize = 10;

/// `n` static nodes, each with a redirect listener on a reserved port that it advertises and that
/// its peers list. `extra(i)` adds flags for node `i`. A node that loses its reserved port to
/// another process fails with `AddrInUse`, and the whole cluster starts again on fresh ports.
pub async fn start_redirect_cluster(
    n: usize,
    extra: impl Fn(usize) -> Vec<String>,
) -> (Cluster, Vec<SocketAddr>) {
    for _ in 0..PORT_ATTEMPTS {
        let ports: Vec<SocketAddr> = (0..n).map(|_| free_port()).collect();
        let started = try_start_cluster_clients(
            n,
            |i| Some(ports[i]),
            |i| {
                let mut flags: Vec<String> = vec![
                    "--redirect-listen".into(),
                    ports[i].to_string(),
                    "--client-advertise".into(),
                    ports[i].to_string(),
                ];
                flags.extend(extra(i));
                flags
            },
        )
        .await;
        match started {
            Ok(c) => return (c, ports),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(e) => panic!("bind: {e}"),
        }
    }
    panic!("no free redirect ports after {PORT_ATTEMPTS} attempts");
}

pub async fn start_cluster(n: usize, extra: &[&str]) -> Cluster {
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    start_cluster_with(n, move |_| extra.clone()).await
}
