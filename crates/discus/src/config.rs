//! Node configuration, parsed from command-line flags.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use discus_placement::{ClusterId, NodeId};

/// A static peer: `<node-id>@<quic-addr>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    pub id: NodeId,
    pub addr: SocketAddr,
}

/// Configuration for one Discus node.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "discus",
    about = "A Redis-compatible, horizontally scalable cache"
)]
pub struct Config {
    /// Client (RESP) listen address.
    #[arg(long, default_value = "0.0.0.0:6379")]
    pub listen: SocketAddr,
    /// Admin (probes) listen address.
    #[arg(long, default_value = "0.0.0.0:9100")]
    pub admin_listen: SocketAddr,
    /// Number of store shards.
    #[arg(long, default_value_t = 256)]
    pub shards: usize,
    /// Memory budget in bytes.
    #[arg(long, default_value_t = 1 << 30)]
    pub max_memory: u64,
    /// Maximum key size in bytes.
    #[arg(long, default_value_t = 65536)]
    pub max_key_bytes: usize,
    /// Maximum value size in bytes.
    #[arg(long, default_value_t = 67_108_864)]
    pub max_value_bytes: usize,
    /// Maximum bytes buffered for one unfinished request on a connection.
    #[arg(long, default_value_t = 134_217_728)]
    pub max_request_bytes: usize,
    /// Maximum in-flight commands per connection.
    #[arg(long, default_value_t = 10_000)]
    pub max_pipeline: usize,
    /// Maximum members in one collection value.
    #[arg(long, default_value_t = 1_048_576)]
    pub max_collection_members: usize,
    /// Seconds to wait for connections to drain on shutdown.
    #[arg(long, default_value_t = 30)]
    pub shutdown_timeout_secs: u64,
    /// Number of owners per key: the primary plus its secondaries. At least 1.
    #[arg(long, default_value_t = discus_placement::DEFAULT_RF, value_parser = parse_rf)]
    pub replication_factor: usize,
    /// Placement weight of this node. Finite and greater than zero.
    #[arg(long, default_value_t = 1.0, value_parser = parse_weight)]
    pub weight: f64,
    /// Shared token that clients must send with AUTH.
    #[arg(long)]
    pub shared_token: Option<String>,

    /// This node's ID, 32 hex digits. Random by default. Required with --peer.
    #[arg(long)]
    pub node_id: Option<NodeId>,
    /// The cluster ID, 32 hex digits. Minted by default. Required with --peer.
    #[arg(long)]
    pub cluster_id: Option<ClusterId>,
    /// Node-to-node (QUIC, UDP) listen address.
    #[arg(long, default_value = "0.0.0.0:7379")]
    pub cluster_listen: SocketAddr,
    /// A static peer as `<node-id>@<address>`. Repeat for each other member.
    #[arg(long = "peer", value_parser = parse_peer)]
    pub peers: Vec<Peer>,
    /// PEM file with the CA that issues node certificates.
    #[arg(long)]
    pub cluster_ca: Option<PathBuf>,
    /// PEM file with this node's certificate chain.
    #[arg(long)]
    pub cluster_cert: Option<PathBuf>,
    /// PEM file with this node's private key.
    #[arg(long)]
    pub cluster_key: Option<PathBuf>,
    /// The URI SAN that every peer certificate must carry.
    #[arg(long, default_value = "spiffe://cluster.local/ns/default/sa/discus")]
    pub cluster_san: String,
    /// Forward timeout for reads, in milliseconds. Then the next owner is tried.
    #[arg(long, default_value_t = 20)]
    pub forward_read_timeout_ms: u64,
    /// Forward timeout for writes, in milliseconds.
    #[arg(long, default_value_t = 200)]
    pub forward_write_timeout_ms: u64,
    /// Timeout for a QUIC connect plus HELLO, in milliseconds.
    #[arg(long, default_value_t = 2000)]
    pub cluster_handshake_timeout_ms: u64,
    /// QUIC idle timeout, in milliseconds.
    #[arg(long, default_value_t = 30_000)]
    pub cluster_idle_timeout_ms: u64,
    /// QUIC keep-alive interval, in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    pub cluster_keep_alive_ms: u64,
    /// Largest node-to-node frame this node accepts, in bytes.
    #[arg(long, default_value_t = discus_wire::DEFAULT_MAX_FRAME)]
    pub cluster_max_frame_bytes: usize,
    /// Read from any owner instead of the primary. Off until replication exists.
    #[arg(long)]
    pub read_from_replicas: bool,
    /// Capability bits offered in HELLO. For tests of mixed builds.
    #[arg(long, hide = true, default_value_t = discus_wire::caps::KNOWN)]
    pub cluster_capabilities: u64,
    /// Security flags (spec §15.3).
    #[command(flatten)]
    pub security: crate::security::SecurityArgs,
    /// Telemetry flags (spec §15.4).
    #[command(flatten)]
    pub telemetry: crate::telemetry::TelemetryArgs,
}

impl Config {
    /// Parse from an argument list. The first item is the program name.
    pub fn parse_from<I, T>(args: I) -> Result<Config, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        <Config as Parser>::try_parse_from(args)
    }

    fn cert_flags(&self) -> [(&'static str, bool); 3] {
        [
            ("--cluster-ca", self.cluster_ca.is_some()),
            ("--cluster-cert", self.cluster_cert.is_some()),
            ("--cluster-key", self.cluster_key.is_some()),
        ]
    }

    /// True when the node runs the QUIC transport: it has peers or certificates.
    pub fn cluster_enabled(&self) -> bool {
        !self.peers.is_empty() || self.cert_flags().iter().all(|(_, set)| *set)
    }

    /// Check the flags that depend on each other. The message names the flags to fix.
    pub fn validate(&self) -> Result<(), String> {
        let certs = self.cert_flags();
        let set = certs.iter().filter(|(_, s)| *s).count();
        if !self.peers.is_empty() {
            let mut missing: Vec<&str> = Vec::new();
            if self.node_id.is_none() {
                missing.push("--node-id");
            }
            if self.cluster_id.is_none() {
                missing.push("--cluster-id");
            }
            missing.extend(certs.iter().filter(|(_, s)| !*s).map(|(f, _)| *f));
            if !missing.is_empty() {
                return Err(format!(
                    "--peer needs {}. Node-to-node traffic always uses mTLS",
                    missing.join(", ")
                ));
            }
            if self.weight != 1.0 {
                return Err(
                    "--weight must be 1 with --peer, because every static member has weight 1"
                        .into(),
                );
            }
            for (i, p) in self.peers.iter().enumerate() {
                if Some(p.id) == self.node_id {
                    return Err(format!("--peer {} is this node's own ID", p.id));
                }
                if self.peers[..i].iter().any(|q| q.id == p.id) {
                    return Err(format!("--peer {} is listed twice", p.id));
                }
            }
        } else if set != 0 && set != certs.len() {
            let missing: Vec<&str> = certs.iter().filter(|(_, s)| !*s).map(|(f, _)| *f).collect();
            return Err(format!(
                "the cluster certificate flags need {}",
                missing.join(", ")
            ));
        }
        Ok(())
    }

    /// Check the flags, then pick a random node ID when none is set. `main` calls it before
    /// telemetry starts, so the telemetry resource and the node share one ID.
    pub fn resolve_identity(&mut self) -> Result<NodeId, String> {
        self.validate()?;
        Ok(*self.node_id.get_or_insert_with(NodeId::random))
    }

    pub fn forward_read_timeout(&self) -> Duration {
        Duration::from_millis(self.forward_read_timeout_ms)
    }

    pub fn forward_write_timeout(&self) -> Duration {
        Duration::from_millis(self.forward_write_timeout_ms)
    }
}

fn parse_peer(s: &str) -> Result<Peer, String> {
    let (id, addr) = s
        .split_once('@')
        .ok_or_else(|| "a peer is <node-id>@<address>".to_string())?;
    let id = id
        .parse::<NodeId>()
        .map_err(|_| "a peer node ID is 32 hex digits".to_string())?;
    let addr = addr
        .parse::<SocketAddr>()
        .map_err(|_| "a peer address is <ip>:<port>".to_string())?;
    Ok(Peer { id, addr })
}

fn parse_rf(s: &str) -> Result<usize, String> {
    match s.parse::<usize>() {
        Ok(n) if n >= 1 => Ok(n),
        _ => Err("the replication factor is a whole number of at least 1".into()),
    }
}

fn parse_weight(s: &str) -> Result<f64, String> {
    s.parse::<f64>()
        .ok()
        .and_then(discus_placement::Weight::new)
        .map(|w| w.get())
        .ok_or_else(|| "the weight is a finite number greater than zero".into())
}
