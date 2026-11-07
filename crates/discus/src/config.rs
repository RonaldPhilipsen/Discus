//! Node configuration, parsed from command-line flags.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use discus_placement::{ClusterId, NodeId};

/// A peer: `<addr>` or `<node-id>@<addr>`. With gossip it is a discovery seed, and the ID is only
/// an expectation. With static membership the ID is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    pub id: Option<NodeId>,
    pub addr: SocketAddr,
}

/// How the node learns the member list (spec section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MembershipMode {
    /// SWIM gossip with the lease bootstrap. `--peer` entries are discovery seeds.
    Gossip,
    /// The fixed member list of `--peer`, with no gossip and no failure detection.
    Static,
}

/// The cluster lease provider (spec section 6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LeaseKind {
    /// The Kubernetes `Lease`. Needs the `k8s` build feature.
    K8s,
    /// No lease: a node forms a cluster alone after the bootstrap deadline. Development only.
    None,
}

impl LeaseKind {
    /// `k8s` when the binary has the `k8s` feature, else `none`.
    pub const fn default_kind() -> LeaseKind {
        if cfg!(feature = "k8s") {
            LeaseKind::K8s
        } else {
            LeaseKind::None
        }
    }
}

impl std::fmt::Display for LeaseKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LeaseKind::K8s => "k8s",
            LeaseKind::None => "none",
        })
    }
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
    /// How long a delete marker (tombstone) lives, in milliseconds.
    #[arg(long, default_value_t = discus_store::DEFAULT_TOMBSTONE_TTL_MS)]
    pub tombstone_ttl_ms: u64,
    /// The share of the memory budget for tombstones, in percent. Past it, the oldest
    /// tombstones are dropped and their shards marked for refill.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=100))]
    pub tombstone_reserve_percent: u64,
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

    /// This node's ID, 32 hex digits. Random by default. Required with static membership.
    #[arg(long)]
    pub node_id: Option<NodeId>,
    /// The cluster ID, 32 hex digits. Required with static membership. With gossip it is the ID
    /// that this node mints if it forms a cluster.
    #[arg(long)]
    pub cluster_id: Option<ClusterId>,
    /// How the node learns the member list.
    #[arg(long, value_enum, default_value_t = MembershipMode::Gossip)]
    pub membership: MembershipMode,
    /// Node-to-node (QUIC, UDP) listen address.
    #[arg(long, default_value = "0.0.0.0:7379")]
    pub cluster_listen: SocketAddr,
    /// A peer as `<address>` or `<node-id>@<address>`. A discovery seed with gossip, a member
    /// with static membership. Repeatable.
    #[arg(long = "peer", value_parser = parse_peer)]
    pub peers: Vec<Peer>,
    /// A headless Service name to resolve for discovery seeds, as `<host>` or `<host>:<port>`.
    /// The port defaults to the `--cluster-listen` port.
    #[arg(long)]
    pub discovery_dns: Option<String>,
    /// The node-to-node address that other members use. Default: `--cluster-listen` when its IP
    /// is specified, else the local address toward the first seed.
    #[arg(long)]
    pub cluster_advertise: Option<SocketAddr>,
    /// The cluster lease provider.
    #[arg(long, value_enum, default_value_t = LeaseKind::default_kind())]
    pub lease_provider: LeaseKind,
    /// The name of the cluster lease.
    #[arg(long, default_value = "discus-cluster")]
    pub lease_name: String,
    /// The namespace of the cluster lease. Default: the service account namespace.
    #[arg(long)]
    pub lease_namespace: Option<String>,
    /// The lease duration, in milliseconds.
    #[arg(long, default_value_t = 15_000)]
    pub lease_duration_ms: u64,
    /// The lease renewal interval, in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    pub lease_renew_ms: u64,
    /// Time to look for a NORMAL peer before the node tries the lease, in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub bootstrap_deadline_ms: u64,
    /// The gossip protocol period: one probe per period, in milliseconds.
    #[arg(long, default_value_t = 1_000)]
    pub gossip_interval_ms: u64,
    /// Time to wait for a direct probe ack, in milliseconds.
    #[arg(long, default_value_t = 500)]
    pub gossip_probe_timeout_ms: u64,
    /// Members asked to probe a target that missed a direct probe.
    #[arg(long, default_value_t = 3)]
    pub gossip_indirect_probes: usize,
    /// The detector score that makes a member suspect.
    #[arg(long, default_value_t = 8.0)]
    pub phi_suspect: f64,
    /// The detector score that makes a suspect member dead after the minimum suspicion time.
    #[arg(long, default_value_t = 16.0)]
    pub phi_dead: f64,
    /// Detector evidence of one data-path failure. One failed probe adds `--phi-suspect`.
    #[arg(long, default_value_t = 10.0)]
    pub data_path_weight: f64,
    /// The suspicion timeout once enough nodes confirm a suspicion, in milliseconds. No suspect
    /// becomes dead sooner.
    #[arg(long, default_value_t = 3_000)]
    pub suspicion_min_ms: u64,
    /// The suspicion timeout without confirmations, in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub suspicion_max_ms: u64,
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
    /// Read from any owner, with this node first when it owns the key (spec section 7). With
    /// `false`, every read goes to the primary.
    #[arg(long, default_value_t = true, num_args = 0..=1, default_missing_value = "true",
          action = clap::ArgAction::Set)]
    pub read_from_replicas: bool,
    /// Keys that wait in the replication queue of one peer. Past it, the oldest go to hints.
    #[arg(long, default_value_t = 65_536)]
    pub replication_queue_keys: usize,
    /// Keys in one replication batch.
    #[arg(long, default_value_t = 512)]
    pub replication_batch_keys: usize,
    /// Time for a peer to acknowledge a replication batch, in milliseconds.
    #[arg(long, default_value_t = 1_000)]
    pub replication_timeout_ms: u64,
    /// Time that a write to a failed primary waits for a new primary, in milliseconds. Then it
    /// returns `-TRYAGAIN`.
    #[arg(long, default_value_t = 3_000)]
    pub hold_budget_ms: u64,
    /// Keys in the write set of one connection, at most.
    #[arg(long, default_value_t = 4_096)]
    pub write_set_keys: usize,
    /// Bytes in the write set of one connection, at most.
    #[arg(long, default_value_t = 1_048_576)]
    pub write_set_bytes: usize,
    /// The share of the memory budget for the write sets of every connection, in percent.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..=100))]
    pub write_set_memory_percent: u64,
    /// Time that a written key stays in the write set, in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    pub write_set_ttl_ms: u64,
    /// Keys with this prefix are strong counter keys. They never enter a write set.
    #[arg(long, default_value = "cnt:")]
    pub counter_prefix: String,
    /// Bytes per second that one handoff scan sends at most.
    #[arg(long, default_value_t = 33_554_432)]
    pub handoff_rate_bytes: u64,
    /// Time after which a node gives up on an incoming handoff, in milliseconds. Its
    /// previous-epoch fallback ends then too.
    #[arg(long, default_value_t = 300_000)]
    pub handoff_timeout_ms: u64,
    /// Hand entries to nodes that gain ownership. Off only for comparisons in tests.
    #[arg(long, hide = true, default_value_t = true, action = clap::ArgAction::Set)]
    pub handoff: bool,
    /// Hints held for one unreachable peer, at most.
    #[arg(long, default_value_t = 262_144)]
    pub hint_max_entries: usize,
    /// Bytes of hints held for one unreachable peer, at most.
    #[arg(long, default_value_t = 67_108_864)]
    pub hint_max_bytes: u64,
    /// The share of the memory budget for the hints of every peer, in percent.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=100))]
    pub hint_memory_percent: u64,
    /// Time between probes of a peer that missed a batch, in milliseconds.
    #[arg(long, default_value_t = 1_000)]
    pub replication_retry_ms: u64,
    /// Capability bits offered in HELLO. For tests of mixed builds.
    #[arg(long, hide = true, default_value_t = discus_wire::caps::KNOWN)]
    pub cluster_capabilities: u64,
    /// Security flags (spec §14).
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

    /// True when the node runs SWIM gossip: the transport runs in gossip mode.
    pub fn gossip_enabled(&self) -> bool {
        self.cluster_enabled() && self.membership == MembershipMode::Gossip
    }

    /// The SWIM settings from the flags.
    pub fn swim(&self) -> discus_membership::SwimConfig {
        discus_membership::SwimConfig {
            period_ms: self.gossip_interval_ms,
            probe_timeout_ms: self.gossip_probe_timeout_ms,
            indirect_probes: self.gossip_indirect_probes,
            phi_suspect: self.phi_suspect,
            phi_dead: self.phi_dead,
            data_path_weight: self.data_path_weight,
            missed_ping_weight: self.phi_suspect,
            suspicion_min_ms: self.suspicion_min_ms,
            suspicion_max_ms: self.suspicion_max_ms,
            bootstrap_deadline_ms: self.bootstrap_deadline_ms,
            replication_factor: self.replication_factor,
            ..discus_membership::SwimConfig::default()
        }
    }

    /// Check the flags that depend on each other. The message names the flags to fix.
    pub fn validate(&self) -> Result<(), String> {
        let certs = self.cert_flags();
        let set = certs.iter().filter(|(_, s)| *s).count();
        if self.gossip_enabled() {
            self.swim().validate()?;
            if self.lease_provider == LeaseKind::K8s && !cfg!(feature = "k8s") {
                return Err("--lease-provider k8s needs the k8s build feature. \
                    Use --lease-provider none outside Kubernetes"
                    .into());
            }
        }
        if !self.peers.is_empty() && self.membership == MembershipMode::Gossip {
            let missing: Vec<&str> = certs.iter().filter(|(_, s)| !*s).map(|(f, _)| *f).collect();
            if !missing.is_empty() {
                return Err(format!(
                    "--peer needs {}. Node-to-node traffic always uses mTLS",
                    missing.join(", ")
                ));
            }
        } else if !self.peers.is_empty() {
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
                let Some(id) = p.id else {
                    return Err(format!(
                        "--peer {} needs a node ID as <node-id>@<address> with --membership static",
                        p.addr
                    ));
                };
                if Some(id) == self.node_id {
                    return Err(format!("--peer {id} is this node's own ID"));
                }
                if self.peers[..i].iter().any(|q| q.id == Some(id)) {
                    return Err(format!("--peer {id} is listed twice"));
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

    pub fn hold_budget(&self) -> Duration {
        Duration::from_millis(self.hold_budget_ms)
    }
}

fn parse_peer(s: &str) -> Result<Peer, String> {
    let (id, addr) = match s.split_once('@') {
        Some((id, addr)) => (Some(id), addr),
        None => (None, s),
    };
    let id = id
        .map(|id| id.parse::<NodeId>())
        .transpose()
        .map_err(|_| "a peer node ID is 32 hex digits".to_string())?;
    let addr = addr
        .parse::<SocketAddr>()
        .map_err(|_| "a peer is <ip>:<port> or <node-id>@<ip>:<port>".to_string())?;
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
