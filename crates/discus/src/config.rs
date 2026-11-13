//! Node configuration, parsed from command-line flags.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use discus_placement::{ClusterId, NodeId};

/// A peer: `<addr>` or `<node-id>@<addr>`, with an optional `/<client-addr>` after the address.
/// With gossip it is a discovery seed, and the ID is only an expectation. With static membership
/// the ID is required, and the client address is the one that the member advertises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    pub id: Option<NodeId>,
    pub addr: SocketAddr,
    pub client_addr: Option<SocketAddr>,
}

/// Whether the node answers the cluster topology commands (spec section "Client listeners").
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TopologyMode {
    /// Answer when the node has a redirect listener.
    Auto,
    /// Always answer. Without a redirect listener the replies name the `--listen` addresses.
    On,
    /// Never answer.
    Off,
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
    /// Redirect listener address: a second RESP listener that answers `-MOVED` for keys that
    /// another node serves. Off by default.
    #[arg(long)]
    pub redirect_listen: Option<SocketAddr>,
    /// The client address that topology replies and `-MOVED` name. Default: the IP of the
    /// cluster address with the bound `--redirect-listen` port, else the bound `--listen` port.
    #[arg(long)]
    pub client_advertise: Option<SocketAddr>,
    /// A host name template for topology replies and `-MOVED`. `{ip}` becomes the advertised IP
    /// with dashes. An empty value means no host name.
    #[arg(long)]
    pub client_hostname: Option<String>,
    /// Whether the topology commands answer: `auto` (with a redirect listener), `on`, or `off`.
    #[arg(long, value_enum, default_value_t = TopologyMode::Auto)]
    pub cluster_topology: TopologyMode,
    /// Admin (probes) listen address.
    #[arg(long, default_value = "0.0.0.0:9100")]
    pub admin_listen: SocketAddr,
    /// Number of store shards.
    #[arg(long, default_value_t = 256)]
    pub shards: usize,
    /// Memory budget in bytes. Default: 75% of the container memory limit, or 1 GiB without a
    /// limit.
    #[arg(long, default_value_t = default_max_memory(), hide_default_value = true)]
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
    /// Shared token that clients must send with AUTH. The environment variable keeps the token
    /// out of the process arguments.
    #[arg(long, env = "DISCUS_SHARED_TOKEN", hide_env_values = true)]
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
    /// Node-to-node RPC (QUIC, UDP) listen address, with gossip membership. With static
    /// membership RPC shares `--cluster-listen`.
    #[arg(long, default_value = "0.0.0.0:7380")]
    pub rpc_listen: SocketAddr,
    /// The RPC address that other members use. Default: the IP of the cluster advertise address
    /// with the bound `--rpc-listen` port.
    #[arg(long)]
    pub rpc_advertise: Option<SocketAddr>,
    /// Raise the nice value of the data runtime threads by this much over the process value,
    /// from 0 to 19. The result is capped at 19. The control thread keeps the process value, so
    /// it wins CPU under a quota. 0 changes nothing. Linux only.
    #[arg(long, default_value_t = 5)]
    pub data_nice: i32,
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
    /// The URI SAN that every peer certificate must carry. Empty: any certificate from the CA.
    #[arg(long, default_value = "spiffe://cluster.local/ns/default/sa/discus")]
    pub cluster_san: String,
    /// Forward timeout for reads, in milliseconds. Then the next owner is tried.
    #[arg(long, default_value_t = 500)]
    pub forward_read_timeout_ms: u64,
    /// Forward timeout for writes, in milliseconds.
    #[arg(long, default_value_t = 2000)]
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
    /// Keys with this prefix are strong counter keys. Counter groups serve them linearizably.
    #[arg(long, default_value = "cnt:")]
    pub counter_prefix: String,
    /// Keys with this prefix read from the primary only and never enter a write set. A counter
    /// key stays a counter key. Repeat the flag for more prefixes.
    #[arg(long)]
    pub primary_read_prefix: Vec<String>,
    /// Counter groups (spec section 10). A change is an offline re-shard.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub counter_groups: u16,
    /// The share of the memory budget for counter state, in percent. Past it, counter writes
    /// return `-OOM`.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=100))]
    pub counter_memory_percent: u64,
    /// Time that a counter command waits for a leader, a quorum, or a commit, in milliseconds.
    /// Then it returns `-TRYAGAIN`.
    #[arg(long, default_value_t = 1_000)]
    pub counter_timeout_ms: u64,
    /// The name prefix of the counter group leases. The lease of group `g` is `<prefix>-<g>`.
    /// Every member of a cluster uses the same value, and each cluster in a namespace its own.
    #[arg(long, default_value = "discus-counter")]
    pub counter_lease_prefix: String,
    /// The duration of a counter group lease, in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    pub counter_lease_duration_ms: u64,
    /// The renewal interval of a counter group lease, in milliseconds.
    #[arg(long, default_value_t = 1_000)]
    pub counter_lease_renew_ms: u64,
    /// A leader stops serving this long before its lease ends by its own clock, in milliseconds.
    #[arg(long, default_value_t = 1_000)]
    pub counter_lease_margin_ms: u64,
    /// Time that a leader-follower leader waits for the follower ack of a write, in
    /// milliseconds. Then it replies anyway and marks the follower lagging.
    #[arg(long, default_value_t = 50)]
    pub counter_follower_timeout_ms: u64,
    /// Time with three `NORMAL` members before the counter groups switch to Raft, in
    /// milliseconds.
    #[arg(long, default_value_t = 30_000)]
    pub counter_dwell_ms: u64,
    /// The Raft heartbeat interval of the counter groups, in milliseconds.
    #[arg(long, default_value_t = 100)]
    pub raft_heartbeat_ms: u64,
    /// The shortest Raft election timeout, in milliseconds. The longest is twice that.
    #[arg(long, default_value_t = 1_000)]
    pub raft_election_timeout_ms: u64,
    /// Log entries between two Raft snapshots of a counter group.
    #[arg(long, default_value_t = 10_000)]
    pub raft_snapshot_entries: u64,
    /// A learner within this many log entries of the leader becomes a voter.
    #[arg(long, default_value_t = 100)]
    pub raft_catchup_entries: u64,
    /// The leader removes a voter that has not answered for this long, in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub raft_voter_timeout_ms: u64,
    /// How long the startup probe waits for the counter tier to settle after the node turns
    /// `NORMAL`, in milliseconds. Keep it below the startup probe budget.
    #[arg(long, default_value_t = 45_000)]
    pub counter_settle_timeout_ms: u64,
    /// Bytes per second that one handoff scan sends at most.
    #[arg(long, default_value_t = 33_554_432)]
    pub handoff_rate_bytes: u64,
    /// Time after which a node gives up on an incoming handoff, in milliseconds. Its
    /// previous-epoch fallback ends then too.
    #[arg(long, default_value_t = 300_000)]
    pub handoff_timeout_ms: u64,
    /// Hand entries to nodes that gain ownership. Off only for comparisons in tests.
    #[cfg(feature = "testing")]
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
    /// Security flags (spec §14).
    #[command(flatten)]
    pub security: crate::security::SecurityArgs,
    /// Telemetry flags (spec §15.4).
    #[command(flatten)]
    pub telemetry: crate::telemetry::TelemetryArgs,
    /// Profiling flags.
    #[command(flatten)]
    pub profiling: crate::profiling::ProfilingArgs,
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

    /// True when the topology commands answer.
    pub fn topology_enabled(&self) -> bool {
        match self.cluster_topology {
            TopologyMode::On => true,
            TopologyMode::Auto => self.redirect_listen.is_some(),
            TopologyMode::Off => false,
        }
    }

    /// The name of the lease of counter group `g`.
    pub fn counter_lease_name(&self, g: u16) -> String {
        format!("{}-{g}", self.counter_lease_prefix)
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
            ..discus_membership::SwimConfig::default()
        }
    }

    /// Check the flags that depend on each other. The message names the flags to fix.
    pub fn validate(&self) -> Result<(), String> {
        if self.primary_read_prefix.iter().any(String::is_empty) {
            return Err("--primary-read-prefix needs a non-empty value".into());
        }
        let certs = self.cert_flags();
        let missing_certs: Vec<&str> = certs.iter().filter(|(_, s)| !*s).map(|(f, _)| *f).collect();
        let set = certs.len() - missing_certs.len();
        let peer_needs = |missing: &[&str]| {
            if missing.is_empty() {
                return Ok(());
            }
            Err(format!(
                "--peer needs {}. Node-to-node traffic always uses mTLS",
                missing.join(", ")
            ))
        };
        if !(0..=19).contains(&self.data_nice) {
            return Err("--data-nice must be from 0 to 19".into());
        }
        if self.redirect_listen.is_some() && self.security.tls_mode == crate::security::TlsMode::Lb
        {
            return Err("--redirect-listen cannot be used with --tls-mode lb. \
                Clients reach the nodes only through the load balancer, so they cannot route"
                .into());
        }
        if self.gossip_enabled() {
            self.swim().validate()?;
            if self.lease_provider == LeaseKind::K8s && !cfg!(feature = "k8s") {
                return Err("--lease-provider k8s needs the k8s build feature. \
                    Use --lease-provider none outside Kubernetes"
                    .into());
            }
        }
        if !self.peers.is_empty() && self.membership == MembershipMode::Gossip {
            if self.peers.iter().any(|p| p.client_addr.is_some()) {
                return Err(
                    "a /<client-ip>:<client-port> suffix on --peer needs --membership \
                    static. Gossip members announce their own client address"
                        .into(),
                );
            }
            peer_needs(&missing_certs)?;
        } else if !self.peers.is_empty() {
            let mut missing: Vec<&str> = Vec::new();
            if self.node_id.is_none() {
                missing.push("--node-id");
            }
            if self.cluster_id.is_none() {
                missing.push("--cluster-id");
            }
            missing.extend(&missing_certs);
            peer_needs(&missing)?;
            let suffixed = self
                .peers
                .iter()
                .filter(|p| p.client_addr.is_some())
                .count();
            let consistent = if suffixed == 0 {
                self.client_advertise.is_none()
            } else {
                suffixed == self.peers.len() && self.client_advertise.is_some()
            };
            if !consistent {
                return Err("with --membership static, either every --peer has a \
                    /<client-ip>:<client-port> suffix and --client-advertise is set, or no --peer \
                    has a suffix and --client-advertise is unset. Every node must build the same \
                    member list"
                    .into());
            }
            let routes =
                self.redirect_listen.is_some() || self.cluster_topology == TopologyMode::On;
            if routes && suffixed == 0 {
                return Err(
                    "--redirect-listen and --cluster-topology on need client addresses \
                    with --membership static. Set --client-advertise and give every --peer a \
                    /<client-ip>:<client-port> suffix"
                        .into(),
                );
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
        } else if set != 0 && !missing_certs.is_empty() {
            return Err(format!(
                "the cluster certificate flags need {}",
                missing_certs.join(", ")
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

    /// The RESP parser limits of a client connection and of a forwarded command. The bulk limit
    /// is looser than the value limit so a moderately oversize value gets a clean `value too
    /// large` error. A hugely oversize one still closes the connection.
    pub fn parser_limits(&self) -> discus_resp::Limits {
        discus_resp::Limits {
            max_bulk_bytes: self
                .max_value_bytes
                .max(self.max_key_bytes)
                .saturating_add(1 << 20),
            ..discus_resp::Limits::default()
        }
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
    let bad = || {
        "a peer is <ip>:<port>, <node-id>@<ip>:<port>, or <node-id>@<ip>:<port>/<client-ip>:<client-port>"
            .to_string()
    };
    let (addr, client_addr) = match addr.split_once('/') {
        Some((addr, client)) => (addr, Some(client.parse::<SocketAddr>().map_err(|_| bad())?)),
        None => (addr, None),
    };
    let addr = addr.parse::<SocketAddr>().map_err(|_| bad())?;
    Ok(Peer {
        id,
        addr,
        client_addr,
    })
}

/// The host name for `ip` from a `--client-hostname` template: `{ip}` becomes the IP with `.`
/// and `:` replaced by `-`.
pub fn client_host(template: &str, ip: IpAddr) -> String {
    template.replace("{ip}", &ip.to_string().replace(['.', ':'], "-"))
}

/// The client address that a node advertises when `--client-advertise` is not set: the cluster
/// IP with the port of the bound redirect listener, else of the bound `--listen`.
pub fn default_client_addr(
    cluster_ip: IpAddr,
    redirect: Option<SocketAddr>,
    listen: SocketAddr,
) -> SocketAddr {
    SocketAddr::new(cluster_ip, redirect.unwrap_or(listen).port())
}

fn parse_rf(s: &str) -> Result<usize, String> {
    match s.parse::<usize>() {
        Ok(n) if n >= 1 => Ok(n),
        _ => Err("the replication factor is a whole number of at least 1".into()),
    }
}

fn default_max_memory() -> u64 {
    let limit = sysinfo::System::new()
        .ok()
        .and_then(|system| system.cgroup_limits())
        .map(|limits| limits.total_memory);
    budget_for_limit(limit)
}

/// The rest of the limit is for what the budget does not count: connection buffers, replication,
/// and allocator overhead. Past the limit, the kernel kills the process.
fn budget_for_limit(limit: Option<u64>) -> u64 {
    limit.map_or(1 << 30, |bytes| bytes / 4 * 3)
}

fn parse_weight(s: &str) -> Result<f64, String> {
    s.parse::<f64>()
        .ok()
        .and_then(discus_placement::Weight::new)
        .map(|w| w.get())
        .ok_or_else(|| "the weight is a finite number greater than zero".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_is_three_quarters_of_the_limit() {
        assert_eq!(budget_for_limit(Some(4 << 30)), 3 << 30);
    }

    #[test]
    fn the_budget_is_one_gibibyte_without_a_limit() {
        assert_eq!(budget_for_limit(None), 1 << 30);
    }

    #[test]
    fn an_explicit_max_memory_wins() {
        let config = Config::parse_from(["discus", "--max-memory", "4096"]).unwrap();
        assert_eq!(config.max_memory, 4096);
    }

    #[test]
    fn counter_lease_name_defaults_to_the_documented_name() {
        let config = Config::parse_from(["discus"]).unwrap();
        assert_eq!(config.counter_lease_name(3), "discus-counter-3");
    }

    #[test]
    fn counter_lease_name_uses_the_prefix() {
        let config =
            Config::parse_from(["discus", "--counter-lease-prefix", "blue-counter"]).unwrap();
        assert_eq!(config.counter_lease_name(0), "blue-counter-0");
    }

    #[test]
    fn data_nice_defaults_to_5() {
        assert_eq!(Config::parse_from(["discus"]).unwrap().data_nice, 5);
    }

    #[test]
    fn data_nice_above_19_is_rejected() {
        let config = Config::parse_from(["discus", "--data-nice", "20"]).unwrap();
        assert!(config.validate().unwrap_err().contains("--data-nice"));
    }

    #[test]
    fn data_nice_zero_and_19_are_accepted() {
        for v in ["0", "19"] {
            Config::parse_from(["discus", "--data-nice", v])
                .unwrap()
                .validate()
                .unwrap();
        }
    }

    #[test]
    fn forward_timeouts_default_to_500_ms_and_2_s() {
        let config = Config::parse_from(["discus"]).unwrap();
        assert_eq!(config.forward_read_timeout_ms, 500);
        assert_eq!(config.forward_write_timeout_ms, 2000);
    }

    #[test]
    fn redirect_listen_with_lb_tls_is_refused() {
        let config = Config::parse_from([
            "discus",
            "--redirect-listen",
            "0.0.0.0:6380",
            "--tls-mode",
            "lb",
        ])
        .unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("--redirect-listen") && err.contains("--tls-mode lb"));
    }

    #[test]
    fn peer_takes_a_client_address() {
        let id = NodeId::random();
        let peer = parse_peer(&format!("{id}@10.0.0.1:7379/10.0.0.1:6380")).unwrap();
        assert_eq!(peer.id, Some(id));
        assert_eq!(peer.addr, "10.0.0.1:7379".parse().unwrap());
        assert_eq!(peer.client_addr, Some("10.0.0.1:6380".parse().unwrap()));
        assert_eq!(parse_peer("10.0.0.1:7379").unwrap().client_addr, None);
        assert!(parse_peer("10.0.0.1:7379/nope").is_err());
    }

    #[test]
    fn client_host_dashes_the_ip() {
        let ip = "10.1.2.3".parse().unwrap();
        assert_eq!(client_host("{ip}.peers", ip), "10-1-2-3.peers");
        let v6 = "fd00::1".parse().unwrap();
        assert_eq!(client_host("{ip}.peers", v6), "fd00--1.peers");
    }

    #[test]
    fn topology_is_on_for_auto_with_a_redirect_listener() {
        let off = Config::parse_from(["discus"]).unwrap();
        assert!(!off.topology_enabled());
        let auto = Config::parse_from(["discus", "--redirect-listen", "0.0.0.0:6380"]).unwrap();
        assert!(auto.topology_enabled());
        let on = Config::parse_from(["discus", "--cluster-topology", "on"]).unwrap();
        assert!(on.topology_enabled());
        let forced = Config::parse_from([
            "discus",
            "--redirect-listen",
            "0.0.0.0:6380",
            "--cluster-topology",
            "off",
        ])
        .unwrap();
        assert!(!forced.topology_enabled());
    }

    #[test]
    fn default_client_addr_uses_the_cluster_ip_and_bound_port() {
        let ip = "10.0.0.5".parse().unwrap();
        let listen = "0.0.0.0:6379".parse().unwrap();
        let redirect = Some("0.0.0.0:41000".parse().unwrap());
        assert_eq!(
            default_client_addr(ip, redirect, listen),
            "10.0.0.5:41000".parse().unwrap()
        );
        assert_eq!(
            default_client_addr(ip, None, listen),
            "10.0.0.5:6379".parse().unwrap()
        );
    }

    fn static_args(extra: &[&str]) -> Vec<String> {
        let mut argv: Vec<String> = [
            "discus",
            "--membership",
            "static",
            "--cluster-ca",
            "ca",
            "--cluster-cert",
            "cert",
            "--cluster-key",
            "key",
            "--node-id",
            &NodeId::random().to_string(),
            "--cluster-id",
            &ClusterId::mint().to_string(),
        ]
        .map(String::from)
        .to_vec();
        argv.extend(extra.iter().map(|s| s.to_string()));
        argv
    }

    #[test]
    fn static_client_addresses_are_all_set_or_all_unset() {
        let (a, b) = (NodeId::random(), NodeId::random());
        let plain = [format!("{a}@10.0.0.1:7379"), format!("{b}@10.0.0.2:7379")];
        let suffixed = [
            format!("{a}@10.0.0.1:7379/10.0.0.1:6380"),
            format!("{b}@10.0.0.2:7379/10.0.0.2:6380"),
        ];
        let check = |peers: &[String], advertise: bool| {
            let mut extra: Vec<&str> = Vec::new();
            for p in peers {
                extra.extend(["--peer", p]);
            }
            if advertise {
                extra.extend(["--client-advertise", "10.0.0.3:6380"]);
            }
            Config::parse_from(static_args(&extra)).unwrap().validate()
        };
        check(&plain, false).unwrap();
        check(&suffixed, true).unwrap();
        let mixed = [plain[0].clone(), suffixed[1].clone()];
        for bad in [
            check(&mixed, true),
            check(&mixed, false),
            check(&suffixed, false),
            check(&plain, true),
        ] {
            let err = bad.unwrap_err();
            assert!(err.contains("--peer") && err.contains("--client-advertise"));
        }
    }

    #[test]
    fn a_gossip_peer_takes_no_client_address() {
        let a = NodeId::random();
        let peer = format!("{a}@10.0.0.1:7379/10.0.0.1:6380");
        let argv = [
            "discus",
            "--cluster-ca",
            "ca",
            "--cluster-cert",
            "cert",
            "--cluster-key",
            "key",
            "--peer",
            &peer,
        ];
        let err = Config::parse_from(argv).unwrap().validate().unwrap_err();
        assert!(err.contains("--membership static"), "{err}");
        let plain = argv.map(|s| s.split_once('/').map_or(s, |(p, _)| p));
        Config::parse_from(plain).unwrap().validate().unwrap();
    }

    #[test]
    fn static_routing_needs_client_addresses() {
        let (a, b) = (NodeId::random(), NodeId::random());
        let (pa, pb) = (format!("{a}@10.0.0.1:7379"), format!("{b}@10.0.0.2:7379"));
        let peers = ["--peer", pa.as_str(), "--peer", pb.as_str()];
        let check = |extra: &[&str]| {
            let mut argv = peers.to_vec();
            argv.extend(extra);
            Config::parse_from(static_args(&argv)).unwrap().validate()
        };
        check(&[]).unwrap();
        check(&["--cluster-topology", "off"]).unwrap();
        for bad in [
            check(&["--redirect-listen", "0.0.0.0:6380"]),
            check(&["--cluster-topology", "on"]),
        ] {
            let err = bad.unwrap_err();
            assert!(err.contains("--client-advertise"), "{err}");
        }
    }
}
