//! Node configuration, parsed from command-line flags.

use std::net::SocketAddr;

use clap::Parser;

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
    /// Shared token that clients must send with AUTH.
    #[arg(long)]
    pub shared_token: Option<String>,
    #[command(flatten)]
    pub security: crate::security::SecurityArgs,
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
}
