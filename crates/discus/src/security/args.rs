//! Security flags. `Config` flattens them, so they read like every other flag.

use std::path::PathBuf;

use clap::{Args, ValueEnum};

/// The client listener TLS mode (spec §14.1). Every client uses TLS: the node or the load
/// balancer terminates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TlsMode {
    /// The node terminates TLS and optional client mTLS.
    Node,
    /// The load balancer terminates TLS and forwards the client identity with PROXY protocol,
    /// over mTLS.
    Lb,
}

impl TlsMode {
    pub fn name(self) -> &'static str {
        match self {
            TlsMode::Node => "node",
            TlsMode::Lb => "lb",
        }
    }
}

/// Whether the node mode asks for a client certificate or requires one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClientAuth {
    Optional,
    Required,
}

fn parse_tlv(s: &str) -> Result<u8, String> {
    let v = match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        Some(hex) => u8::from_str_radix(hex, 16),
        None => s.parse(),
    }
    .map_err(|e| e.to_string())?;
    if (0xE0..=0xEF).contains(&v) {
        Ok(v)
    } else {
        Err("use a custom PROXY v2 TLV type from 0xE0 to 0xEF".into())
    }
}

/// Client TLS and authentication settings.
#[derive(Debug, Clone, Args)]
pub struct SecurityArgs {
    /// Client listener TLS mode: node or lb.
    #[arg(long, value_enum, default_value = "node")]
    pub tls_mode: TlsMode,
    /// PEM certificate chain of the client listener.
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    /// PEM private key of the client listener.
    #[arg(long)]
    pub tls_key: Option<PathBuf>,
    /// PEM CA that signs client certificates (node mode) or the load balancer certificate
    /// (lb mode).
    #[arg(long)]
    pub tls_client_ca: Option<PathBuf>,
    /// In node mode, whether a client certificate is optional or required.
    #[arg(long, value_enum, default_value = "optional")]
    pub tls_client_auth: ClientAuth,
    /// In lb mode, a network that the load balancer connects from. Repeatable.
    #[arg(long)]
    pub lb_trusted_source: Vec<ipnet::IpNet>,
    /// In lb mode, a custom PROXY v2 TLV type that carries the client identity.
    #[arg(long, value_parser = parse_tlv)]
    pub lb_identity_tlv: Option<u8>,
    /// Client authentication policy: none, any:<factors>, or all:<factors>. The default is none, or
    /// all:tls,shared_token with --shared-token.
    #[arg(long)]
    pub auth_policy: Option<String>,
    /// TOML file with trusted issuers, certificate rules, and admin identities.
    #[arg(long)]
    pub auth_config: Option<PathBuf>,
    /// Allow http issuers and admin bearer tokens over a plaintext admin port. Development only.
    #[arg(long)]
    pub dev_insecure_oauth2: bool,
    /// Clock skew allowed on token times, in seconds.
    #[arg(long, default_value_t = 60)]
    pub auth_clock_skew_secs: u64,
    /// Failed authentication attempts that close a connection.
    /// Also `[auth] max-failures-per-connection` in the --config file.
    #[arg(long, default_value_t = 5)]
    pub auth_max_failures_per_connection: u32,
    /// Failed authentication attempts per source address in one window.
    /// Also `[auth] max-failures-per-source` in the --config file.
    #[arg(long, default_value_t = 20)]
    pub auth_max_failures_per_source: u32,
    /// The failed-attempt window per source address, in seconds.
    /// Also `[auth] failure-window-secs` in the --config file.
    #[arg(long, default_value_t = 60)]
    pub auth_failure_window_secs: u64,
    /// Serve the admin port over TLS with the client listener certificate and CA.
    #[arg(long)]
    pub admin_tls: bool,
}
