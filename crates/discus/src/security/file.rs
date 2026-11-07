//! The TOML file named by `--auth-config`.
//!
//! The file parses in every build. A section that needs a missing build feature makes the node
//! refuse to start with the name of the feature.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Lists that do not fit in flags.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthFile {
    /// Scopes per client certificate identity. The first match wins.
    #[serde(default)]
    pub client_cert: Vec<CertRule>,
    /// Trusted OAuth 2.0 issuers.
    #[serde(default)]
    pub issuer: Vec<IssuerFile>,
}

/// One client certificate rule.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertRule {
    /// The identity pattern. A trailing `*` matches any suffix.
    pub identity: String,
    pub scopes: Vec<String>,
}

fn d300() -> u64 {
    300
}
fn d30() -> u64 {
    30
}
fn d3600() -> u64 {
    3600
}

/// One trusted OAuth 2.0 issuer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssuerFile {
    /// The exact `iss` value.
    pub issuer: String,
    /// The audience that tokens must name.
    pub audience: String,
    /// Default: `<issuer>/.well-known/openid-configuration`.
    pub discovery_url: Option<String>,
    /// A PEM CA for this issuer. Default: the trust root feature.
    pub ca_file: Option<PathBuf>,
    /// Default: `["RS256", "ES256"]`.
    pub algorithms: Option<Vec<String>>,
    /// Removed from each token scope before it maps to a Discus scope.
    #[serde(default)]
    pub scope_prefix: String,
    #[serde(default = "d300")]
    pub jwks_refresh_secs: u64,
    #[serde(default = "d30")]
    pub jwks_min_refetch_secs: u64,
    #[serde(default = "d3600")]
    pub jwks_max_stale_secs: u64,
    /// Scopes per `sub` pattern. When present, they replace the `scope` claim.
    #[serde(default)]
    pub subject: Vec<SubjectFile>,
}

/// A `sub` pattern and its scopes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectFile {
    #[serde(rename = "match")]
    pub pattern: String,
    pub scopes: Vec<String>,
}

impl AuthFile {
    pub fn load(path: &Path) -> Result<AuthFile, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("--auth-config {}: {e}", path.display()))?;
        toml::from_str(&text).map_err(|e| format!("--auth-config {}: {e}", path.display()))
    }
}
