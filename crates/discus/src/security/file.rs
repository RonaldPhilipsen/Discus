//! The TOML file named by `--auth-config`.

use std::path::Path;

use serde::Deserialize;

/// Lists that do not fit in flags.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthFile {}

impl AuthFile {
    pub fn load(path: &Path) -> Result<AuthFile, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("--auth-config {}: {e}", path.display()))?;
        toml::from_str(&text).map_err(|e| format!("--auth-config {}: {e}", path.display()))
    }
}
