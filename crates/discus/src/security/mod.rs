//! Client-side security: TLS, authentication, and authorization (spec §14).
//!
//! `Security` owns every decision. The server calls it at accept time, the `AUTH` and `HELLO`
//! handlers call `authenticate`, and the dispatcher calls `authorize` before each command.

pub mod args;
pub mod file;
pub mod limiter;
pub mod policy;
pub mod session;
pub mod tls;

use std::net::IpAddr;
use std::time::Duration;

use subtle::ConstantTimeEq;

pub use args::{ClientAuth, SecurityArgs, TlsMode};
pub use file::AuthFile;
pub use policy::{Factor, Policy, Scopes};
pub use session::{CertGrant, ConnAuth, TokenGrant};

use crate::command::access::{access, Access};
use crate::command::CommandError;
use crate::node::ConnState;
use crate::Config;
use limiter::FailureLimiter;

/// A configuration that the node refuses to start with. The text names the flag or the feature.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SecurityError(pub String);

impl From<String> for SecurityError {
    fn from(s: String) -> SecurityError {
        SecurityError(s)
    }
}

fn fail<T>(msg: impl Into<String>) -> Result<T, SecurityError> {
    Err(SecurityError(msg.into()))
}

/// The security state of a node.
pub struct Security {
    mode: TlsMode,
    policy: Policy,
    shared_token: Option<String>,
    max_conn_failures: u32,
    limiter: FailureLimiter,
}

impl Security {
    /// Check the configuration and build the security state.
    pub fn from_config(config: &Config) -> Result<Security, SecurityError> {
        let args = &config.security;
        let _file = match &args.auth_config {
            Some(path) => AuthFile::load(path)?,
            None => AuthFile::default(),
        };
        let tls_on = args.tls_mode != TlsMode::Off;
        let policy = match &args.auth_policy {
            Some(p) => Policy::parse(p)?,
            None if tls_on => Policy::All(vec![Factor::Tls, Factor::Oauth2]),
            None if config.shared_token.is_some() => Policy::Any(vec![Factor::SharedToken]),
            None => Policy::None,
        };

        if tls_on && (args.tls_cert.is_none() || args.tls_key.is_none()) {
            return fail(format!(
                "--tls-mode {} needs --tls-cert and --tls-key",
                args.tls_mode.name()
            ));
        }
        for f in [Factor::Tls, Factor::Mtls] {
            if policy.uses(f) && !tls_on {
                return fail(format!(
                    "the policy uses {}, so set --tls-mode to node or lb",
                    f.name()
                ));
            }
        }
        if policy.uses(Factor::Mtls) && args.tls_client_ca.is_none() {
            return fail("the policy uses mtls, so set --tls-client-ca");
        }
        match (policy.uses(Factor::SharedToken), &config.shared_token) {
            (true, None) => return fail("the policy uses shared_token, so set --shared-token"),
            (false, Some(_)) => {
                return fail(format!(
                    "--shared-token is set, but the policy {policy} does not use shared_token"
                ))
            }
            _ => {}
        }
        if policy.uses(Factor::Oauth2) {
            Self::check_oauth2(args, tls_on)?;
        }

        Ok(Security {
            mode: args.tls_mode,
            policy,
            shared_token: config.shared_token.clone(),
            max_conn_failures: args.auth_max_failures_per_connection.max(1),
            limiter: FailureLimiter::new(
                args.auth_max_failures_per_source.max(1),
                Duration::from_secs(args.auth_failure_window_secs),
            ),
        })
    }

    #[cfg(not(feature = "oauth2"))]
    fn check_oauth2(_: &SecurityArgs, _: bool) -> Result<(), SecurityError> {
        fail("the policy uses oauth2, but this binary was built without the \"oauth2\" feature")
    }

    #[cfg(feature = "oauth2")]
    fn check_oauth2(args: &SecurityArgs, tls_on: bool) -> Result<(), SecurityError> {
        if !tls_on && !args.dev_insecure_oauth2 {
            return fail(
                "the policy uses oauth2, which needs TLS: set --tls-mode, or set \
                 --dev-insecure-oauth2 for development",
            );
        }
        fail("the policy uses oauth2, but --auth-config lists no trusted issuer")
    }

    pub fn mode(&self) -> TlsMode {
        self.mode
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// The state of a new plaintext connection.
    pub fn plain_conn(&self, peer: Option<IpAddr>) -> ConnAuth {
        ConnAuth::new(peer)
    }

    /// True when the connection meets the policy.
    pub fn is_authenticated(&self, auth: &ConnAuth) -> bool {
        auth.scopes(&self.policy).is_some()
    }

    /// Handle the credentials of `AUTH` or `HELLO ... AUTH`. `user` is only a hint, except that
    /// `OAUTH2` selects OAuth 2.0.
    pub fn authenticate(
        &self,
        conn: &mut ConnState,
        user: Option<&[u8]>,
        token: &[u8],
    ) -> Result<(), CommandError> {
        if !self.policy.uses(Factor::SharedToken) && !self.policy.uses(Factor::Oauth2) {
            return Err(CommandError::Custom(
                "ERR AUTH <password> called without any password configured for the default \
                 user. Are you sure your configuration is correct?"
                    .into(),
            ));
        }
        if let Some(ip) = conn.auth.peer {
            if self.limiter.blocked(ip) {
                return Err(self.failed(conn, "the source is over its failed-attempt limit"));
            }
        }
        let oauth2_only = user.is_some_and(|u| u.eq_ignore_ascii_case(b"OAUTH2"));
        if !oauth2_only && self.policy.uses(Factor::SharedToken) {
            if let Some(expected) = &self.shared_token {
                if bool::from(expected.as_bytes().ct_eq(token)) {
                    conn.auth.shared = true;
                    return Ok(());
                }
            }
        }
        Err(self.failed(conn, "the token does not match"))
    }

    /// Count a failed attempt and return the generic error. The reason goes to the debug log.
    fn failed(&self, conn: &mut ConnState, reason: &str) -> CommandError {
        conn.auth.failures += 1;
        if let Some(ip) = conn.auth.peer {
            self.limiter.record(ip);
        }
        tracing::debug!(conn = conn.id, reason, "authentication failed");
        if conn.auth.failures >= self.max_conn_failures {
            tracing::debug!(
                conn = conn.id,
                "too many failed attempts, closing the connection"
            );
            conn.closing = true;
        }
        CommandError::Custom("WRONGPASS invalid username-password pair or user is disabled.".into())
    }

    /// Check that the connection can run the command `name` (upper case).
    pub fn authorize(&self, conn: &mut ConnState, name: &str) -> Result<(), CommandError> {
        if self.policy == Policy::None {
            return Ok(());
        }
        let scopes = conn.auth.scopes(&self.policy).ok_or(CommandError::NoAuth)?;
        let allowed = match access(name) {
            Access::Connection => true,
            Access::Read => scopes.can_read(),
            Access::Write => scopes.can_write(),
        };
        if allowed {
            Ok(())
        } else {
            Err(CommandError::NoPerm(name.to_ascii_lowercase()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn security(args: &[&str]) -> Security {
        let mut argv = vec!["discus"];
        argv.extend_from_slice(args);
        Security::from_config(&Config::parse_from(argv).unwrap()).unwrap()
    }

    #[test]
    fn a_read_only_connection_cannot_write() {
        let s = security(&["--shared-token", "x"]);
        let mut conn = ConnState::new(1, ConnAuth::default());
        assert_eq!(s.authorize(&mut conn, "GET"), Err(CommandError::NoAuth));
        conn.auth.shared = true;
        assert_eq!(s.authorize(&mut conn, "SET"), Ok(()));
        // Only an mTLS or OAuth 2.0 grant can be read-only, so fake one through a policy that
        // takes the intersection.
        let ro = Security {
            policy: Policy::Any(vec![Factor::Mtls]),
            ..s
        };
        conn.auth.cert = Some(CertGrant {
            identity: "ro".into(),
            scopes: Scopes::READ,
        });
        assert_eq!(ro.authorize(&mut conn, "GET"), Ok(()));
        assert_eq!(
            ro.authorize(&mut conn, "SET"),
            Err(CommandError::NoPerm("set".into()))
        );
        assert_eq!(
            CommandError::NoPerm("set".into()).text(),
            "NOPERM this user has no permissions to run the 'set' command"
        );
    }
}
