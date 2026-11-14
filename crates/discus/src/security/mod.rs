//! Client-side security: TLS, authentication, and authorization (spec §14).
//!
//! `Security` owns every decision. The server calls it at accept time, the `AUTH` and `HELLO`
//! handlers call `authenticate`, and the dispatcher calls `authorize` before each command.

pub mod admin;
pub mod args;
pub mod file;
pub mod identity;
pub mod limiter;
#[cfg(feature = "oauth2")]
pub mod oauth2;
pub mod policy;
pub mod proxy;
pub mod session;
pub mod stream;
pub mod tls;

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::net::TcpStream;

use ctutils::CtEq;

pub use args::{ClientAuth, SecurityArgs, TlsMode};
pub use file::AuthFile;
pub use policy::{Factor, Policy, Scopes};
pub use session::{CertGrant, ConnAuth, TokenGrant};
pub use stream::ClientStream;

use crate::command::table::Access;
use crate::command::{CommandError, CommandSpec};
use crate::node::ConnState;
use crate::settings::Live;
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

/// How long a client has to finish the connection setup, the TLS handshake included.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The files of the client listener certificate. A reload reads them again.
struct ClientTlsFiles {
    cert: PathBuf,
    key: PathBuf,
    client_ca: Option<PathBuf>,
    require_client: bool,
}

impl ClientTlsFiles {
    fn listener(&self) -> io::Result<rustls::ServerConfig> {
        let ca = self.client_ca.as_deref();
        tls::server_config(&self.cert, &self.key, ca, self.require_client)
    }

    /// The admin port asks for a client certificate but never requires one.
    fn admin(&self) -> io::Result<rustls::ServerConfig> {
        tls::server_config(&self.cert, &self.key, self.client_ca.as_deref(), false)
    }
}

/// The security state of a node.
pub struct Security {
    mode: TlsMode,
    tls_files: ClientTlsFiles,
    server_tls: ArcSwap<rustls::ServerConfig>,
    lb_sources: Vec<ipnet::IpNet>,
    lb_identity_tlv: Option<u8>,
    cert_rules: Vec<(String, Scopes)>,
    policy: Policy,
    shared_token: Option<String>,
    live_settings: Arc<Live>,
    limiter: FailureLimiter,
    admin_tls: Option<Arc<ArcSwap<rustls::ServerConfig>>>,
    admin_identities: Vec<String>,
    dev_insecure: bool,
    #[cfg(feature = "oauth2")]
    oauth: Option<oauth2::OAuth2>,
}

impl Security {
    /// Check the configuration and build the security state.
    pub fn from_config(
        config: &Config,
        live_settings: Arc<Live>,
    ) -> Result<Security, SecurityError> {
        let args = &config.security;
        tls::install_default();
        let file = match &args.auth_config {
            Some(path) => AuthFile::load(path)?,
            None => AuthFile::default(),
        };
        let policy = match &args.auth_policy {
            Some(p) => Policy::parse(p)?,
            None if config.shared_token.is_some() => {
                Policy::All(vec![Factor::Tls, Factor::SharedToken])
            }
            // Every client uses TLS, so `none` means TLS without credentials.
            None => Policy::None,
        };

        let (Some(tls_cert), Some(tls_key)) = (&args.tls_cert, &args.tls_key) else {
            return fail(
                "every client uses TLS, so set --tls-cert and --tls-key for the client listener",
            );
        };
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
        #[cfg(feature = "oauth2")]
        let oauth = Self::oauth2(args, &policy, &file)?;
        #[cfg(not(feature = "oauth2"))]
        Self::no_oauth2(&policy, &file)?;
        let lb = args.tls_mode == TlsMode::Lb;
        if lb && (args.lb_trusted_source.is_empty() || args.tls_client_ca.is_none()) {
            return fail(
                "--tls-mode lb needs --lb-trusted-source and --tls-client-ca, so the node trusts \
                 a forwarded identity only from the load balancer over mTLS",
            );
        }
        // In lb mode the load balancer must always present its certificate.
        let required = lb || args.tls_client_auth == ClientAuth::Required;
        if required && args.tls_client_ca.is_none() {
            return fail("--tls-client-auth required needs --tls-client-ca");
        }
        let mut cert_rules = Vec::new();
        for rule in &file.client_cert {
            let scopes = Scopes::parse_names(&rule.scopes)
                .map_err(|e| format!("--auth-config client_cert {:?}: {e}", rule.identity))?;
            cert_rules.push((rule.identity.clone(), scopes));
        }
        let tls_files = ClientTlsFiles {
            cert: tls_cert.clone(),
            key: tls_key.clone(),
            client_ca: args.tls_client_ca.clone(),
            require_client: required,
        };
        let admin_tls = if args.admin_tls {
            let config = tls_files.admin().map_err(|e| e.to_string())?;
            Some(Arc::new(ArcSwap::from_pointee(config)))
        } else {
            None
        };
        if !file.admin.cert_identities.is_empty()
            && (!args.admin_tls || args.tls_client_ca.is_none())
        {
            return fail("[admin] cert_identities needs --admin-tls and --tls-client-ca");
        }
        let server_tls = ArcSwap::from_pointee(tls_files.listener().map_err(|e| e.to_string())?);

        Ok(Security {
            mode: args.tls_mode,
            tls_files,
            server_tls,
            lb_sources: args.lb_trusted_source.clone(),
            lb_identity_tlv: args.lb_identity_tlv,
            cert_rules,
            policy,
            shared_token: config.shared_token.clone(),
            limiter: FailureLimiter::new(live_settings.clone()),
            live_settings,
            admin_tls,
            admin_identities: file.admin.cert_identities.clone(),
            dev_insecure: args.dev_insecure_oauth2,
            #[cfg(feature = "oauth2")]
            oauth,
        })
    }

    #[cfg(not(feature = "oauth2"))]
    fn no_oauth2(policy: &Policy, file: &AuthFile) -> Result<(), SecurityError> {
        let missing = |what: &str| {
            fail(format!(
                "{what} needs OAuth 2.0, but this binary was built without the \"oauth2\" feature"
            ))
        };
        if policy.uses(Factor::Oauth2) {
            return missing("the policy oauth2 factor");
        }
        if !file.issuer.is_empty() {
            return missing("the [[issuer]] section of --auth-config");
        }
        if file.kubernetes.is_some() {
            return missing("the [kubernetes] section of --auth-config");
        }
        Ok(())
    }

    #[cfg(feature = "oauth2")]
    fn oauth2(
        args: &SecurityArgs,
        policy: &Policy,
        file: &AuthFile,
    ) -> Result<Option<oauth2::OAuth2>, SecurityError> {
        #[cfg(not(feature = "k8s"))]
        if file.kubernetes.is_some() {
            return fail(
                "the [kubernetes] section of --auth-config needs the service account issuer, but \
                 this binary was built without the \"k8s\" feature",
            );
        }
        if !policy.uses(Factor::Oauth2) {
            if !file.issuer.is_empty() || file.kubernetes.is_some() {
                return fail(format!(
                    "--auth-config lists trusted issuers, but the policy {policy} does not use \
                     oauth2"
                ));
            }
            return Ok(None);
        }
        let oauth = oauth2::OAuth2::from_file(
            &file.issuer,
            file.kubernetes.as_ref(),
            args.auth_clock_skew_secs,
            args.dev_insecure_oauth2,
        )?;
        if oauth.is_empty() {
            return fail("the policy uses oauth2, but --auth-config lists no trusted issuer");
        }
        Ok(Some(oauth))
    }

    /// True when every trusted issuer has its keys. The probes stay unready until then.
    pub fn ready(&self) -> bool {
        #[cfg(feature = "oauth2")]
        if let Some(o) = &self.oauth {
            return o.ready();
        }
        true
    }

    /// Start the background tasks: one JWKS loader per trusted issuer.
    pub fn start(&self, stop: tokio_util::sync::CancellationToken) {
        #[cfg(feature = "oauth2")]
        if let Some(o) = &self.oauth {
            o.start(stop);
        }
        #[cfg(not(feature = "oauth2"))]
        drop(stop);
    }

    /// The TLS config of the admin port, with `--admin-tls`. A reload replaces its content.
    pub fn admin_tls(&self) -> Option<Arc<ArcSwap<rustls::ServerConfig>>> {
        self.admin_tls.clone()
    }

    /// The files that [`Security::reload_client_tls`] reads.
    pub fn client_tls_files(args: &SecurityArgs) -> Vec<PathBuf> {
        [&args.tls_cert, &args.tls_key, &args.tls_client_ca]
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }

    /// Read the certificate, the key, and the client CA again, and use them for every new
    /// connection on the client, redirect, and admin listeners. On an error, the listeners keep
    /// their current certificates.
    pub fn reload_client_tls(&self) -> io::Result<()> {
        let listener = self.tls_files.listener()?;
        let admin = match &self.admin_tls {
            Some(_) => Some(self.tls_files.admin()?),
            None => None,
        };
        self.server_tls.store(Arc::new(listener));
        if let (Some(slot), Some(admin)) = (&self.admin_tls, admin) {
            slot.store(Arc::new(admin));
        }
        Ok(())
    }

    /// Whether an admin request may pass: an admin certificate, or a bearer token with
    /// `discus.admin`. A bearer token over plaintext needs `--dev-insecure-oauth2`.
    pub async fn admin_allowed(&self, peer: &admin::AdminPeer, bearer: Option<&str>) -> bool {
        if let Some(id) = &peer.identity {
            if self
                .admin_identities
                .iter()
                .any(|p| identity::pattern_matches(p, id))
            {
                return true;
            }
        }
        let Some(token) = bearer else {
            return false;
        };
        if !peer.tls && !self.dev_insecure {
            tracing::debug!("admin bearer token over plaintext refused");
            return false;
        }
        #[cfg(feature = "oauth2")]
        if let Some(o) = &self.oauth {
            o.prefetch(token.as_bytes()).await;
            return match o.check(token.as_bytes(), oauth2::unix_now()) {
                Ok(c) => c.scopes.is_admin(),
                Err(reject) => {
                    tracing::debug!(reason = %reject, "admin token refused");
                    false
                }
            };
        }
        let _ = token;
        false
    }

    /// The security status for the admin page.
    pub fn status(&self) -> admin::SecurityStatus {
        #[cfg_attr(not(feature = "oauth2"), allow(unused_mut))]
        let mut issuers = Vec::new();
        #[cfg(feature = "oauth2")]
        if let Some(o) = &self.oauth {
            let now = oauth2::unix_now();
            for s in o.sources() {
                let t = &s.trusted;
                issuers.push(admin::IssuerStatus {
                    issuer: t.name(),
                    loaded: t.loaded(),
                    last_refresh_unix: t.loaded_at().map(|at| now - at.elapsed().as_secs() as i64),
                });
            }
        }
        admin::SecurityStatus {
            tls_mode: self.mode.name(),
            policy: self.policy.to_string(),
            ready: self.ready(),
            issuers,
        }
    }

    /// True for a command that `prefetch` must see before it runs.
    pub fn wants_prefetch(&self, cmd: &discus_resp::Command) -> bool {
        #[cfg(feature = "oauth2")]
        if self.oauth.is_some() {
            let name = &cmd[0];
            return name.eq_ignore_ascii_case(b"AUTH") || name.eq_ignore_ascii_case(b"HELLO");
        }
        let _ = cmd;
        false
    }

    /// The async part of `AUTH` and `HELLO ... AUTH`: refresh the JWKS on an unknown `kid`, so
    /// the synchronous handler finds the key.
    /// A source over its failed-attempt limit never reaches the identity provider.
    pub async fn prefetch(&self, conn: &ConnState, cmd: &discus_resp::Command) {
        #[cfg(feature = "oauth2")]
        if let (Some(o), Some(token)) = (&self.oauth, token_arg(cmd)) {
            if conn.auth.peer.is_some_and(|ip| self.limiter.blocked(ip)) {
                return;
            }
            o.prefetch(token).await;
        }
        let _ = (conn, cmd);
    }

    /// The grant of a verified certificate identity. Read-write without a matching rule.
    fn cert_grant(&self, identity: String) -> CertGrant {
        let scopes = self
            .cert_rules
            .iter()
            .find(|(pattern, _)| identity::pattern_matches(pattern, &identity))
            .map_or(Scopes::READ_WRITE, |(_, scopes)| *scopes);
        CertGrant { identity, scopes }
    }

    /// Run the transport setup of a new client connection: the TLS handshake in `node` mode, and
    /// the PROXY header plus the load balancer handshake in `lb` mode. Returns the stream and what
    /// the node knows about the client.
    pub async fn accept(
        &self,
        tcp: TcpStream,
        peer: SocketAddr,
    ) -> io::Result<(ClientStream, ConnAuth)> {
        tokio::time::timeout(HANDSHAKE_TIMEOUT, self.setup(tcp, peer))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection setup timeout"))?
    }

    async fn setup(
        &self,
        mut tcp: TcpStream,
        peer: SocketAddr,
    ) -> io::Result<(ClientStream, ConnAuth)> {
        let mut auth = ConnAuth::new(Some(peer.ip()));
        auth.tls = true;
        let forwarded = if self.mode == TlsMode::Lb {
            if !self.lb_sources.iter().any(|n| n.contains(&peer.ip())) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "lb mode: the peer is not a trusted load balancer source",
                ));
            }
            Some(proxy::read_proxy_header(&mut tcp, self.lb_identity_tlv).await?)
        } else {
            None
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(self.server_tls.load_full());
        let tls = acceptor.accept(tcp).await?;
        if let Some(header) = forwarded {
            // The handshake verified the load balancer certificate, so the header is trusted.
            if let Some(source) = header.source {
                auth.peer = Some(source.ip());
            }
            if header.client_tls == Some(false) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "lb mode: the client did not use TLS to the load balancer",
                ));
            }
            if let Some(identity) = header.identity {
                tracing::debug!(%identity, "forwarded client identity");
                auth.cert = Some(self.cert_grant(identity));
            }
            return Ok((Box::new(tls), auth));
        }
        let conn = tls.get_ref().1;
        if conn.peer_certificates().is_some_and(|c| !c.is_empty()) {
            // A verified certificate without a SAN or CN still meets mtls.
            let identity = identity::peer_identity(conn).unwrap_or_default();
            tracing::debug!(%identity, "client certificate verified");
            auth.cert = Some(self.cert_grant(identity));
        }
        Ok((Box::new(tls), auth))
    }

    /// True when the connection meets the policy.
    pub fn is_authenticated(&self, auth: &ConnAuth) -> bool {
        auth.scopes(&self.policy).is_some()
    }

    /// Handle the credentials of `AUTH` or `HELLO ... AUTH`. `user` is only a hint, except that
    /// `OAUTH2` selects OAuth 2.0. The auth attempt metric counts every result (spec §15.4).
    pub fn authenticate(
        &self,
        conn: &mut ConnState,
        user: Option<&[u8]>,
        token: &[u8],
    ) -> Result<(), CommandError> {
        let result = self.check_credentials(conn, user, token);
        discus_telemetry::global().auth(result.is_ok());
        result
    }

    fn check_credentials(
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
        #[cfg(feature = "oauth2")]
        if let Some(o) = &self.oauth {
            let generation = o.generation();
            return match o.check(token, oauth2::unix_now()) {
                Ok(claims) => {
                    tracing::debug!(
                        conn = conn.id,
                        sub = claims.sub.as_deref().unwrap_or_default(),
                        jti = claims.jti.as_deref().unwrap_or_default(),
                        "token accepted"
                    );
                    conn.auth.token = Some(TokenGrant {
                        raw: bytes::Bytes::copy_from_slice(token),
                        exp: claims.exp,
                        nbf: claims.nbf,
                        scopes: claims.scopes,
                        // An introspected token has no JWKS to check against again.
                        generation: oauth2::is_jwt(token).then_some(generation),
                    });
                    Ok(())
                }
                Err(reject) => Err(self.failed(conn, &reject.to_string())),
            };
        }
        Err(self.failed(conn, "the token does not match"))
    }

    /// Drop the token grant of a connection when it expired, or when it fails a full check
    /// after a JWKS refresh.
    #[cfg(feature = "oauth2")]
    fn recheck_token(&self, conn: &mut ConnState) {
        let (Some(o), Some(t)) = (&self.oauth, &conn.auth.token) else {
            return;
        };
        let now = oauth2::unix_now();
        let skew = o.skew();
        let current = o.generation();
        if t.generation.is_some_and(|g| g != current) {
            match o.check(&t.raw.clone(), now) {
                Ok(c) => {
                    if let Some(t) = conn.auth.token.as_mut() {
                        t.exp = c.exp;
                        t.nbf = c.nbf;
                        t.scopes = c.scopes;
                        t.generation = Some(current);
                    }
                }
                Err(reject) => {
                    tracing::debug!(conn = conn.id, reason = %reject, "token no longer valid");
                    conn.auth.token = None;
                }
            }
        } else if now > t.exp + skew || t.nbf.is_some_and(|n| now + skew < n) {
            tracing::debug!(conn = conn.id, "token expired");
            conn.auth.token = None;
        }
    }

    /// Count a failed attempt and return the generic error. The reason goes to the debug log.
    fn failed(&self, conn: &mut ConnState, reason: &str) -> CommandError {
        conn.auth.failures += 1;
        if let Some(ip) = conn.auth.peer {
            self.limiter.record(ip);
        }
        tracing::debug!(conn = conn.id, reason, "authentication failed");
        if conn.auth.failures
            >= self
                .live_settings
                .get()
                .auth_max_failures_per_connection
                .max(1)
        {
            tracing::debug!(
                conn = conn.id,
                "too many failed attempts, closing the connection"
            );
            conn.closing = true;
        }
        CommandError::Custom("WRONGPASS invalid username-password pair or user is disabled.".into())
    }

    /// Check that the connection can run the command `spec`.
    pub fn authorize(&self, conn: &mut ConnState, spec: &CommandSpec) -> Result<(), CommandError> {
        if self.policy == Policy::None || spec.access == Access::Public {
            return Ok(());
        }
        #[cfg(feature = "oauth2")]
        self.recheck_token(conn);
        let scopes = conn.auth.scopes(&self.policy).ok_or(CommandError::NoAuth)?;
        let allowed = match spec.access {
            Access::Public | Access::Connection => true,
            Access::Read => scopes.can_read(),
            Access::Write => scopes.can_write(),
        };
        if allowed {
            Ok(())
        } else {
            Err(CommandError::NoPerm(spec.name.to_ascii_lowercase()))
        }
    }
}

/// The token of `AUTH [user] <token>` or `HELLO ... AUTH <user> <token>`.
#[cfg(feature = "oauth2")]
fn token_arg(cmd: &discus_resp::Command) -> Option<&[u8]> {
    let name = &cmd[0];
    if name.eq_ignore_ascii_case(b"AUTH") {
        return match cmd.len() {
            2 | 3 => Some(&cmd[cmd.len() - 1]),
            _ => None,
        };
    }
    (1..cmd.len())
        .find(|&i| cmd[i].eq_ignore_ascii_case(b"AUTH") && i + 2 < cmd.len())
        .map(|i| &cmd[i + 2][..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &[u8]) -> &'static CommandSpec {
        crate::command::lookup(name).unwrap()
    }

    fn security(args: &[&str]) -> Security {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = (dir.path().join("cert.pem"), dir.path().join("key.pem"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        let (cert_path, key_path) = (cert_path.to_str().unwrap(), key_path.to_str().unwrap());
        let mut argv = vec!["discus", "--tls-cert", cert_path, "--tls-key", key_path];
        argv.extend_from_slice(args);
        let config = Config::parse_from(argv).unwrap();
        let live_settings = Live::new(crate::settings::Reloadable::from_config(&config));
        Security::from_config(&config, live_settings).unwrap()
    }

    #[test]
    fn a_read_only_connection_cannot_write() {
        let s = security(&["--shared-token", "x"]);
        let mut conn = ConnState::new(
            1,
            ConnAuth {
                tls: true,
                ..ConnAuth::default()
            },
        );
        assert_eq!(
            s.authorize(&mut conn, spec(b"GET")),
            Err(CommandError::NoAuth)
        );
        conn.auth.shared = true;
        assert_eq!(s.authorize(&mut conn, spec(b"SET")), Ok(()));
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
        assert_eq!(ro.authorize(&mut conn, spec(b"GET")), Ok(()));
        assert_eq!(
            ro.authorize(&mut conn, spec(b"SET")),
            Err(CommandError::NoPerm("set".into()))
        );
        assert_eq!(
            CommandError::NoPerm("set".into()).text(),
            "NOPERM this user has no permissions to run the 'set' command"
        );
    }
}
