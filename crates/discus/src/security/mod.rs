//! Client-side security: TLS, authentication, and authorization (spec §14).
//!
//! `Security` owns every decision. The server calls it at accept time, the `AUTH` and `HELLO`
//! handlers call `authenticate`, and the dispatcher calls `authorize` before each command.

pub mod acl;
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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::net::TcpStream;

pub use args::{ClientAuth, SecurityArgs, TlsMode};
pub use file::AuthFile;
pub use policy::{Factor, Policy, Scopes};
pub use session::{CertGrant, ConnAuth, TokenGrant, UserGrant};
pub use stream::ClientStream;

use crate::command::table::Access;
use crate::command::{CommandError, CommandSpec};
use crate::node::ConnState;
use crate::settings::Live;
use crate::Config;
use acl::Acl;
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
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The files of the client listener certificate. A reload reads them again.
struct ClientTlsFiles {
    cert: PathBuf,
    key: PathBuf,
    client_ca: Option<PathBuf>,
    require_client: bool,
    /// The certificate and key of the admin port, if they differ from the listener.
    admin: Option<(PathBuf, PathBuf)>,
}

impl ClientTlsFiles {
    fn listener(&self) -> io::Result<rustls::ServerConfig> {
        let ca = self.client_ca.as_deref();
        tls::server_config(&self.cert, &self.key, ca, self.require_client)
    }

    /// The admin port asks for a client certificate but never requires one.
    fn admin(&self) -> io::Result<rustls::ServerConfig> {
        let (cert, key) = match &self.admin {
            Some((cert, key)) => (cert, key),
            None => (&self.cert, &self.key),
        };
        tls::server_config(cert, key, self.client_ca.as_deref(), false)
    }
}

/// The security state of a node.
pub struct Security {
    mode: TlsMode,
    tls_files: ClientTlsFiles,
    server_tls: ArcSwap<rustls::ServerConfig>,
    lb_sources: Vec<ipnet::IpNet>,
    lb_identity_tlv: Option<u8>,
    lb_san: String,
    cert_rules: Vec<(String, Scopes)>,
    policy: Policy,
    acl: Option<ArcSwap<Acl>>,
    acl_file: Option<PathBuf>,
    /// Counts the loads of `acl_file`, so a connection can tell that its user is stale.
    acl_generation: AtomicU64,
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
        let acl = match &args.acl_file {
            Some(path) => {
                Some(Acl::load(path).map_err(|e| format!("--acl-file {}: {e}", path.display()))?)
            }
            None => None,
        };
        let policy = match &args.auth_policy {
            Some(p) => Policy::parse(p)?,
            None if acl.is_some() => Policy::All(vec![Factor::Password]),
            // An open node must be a choice of the operator, never a silent default.
            None => {
                return fail(
                    "--auth-policy is not set. Choose a policy: --auth-policy none for TLS \
                     without credentials, or a policy with mtls, oauth2, or password",
                )
            }
        };
        match (policy.uses(Factor::Password), &acl) {
            (true, None) => return fail("the policy uses password, so set --acl-file"),
            (false, Some(_)) => {
                return fail(format!(
                    "--acl-file is set, but the policy {policy} does not use password"
                ))
            }
            _ => {}
        }

        let (Some(tls_cert), Some(tls_key)) = (&args.tls_cert, &args.tls_key) else {
            return fail(
                "every client uses TLS, so set --tls-cert and --tls-key for the client listener",
            );
        };
        if policy.uses(Factor::Mtls) && args.tls_client_ca.is_none() {
            return fail("the policy uses mtls, so set --tls-client-ca");
        }
        #[cfg(feature = "oauth2")]
        let oauth = Self::oauth2(args, &policy, &file)?;
        #[cfg(not(feature = "oauth2"))]
        Self::no_oauth2(&policy, &file)?;
        let users = trust_root_users(config, &file, policy.uses(Factor::Oauth2));
        tls::warn_if_no_trust_roots(&users, &tls::trust_roots());
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
            admin: args.admin_tls_cert.clone().zip(args.admin_tls_key.clone()),
        };
        if args.admin_tls_cert.is_some() != args.admin_tls_key.is_some() {
            return fail("--admin-tls-cert and --admin-tls-key must be set together");
        }
        if args.admin_tls_cert.is_some() && !args.admin_tls {
            return fail("--admin-tls-cert needs --admin-tls");
        }
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
        if args.dev_insecure_oauth2 {
            tracing::warn!(
                "--dev-insecure-oauth2: the node accepts http issuers and bearer tokens over a \
                 plaintext admin port. Development only"
            );
        }
        if policy == Policy::None {
            tracing::warn!(
                "--auth-policy none: every client that completes a TLS handshake gets read and \
                 write access"
            );
        }

        Ok(Security {
            mode: args.tls_mode,
            tls_files,
            server_tls,
            lb_sources: args.lb_trusted_source.clone(),
            lb_identity_tlv: args.lb_identity_tlv,
            lb_san: args.lb_san.clone(),
            cert_rules,
            policy,
            acl: acl.map(ArcSwap::from_pointee),
            acl_file: args.acl_file.clone(),
            acl_generation: AtomicU64::new(0),
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
        #[cfg(all(feature = "testing", feature = "oauth2-introspection"))]
        let oauth = match args.test_introspection_cache_secs {
            Some(secs) => {
                let mut oauth = oauth;
                oauth.set_introspection_cache_secs(secs);
                oauth
            }
            None => oauth,
        };
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
        [
            &args.tls_cert,
            &args.tls_key,
            &args.tls_client_ca,
            &args.admin_tls_cert,
            &args.admin_tls_key,
        ]
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

    /// The file that [`Security::reload_acl`] reads, with `--acl-file`.
    pub fn acl_file(&self) -> Option<&Path> {
        self.acl_file.as_deref()
    }

    /// The current users, with `--acl-file`.
    pub fn acl(&self) -> Option<Arc<Acl>> {
        self.acl.as_ref().map(|slot| slot.load_full())
    }

    /// Read the ACL file again. A connection takes its new rules, or loses its login, at its
    /// next command. On an error, the node keeps its users.
    pub fn reload_acl(&self) -> io::Result<()> {
        let Some(path) = &self.acl_file else {
            return Ok(());
        };
        self.reload_acl_text(&std::fs::read(path)?)
    }

    /// Load `content` as the new ACL file.
    pub fn reload_acl_text(&self, content: &[u8]) -> io::Result<()> {
        if let Some(slot) = &self.acl {
            let invalid = |e: String| io::Error::new(io::ErrorKind::InvalidData, e);
            let text = std::str::from_utf8(content).map_err(|e| invalid(e.to_string()))?;
            slot.store(Arc::new(Acl::parse(text).map_err(invalid)?));
            // The users go first, so a connection that sees the new generation sees them too.
            self.acl_generation.fetch_add(1, Ordering::Release);
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
        // A source over its failed-attempt limit never reaches the identity provider.
        if peer.addr.is_some_and(|ip| self.limiter.blocked(ip)) {
            tracing::debug!("admin bearer token refused, the source is over its limit");
            return false;
        }
        #[cfg(feature = "oauth2")]
        if let Some(o) = &self.oauth {
            o.prefetch(token.as_bytes()).await;
            match o.check(token.as_bytes(), oauth2::unix_now()) {
                Ok(c) if c.scopes.is_admin() => return true,
                Ok(_) => tracing::debug!("admin token refused, it has no discus.admin scope"),
                Err(reject) => tracing::debug!(reason = %reject, "admin token refused"),
            }
        }
        let _ = token;
        if let Some(ip) = peer.addr {
            self.limiter.record(ip);
        }
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

    /// True for a command that `prefetch` must see before it runs, and for every command of a
    /// connection whose introspected grant is due for a recheck. A connection without such a
    /// grant pays only the `Option` test, and one with such a grant pays a clock read. An `AUTH`
    /// or `HELLO` with a password policy also pays the password check.
    pub fn wants_prefetch(&self, conn: &ConnState, cmd: &discus_resp::Command) -> bool {
        #[cfg(feature = "oauth2")]
        if self.oauth.is_some() {
            #[cfg(feature = "oauth2-introspection")]
            if recheck_due(conn) {
                return true;
            }
            return self.token_to_prefetch(cmd).is_some();
        }
        let _ = (conn, cmd);
        false
    }

    /// The async part of `AUTH` and `HELLO ... AUTH`: refresh the JWKS on an unknown `kid`, so
    /// the synchronous handler finds the key.
    /// A source over its failed-attempt limit never reaches the identity provider.
    /// Before that, it introspects again the opaque token of a grant that is due.
    pub async fn prefetch(&self, conn: &mut ConnState, cmd: &discus_resp::Command) {
        #[cfg(feature = "oauth2-introspection")]
        self.refresh_grant(conn).await;
        #[cfg(feature = "oauth2")]
        if let (Some(o), Some(token)) = (&self.oauth, self.token_to_prefetch(cmd)) {
            if conn.auth.peer.is_some_and(|ip| self.limiter.blocked(ip)) {
                return;
            }
            o.prefetch(token).await;
        }
        let _ = (conn, cmd);
    }

    /// The token of `AUTH` or `HELLO ... AUTH` that the identity provider may see: none when the
    /// password path decides the value, because a password must never leave the node.
    #[cfg(feature = "oauth2")]
    fn token_to_prefetch<'c>(&self, cmd: &'c discus_resp::Command) -> Option<&'c [u8]> {
        let (user, value) = credentials_arg(cmd)?;
        matches!(self.password_path(user, value), PasswordPath::Undecided).then_some(value)
    }

    /// Introspect the opaque token of a due grant again, and drop the grant unless the answer
    /// says the token is active. No answer also drops the grant: a provider that is unreachable
    /// cannot confirm that the token is not revoked, so the node fails closed.
    ///
    /// The failed-attempt limit does not apply here. The grant came from a valid token, and
    /// each token costs at most one request per cache time.
    #[cfg(feature = "oauth2-introspection")]
    async fn refresh_grant(&self, conn: &mut ConnState) {
        let Some(o) = &self.oauth else {
            return;
        };
        if !recheck_due(conn) {
            return;
        }
        let Some(raw) = conn.auth.token.as_ref().map(|t| t.raw.clone()) else {
            return;
        };
        let before = self.is_authenticated(&conn.auth);
        o.prefetch(&raw).await;
        match o.check(&raw, oauth2::unix_now()) {
            Ok(c) => {
                if let Some(t) = conn.auth.token.as_mut() {
                    t.exp = c.exp;
                    t.nbf = c.nbf;
                    t.scopes = c.scopes;
                    t.recheck = c.recheck;
                }
            }
            Err(reject) => {
                tracing::debug!(conn = conn.id, reason = %reject, "token no longer active");
                conn.auth.token = None;
                self.close_if_lost(conn, before);
            }
        }
    }

    /// Close a connection that met the policy and no longer does, as Valkey closes the
    /// connections of a user that `ACL LOAD` removes. The connection must not keep the limits of
    /// an authenticated client.
    fn close_if_lost(&self, conn: &mut ConnState, before: bool) {
        if before && !self.is_authenticated(&conn.auth) {
            tracing::debug!(conn = conn.id, "the login is gone, closing the connection");
            conn.closing = true;
        }
    }

    /// The grant of a verified certificate identity. Without any rule every identity gets
    /// read-write. With rules, an identity that matches none gets no grant, so a typing error in
    /// a rule never widens access. An empty identity gets no grant.
    fn cert_grant(&self, identity: String) -> Option<CertGrant> {
        if identity.is_empty() {
            tracing::debug!("the certificate identity is empty");
            return None;
        }
        let scopes = if self.cert_rules.is_empty() {
            Scopes::READ_WRITE
        } else {
            let rule = self
                .cert_rules
                .iter()
                .find(|(pattern, _)| identity::pattern_matches(pattern, &identity));
            let Some((_, scopes)) = rule else {
                tracing::debug!(%identity, "no client_cert rule matches the identity");
                return None;
            };
            *scopes
        };
        Some(CertGrant { identity, scopes })
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
            let pinned = self.lb_san.is_empty()
                || tls
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|c| c.first())
                    .is_some_and(|der| identity::has_uri_san(der, &self.lb_san));
            if !pinned {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "lb mode: the load balancer certificate does not carry the --lb-san URI SAN",
                ));
            }
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
                auth.cert = self.cert_grant(identity);
            }
            return Ok((Box::new(tls), auth));
        }
        let conn = tls.get_ref().1;
        if conn.peer_certificates().is_some_and(|c| !c.is_empty()) {
            // A verified certificate without a SAN or CN has an empty identity, which does not
            // meet mtls.
            let identity = identity::peer_identity(conn).unwrap_or_default();
            tracing::debug!(%identity, "client certificate verified");
            auth.cert = self.cert_grant(identity);
        }
        Ok((Box::new(tls), auth))
    }

    /// True when the connection meets the policy.
    pub fn is_authenticated(&self, auth: &ConnAuth) -> bool {
        auth.authenticated(&self.policy)
    }

    /// Handle the credentials of `AUTH` or `HELLO ... AUTH`. A user of the ACL file takes the
    /// password path. Any other `user` is only a hint, except that `OAUTH2` selects OAuth 2.0
    /// and `default` is the form without a name.
    /// The auth attempt metric counts every result (spec §15.4).
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
        if !self.policy.uses(Factor::Oauth2) && !self.policy.uses(Factor::Password) {
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
        // With an `any` policy, one credential is the whole login, so a new login replaces the
        // earlier one. An earlier token must not keep its scopes next to a user with fewer
        // rights. With an `all` policy, the connection needs both.
        let replaces = matches!(self.policy, Policy::Any(_));
        match self.password_path(user, token) {
            PasswordPath::Accepted(grant) => {
                conn.auth.user = Some(grant);
                if replaces {
                    conn.auth.token = None;
                }
                return Ok(());
            }
            PasswordPath::Refused => return Err(self.failed(conn, "wrong password or user is off")),
            PasswordPath::Undecided => {}
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
                        recheck: claims.recheck,
                    });
                    if replaces {
                        conn.auth.user = None;
                    }
                    Ok(())
                }
                Err(reject) => Err(self.failed(conn, &reject.to_string())),
            };
        }
        Err(self.failed(conn, "no factor accepts the credential"))
    }

    /// Decide whether the password path takes the value of `AUTH [user] <value>`. Both
    /// `check_credentials` and the OAuth 2.0 prefetch use this, so they never disagree, and a value
    /// that the password path decides never reaches an identity provider.
    fn password_path(&self, user: Option<&[u8]>, value: &[u8]) -> PasswordPath {
        let Some(acl) = self
            .acl
            .as_ref()
            .filter(|_| self.policy.uses(Factor::Password))
        else {
            return PasswordPath::Undecided;
        };
        if user.is_some_and(|u| u.eq_ignore_ascii_case(b"OAUTH2")) {
            return PasswordPath::Undecided;
        }
        // Read the generation before the users: `reload_acl` stores the users first and changes
        // the generation second, so an old user never pairs with a new generation.
        let generation = self.acl_generation.load(Ordering::Acquire);
        let acl = acl.load();
        // Hash before the lookup, so an unknown name costs the same as a wrong password.
        let hash = acl::sha256(value);
        // Most client libraries send `AUTH default <value>` for a credential without a user, so
        // the exact name `default` is the form without a name. Names are case-sensitive.
        let named = user.filter(|u| *u != acl::DEFAULT_USER.as_bytes());
        match acl.user(named.unwrap_or(acl::DEFAULT_USER.as_bytes())) {
            Some(u) if u.accepts(&hash) => PasswordPath::Accepted(UserGrant {
                user: u.clone(),
                generation,
            }),
            // The file names this user, so the value is its password and nothing else.
            Some(_) if named.is_some() => PasswordPath::Refused,
            _ => PasswordPath::Undecided,
        }
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
        } else if now > t.exp.saturating_add(skew) || t.nbf.is_some_and(|n| now + skew < n) {
            tracing::debug!(conn = conn.id, "token expired");
            conn.auth.token = None;
        }
    }

    /// After a reload, give the connection the new rules of its user, or drop the login when the
    /// user is gone. As in Valkey, a user that is now `off` or has a new password keeps its open
    /// logins: `off` only stops a new login.
    fn recheck_user(&self, conn: &mut ConnState) {
        let Some(grant) = &conn.auth.user else { return };
        let current = self.acl_generation.load(Ordering::Acquire);
        if grant.generation == current {
            return;
        }
        let users = self.acl.as_ref().map(|a| a.load());
        conn.auth.user = users
            .and_then(|acl| acl.user(grant.user.name.as_bytes()).cloned())
            .map(|user| UserGrant {
                user,
                generation: current,
            });
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

    /// Check that the connection can run the command `cmd` with the spec `spec`. With an `all`
    /// policy, the scopes and the user rules must both allow it. With an `any` policy, one of
    /// them is enough.
    pub fn authorize(
        &self,
        conn: &mut ConnState,
        spec: &CommandSpec,
        cmd: &discus_resp::Command,
    ) -> Result<(), CommandError> {
        if self.policy == Policy::None || spec.access == Access::Public {
            return Ok(());
        }
        let before = conn.auth.authenticated(&self.policy);
        #[cfg(feature = "oauth2")]
        self.recheck_token(conn);
        self.recheck_user(conn);
        if !conn.auth.authenticated(&self.policy) {
            self.close_if_lost(conn, before);
            return Err(CommandError::NoAuth);
        }
        let sub = spec.subcommand(cmd);
        let no_perm = || CommandError::NoPerm(spec.rule_name(sub));
        let scopes = conn.auth.scopes(&self.policy);
        // The access of the command, and the access of what `cmd` runs: a subcommand with its own
        // entry can need more.
        let by_access = scopes.map(|s| s.allows(spec.access));
        let by_scopes = scopes.is_some_and(|s| s.allows(sub.map_or(spec.access, |sub| sub.access)));
        let all = matches!(self.policy, Policy::All(_));
        if !all && by_scopes {
            return Ok(());
        }
        let by_user = conn
            .auth
            .user
            .as_ref()
            .filter(|_| self.policy.uses(Factor::Password))
            .map(|grant| {
                grant.user.check(spec, sub, cmd).map_err(|d| match d {
                    acl::Denied::Command => no_perm(),
                    acl::Denied::Key => CommandError::NoPermKey,
                })
            });
        match (by_access, by_user) {
            // The rule of the user decides the subcommand, so the scopes need only the access.
            (Some(access), Some(user)) if all => {
                user?;
                if access {
                    Ok(())
                } else {
                    Err(no_perm())
                }
            }
            (_, Some(user)) => user,
            (Some(_), None) if by_scopes => Ok(()),
            (Some(_), None) => Err(no_perm()),
            (None, None) => Err(CommandError::NoAuth),
        }
    }
}

/// The result of the password path for one credential.
enum PasswordPath {
    /// A user of the file accepts the value.
    Accepted(UserGrant),
    /// The file names the user, and the value is not its password.
    Refused,
    /// The password path does not decide, and the value goes on to the other factors.
    Undecided,
}

/// True when the connection holds an introspected grant whose answer ended. Only such a grant
/// reads the clock.
#[cfg(feature = "oauth2-introspection")]
fn recheck_due(conn: &ConnState) -> bool {
    conn.auth
        .token
        .as_ref()
        .and_then(|t| t.recheck)
        .is_some_and(|at| oauth2::unix_now() > at)
}

/// The user and the value of `AUTH [user] <value>` or `HELLO ... AUTH <user> <value>`.
#[cfg(feature = "oauth2")]
fn credentials_arg(cmd: &discus_resp::Command) -> Option<(Option<&[u8]>, &[u8])> {
    let name = &cmd[0];
    if name.eq_ignore_ascii_case(b"AUTH") {
        return match cmd.len() {
            2 => Some((None, &cmd[1])),
            3 => Some((Some(&cmd[1]), &cmd[2])),
            _ => None,
        };
    }
    // `prefetch` also runs for other commands when a grant is due. Their arguments are data.
    if !name.eq_ignore_ascii_case(b"HELLO") {
        return None;
    }
    (1..cmd.len())
        .find(|&i| cmd[i].eq_ignore_ascii_case(b"AUTH") && i + 2 < cmd.len())
        .map(|i| (Some(&cmd[i + 1][..]), &cmd[i + 2][..]))
}

/// What uses the build trust roots, named for the log: each OAuth 2.0 issuer without `ca_file`
/// when the policy uses oauth2, and an https OTLP endpoint without `--otel-ca-file`.
fn trust_root_users(config: &Config, file: &AuthFile, oauth2: bool) -> Vec<String> {
    let mut users = Vec::new();
    if oauth2 {
        let plain = |f: &&file::IssuerFile| {
            f.discovery_url
                .as_deref()
                .unwrap_or(&f.issuer)
                .starts_with("http://")
        };
        for f in file
            .issuer
            .iter()
            .filter(|f| f.ca_file.is_none() && !plain(f))
        {
            users.push(format!("OAuth 2.0 issuer {:?}", f.issuer));
        }
    }
    let otel = &config.telemetry;
    if otel.otel && otel.otel_endpoint.starts_with("https://") && otel.otel_ca_file.is_none() {
        users.push(format!("OTLP endpoint {:?}", otel.otel_endpoint));
    }
    users
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn spec(name: &[u8]) -> &'static CommandSpec {
        crate::command::lookup(name).unwrap()
    }

    /// The result of `Security::from_config` with a self-signed client certificate and `args`.
    fn try_security(args: &[&str]) -> Result<Security, SecurityError> {
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
        Security::from_config(&config, live_settings)
    }

    /// A `Security` with a self-signed client certificate and `args`. Without a policy and
    /// without an ACL file in `args`, the policy is `none`.
    pub(crate) fn security(args: &[&str]) -> Security {
        let mut all = args.to_vec();
        if !args.contains(&"--auth-policy") && !args.contains(&"--acl-file") {
            all.extend(["--auth-policy", "none"]);
        }
        try_security(&all).unwrap()
    }

    /// The log lines that `f` writes at WARN and above.
    fn warnings(f: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Buf(Arc<parking_lot::Mutex<Vec<u8>>>);
        impl io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                self.0.lock().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let out = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || out.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let text = String::from_utf8_lossy(&buf.0.lock()).into_owned();
        text
    }

    #[test]
    fn dev_insecure_oauth2_logs_a_warning() {
        let on = warnings(|| drop(security(&["--dev-insecure-oauth2"])));
        assert!(on.contains("--dev-insecure-oauth2"), "{on}");
        let off = warnings(|| drop(security(&[])));
        assert!(!off.contains("--dev-insecure-oauth2"), "{off}");
    }

    #[test]
    fn trust_root_users_name_what_has_no_ca_file() {
        let file: AuthFile = toml::from_str(
            r#"
            [[issuer]]
            issuer = "https://public"
            audience = "discus"
            [[issuer]]
            issuer = "https://private"
            audience = "discus"
            ca_file = "/ca.pem"
            [[issuer]]
            issuer = "https://dev"
            audience = "discus"
            discovery_url = "http://dev/.well-known/openid-configuration"
            "#,
        )
        .unwrap();
        let parse = |args: &[&str]| {
            let mut argv = vec!["discus"];
            argv.extend_from_slice(args);
            Config::parse_from(argv).unwrap()
        };
        let plain = parse(&[]);
        assert_eq!(
            trust_root_users(&plain, &file, true),
            ["OAuth 2.0 issuer \"https://public\""]
        );
        assert!(trust_root_users(&plain, &file, false).is_empty());

        let https = ["--otel", "--otel-endpoint", "https://collector:4317"];
        assert_eq!(
            trust_root_users(&parse(&https), &AuthFile::default(), false),
            ["OTLP endpoint \"https://collector:4317\""]
        );
        let with_ca = [&https[..], &["--otel-ca-file", "/ca.pem"]].concat();
        assert!(trust_root_users(&parse(&with_ca), &AuthFile::default(), false).is_empty());
        let http = ["--otel", "--otel-endpoint", "http://collector:4317"];
        assert!(trust_root_users(&parse(&http), &AuthFile::default(), false).is_empty());
        let off = ["--otel-endpoint", "https://collector:4317"];
        assert!(trust_root_users(&parse(&off), &AuthFile::default(), false).is_empty());
    }

    #[test]
    fn a_node_without_a_policy_refuses_to_start() {
        let Err(e) = try_security(&[]) else {
            panic!("the node started without a policy");
        };
        assert!(e.to_string().contains("--auth-policy"), "{e}");
    }

    #[test]
    fn an_explicit_none_policy_starts() {
        let s = try_security(&["--auth-policy", "none"]).unwrap();
        assert_eq!(s.policy, Policy::None);
    }

    #[test]
    fn a_certificate_identity_needs_a_matching_rule_when_rules_exist() {
        let open = security(&[]);
        let grant = open.cert_grant("spiffe://test/app".into()).unwrap();
        assert_eq!(grant.scopes, Scopes::READ_WRITE);

        let ruled = Security {
            cert_rules: vec![("spiffe://test/ro*".into(), Scopes::READ)],
            ..open
        };
        let grant = ruled
            .cert_grant("spiffe://test/ro-reporting".into())
            .unwrap();
        assert_eq!(grant.identity, "spiffe://test/ro-reporting");
        assert_eq!(grant.scopes, Scopes::READ);
        assert!(ruled.cert_grant("spiffe://test/app".into()).is_none());
        assert!(ruled.cert_grant(String::new()).is_none());
    }

    #[test]
    fn a_read_only_connection_cannot_write() {
        let s = Security {
            policy: Policy::Any(vec![Factor::Mtls]),
            ..security(&[])
        };
        let mut conn = ConnState::new(1, ConnAuth::default());
        let get = crate::counter::cmd(&["GET", "k"]);
        let set = crate::counter::cmd(&["SET", "k", "v"]);
        assert_eq!(
            s.authorize(&mut conn, spec(b"GET"), &get),
            Err(CommandError::NoAuth)
        );
        conn.auth.cert = Some(CertGrant {
            identity: "rw".into(),
            scopes: Scopes::READ_WRITE,
        });
        assert_eq!(s.authorize(&mut conn, spec(b"SET"), &set), Ok(()));
        conn.auth.cert = Some(CertGrant {
            identity: "ro".into(),
            scopes: Scopes::READ,
        });
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), Ok(()));
        assert_eq!(
            s.authorize(&mut conn, spec(b"SET"), &set),
            Err(CommandError::NoPerm("set".into()))
        );
        assert_eq!(
            CommandError::NoPerm("set".into()).text(),
            "NOPERM this user has no permissions to run the 'set' command"
        );
    }

    #[test]
    fn a_failed_acl_reload_keeps_the_users() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("users.acl");
        std::fs::write(&file, "user a on >old ~* +@all\n").unwrap();
        let s = security(&["--acl-file", file.to_str().unwrap()]);
        let accepts = |password: &[u8]| {
            let mut conn = ConnState::new(1, ConnAuth::default());
            s.authenticate(&mut conn, Some(b"a"), password).is_ok()
        };
        assert!(accepts(b"old"));

        std::fs::write(&file, "usr a\n").unwrap();
        assert!(s.reload_acl().is_err());
        assert!(accepts(b"old"));
        std::fs::remove_file(&file).unwrap();
        assert!(s.reload_acl().is_err());
        assert!(accepts(b"old"));

        std::fs::write(&file, "user a on >new ~* +@all\n").unwrap();
        s.reload_acl().unwrap();
        assert!(accepts(b"new"));
        assert!(!accepts(b"old"));
    }

    /// A node with the user `u` (password `p`) and the policy `policy`.
    fn with_user(rules: &str, policy: &str) -> Security {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("users.acl");
        std::fs::write(&file, format!("user u on >p {rules}\n")).unwrap();
        let s = security(&["--acl-file", file.to_str().unwrap()]);
        Security {
            policy: Policy::parse(policy).unwrap(),
            ..s
        }
    }

    fn user_conn(s: &Security, scopes: Option<Scopes>) -> ConnState {
        let mut conn = ConnState::new(1, ConnAuth::default());
        conn.auth.cert = scopes.map(|scopes| CertGrant {
            identity: "c".into(),
            scopes,
        });
        s.authenticate(&mut conn, Some(b"u"), b"p").unwrap();
        conn
    }

    fn no_perm(name: &str) -> Result<(), CommandError> {
        Err(CommandError::NoPerm(name.into()))
    }

    #[test]
    fn all_needs_the_scopes_and_the_rules() {
        let get = crate::counter::cmd(&["GET", "k"]);
        let set = crate::counter::cmd(&["SET", "k", "v"]);

        let s = with_user("~k +@all", "all:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ));
        assert_eq!(s.authorize(&mut conn, spec(b"SET"), &set), no_perm("set"));
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), Ok(()));

        let s = with_user("~k +get", "all:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ_WRITE));
        assert_eq!(s.authorize(&mut conn, spec(b"SET"), &set), no_perm("set"));
        let other = crate::counter::cmd(&["GET", "other"]);
        assert_eq!(
            s.authorize(&mut conn, spec(b"GET"), &other),
            Err(CommandError::NoPermKey)
        );
    }

    #[test]
    fn the_rule_of_a_user_decides_a_subcommand_that_shows_the_users() {
        let list = crate::counter::cmd(&["ACL", "LIST"]);
        let acl = spec(b"ACL");

        // `all`: the certificate has no admin scope, and the rule of the user is enough.
        let s = with_user("~k +@all", "all:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ_WRITE));
        assert_eq!(s.authorize(&mut conn, acl, &list), Ok(()));
        let s = with_user("~k +@connection", "all:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ_WRITE.union(Scopes::ADMIN)));
        assert_eq!(s.authorize(&mut conn, acl, &list), no_perm("acl|list"));

        // `any`: a certificate without the admin scope never opens it for a user without the rule.
        let s = with_user("~k +@connection", "any:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ_WRITE));
        assert_eq!(s.authorize(&mut conn, acl, &list), no_perm("acl|list"));
        let mut conn = user_conn(&s, Some(Scopes::ADMIN));
        assert_eq!(s.authorize(&mut conn, acl, &list), Ok(()));
        let s = with_user("~k +acl|list", "any:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ_WRITE));
        assert_eq!(s.authorize(&mut conn, acl, &list), Ok(()));
    }

    #[test]
    fn a_login_replaces_the_earlier_one_only_with_an_any_policy() {
        let token = || TokenGrant {
            raw: bytes::Bytes::new(),
            exp: i64::MAX,
            nbf: None,
            scopes: Scopes::READ_WRITE,
            generation: None,
            recheck: None,
        };
        for (policy, kept) in [
            ("any:oauth2,password", false),
            ("all:oauth2,password", true),
        ] {
            let s = with_user("~k +get", policy);
            let mut conn = ConnState::new(1, ConnAuth::default());
            conn.auth.token = Some(token());
            s.authenticate(&mut conn, Some(b"u"), b"p").unwrap();
            assert!(conn.auth.user.is_some(), "{policy}");
            assert_eq!(conn.auth.token.is_some(), kept, "{policy}");
        }
    }

    #[test]
    fn any_needs_the_scopes_or_the_rules() {
        let set = crate::counter::cmd(&["SET", "k", "v"]);

        let s = with_user("~k +@all", "any:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ));
        assert_eq!(s.authorize(&mut conn, spec(b"SET"), &set), Ok(()));

        let s = with_user("~k +get", "any:mtls,password");
        let mut conn = user_conn(&s, Some(Scopes::READ));
        assert_eq!(s.authorize(&mut conn, spec(b"SET"), &set), no_perm("set"));

        let mut conn = user_conn(&s, None);
        let get = crate::counter::cmd(&["GET", "k"]);
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), Ok(()));
        assert_eq!(s.authorize(&mut conn, spec(b"SET"), &set), no_perm("set"));
    }

    #[test]
    fn tls_without_a_user_is_not_authenticated() {
        let s = with_user("~k +@all", "all:password");
        let mut conn = ConnState::new(1, ConnAuth::default());
        let get = crate::counter::cmd(&["GET", "k"]);
        assert_eq!(
            s.authorize(&mut conn, spec(b"GET"), &get),
            Err(CommandError::NoAuth)
        );
    }

    #[test]
    fn a_reload_that_turns_a_user_off_keeps_its_login_with_the_new_rules() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("users.acl");
        std::fs::write(&file, "user u on >p ~* +@all\n").unwrap();
        let s = security(&["--acl-file", file.to_str().unwrap()]);
        let mut conn = ConnState::new(1, ConnAuth::default());
        s.authenticate(&mut conn, Some(b"u"), b"p").unwrap();
        let get = crate::counter::cmd(&["GET", "k"]);
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), Ok(()));

        let old_grant = conn.auth.user.clone();
        std::fs::write(&file, "user u on >p ~* +@connection\n").unwrap();
        s.reload_acl().unwrap();
        conn.auth.user = old_grant;
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), no_perm("get"));

        // As in Valkey: `off` stops a new login, and an open connection keeps its login and
        // takes the new rules of the user.
        std::fs::write(&file, "user u off >p ~* +@connection\n").unwrap();
        s.reload_acl().unwrap();
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), no_perm("get"));
        std::fs::write(&file, "user u off >p ~* +@all\n").unwrap();
        s.reload_acl().unwrap();
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), Ok(()));
        assert!(!conn.closing);
        let mut fresh = ConnState::new(2, ConnAuth::default());
        assert!(s.authenticate(&mut fresh, Some(b"u"), b"p").is_err());
    }

    #[test]
    fn a_reload_closes_the_connections_of_a_removed_user_and_keeps_a_changed_password() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("users.acl");
        std::fs::write(&file, "user u on >p ~* +@all\nuser v on >p ~* +@all\n").unwrap();
        let s = security(&["--acl-file", file.to_str().unwrap()]);
        let get = crate::counter::cmd(&["GET", "k"]);
        let login = |user: &[u8]| {
            let mut conn = ConnState::new(1, ConnAuth::default());
            s.authenticate(&mut conn, Some(user), b"p").unwrap();
            conn
        };
        let (mut u, mut v) = (login(b"u"), login(b"v"));
        std::fs::write(&file, "user v on >new ~* +@all\n").unwrap();
        s.reload_acl().unwrap();
        assert_eq!(
            s.authorize(&mut u, spec(b"GET"), &get),
            Err(CommandError::NoAuth)
        );
        assert!(u.closing);
        assert_eq!(s.authorize(&mut v, spec(b"GET"), &get), Ok(()));
        assert!(!v.closing);
    }

    #[test]
    fn a_lost_user_keeps_a_connection_that_another_factor_authenticates() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("users.acl");
        std::fs::write(&file, "user u on >p ~* +@all\n").unwrap();
        let s = Security {
            policy: Policy::parse("any:password,mtls").unwrap(),
            ..security(&["--acl-file", file.to_str().unwrap()])
        };
        let mut conn = ConnState::new(1, ConnAuth::default());
        conn.auth.cert = Some(CertGrant {
            identity: "c".into(),
            scopes: Scopes::READ_WRITE,
        });
        s.authenticate(&mut conn, Some(b"u"), b"p").unwrap();
        std::fs::write(&file, "user w on >p ~* +@all\n").unwrap();
        s.reload_acl().unwrap();
        let get = crate::counter::cmd(&["GET", "k"]);
        assert_eq!(s.authorize(&mut conn, spec(b"GET"), &get), Ok(()));
        assert!(conn.auth.user.is_none());
        assert!(!conn.closing);
    }

    #[test]
    fn a_login_without_a_user_needs_the_admin_scope_to_show_the_users() {
        let s = Security {
            policy: Policy::Any(vec![Factor::Mtls]),
            ..security(&[])
        };
        let run = |scopes: Scopes, parts: &[&str]| {
            let mut conn = ConnState::new(1, ConnAuth::default());
            conn.auth.cert = Some(CertGrant {
                identity: "c".into(),
                scopes,
            });
            s.authorize(&mut conn, spec(b"ACL"), &crate::counter::cmd(parts))
        };
        let no_perm = |name: &str| Err(CommandError::NoPerm(name.into()));

        let rw = Scopes::READ_WRITE;
        assert_eq!(run(rw, &["ACL", "WHOAMI"]), Ok(()));
        assert_eq!(run(rw, &["ACL", "CAT"]), Ok(()));
        assert_eq!(run(rw, &["ACL", "BOGUS"]), Ok(()));
        assert_eq!(run(rw, &["ACL", "LIST"]), no_perm("acl|list"));
        assert_eq!(run(rw, &["acl", "users"]), no_perm("acl|users"));
        assert_eq!(run(rw, &["ACL", "GETUSER", "u"]), no_perm("acl|getuser"));
        assert_eq!(run(rw, &["ACL", "SETUSER", "u"]), no_perm("acl|setuser"));

        let admin = Scopes::READ.union(Scopes::ADMIN);
        assert_eq!(run(admin, &["ACL", "LIST"]), Ok(()));
        assert_eq!(run(admin, &["ACL", "GETUSER", "u"]), Ok(()));
    }
}
