//! The authentication state of one client connection.

use std::net::IpAddr;
use std::sync::Arc;

use bytes::Bytes;

use super::acl::User;
use super::policy::{Factor, Policy, Scopes};

/// A verified client certificate and the scopes its identity maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertGrant {
    pub identity: String,
    pub scopes: Scopes,
}

/// A valid OAuth 2.0 token. The raw token stays in memory so that the node can check it again
/// after a JWKS refresh. It is never logged.
#[derive(Clone)]
pub struct TokenGrant {
    pub raw: Bytes,
    pub exp: i64,
    pub nbf: Option<i64>,
    pub scopes: Scopes,
    /// The JWKS generation the token was last checked against. `None` for an introspected token.
    pub generation: Option<u64>,
    /// The end of the introspection answer, in Unix seconds. After it, the node introspects the
    /// token again before the next command. `None` for a JWT.
    pub recheck: Option<i64>,
}

impl std::fmt::Debug for TokenGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenGrant")
            .field("exp", &self.exp)
            .field("nbf", &self.nbf)
            .field("scopes", &self.scopes)
            .finish_non_exhaustive()
    }
}

/// A logged-in user of the ACL file.
#[derive(Debug, Clone)]
pub struct UserGrant {
    pub user: Arc<User>,
    /// The ACL generation that `user` comes from.
    pub generation: u64,
}

/// What the node knows about the client of one connection.
#[derive(Debug, Clone, Default)]
pub struct ConnAuth {
    /// The client address. In `lb` mode it is the address that the PROXY header carries.
    pub peer: Option<IpAddr>,
    /// The client used TLS.
    pub tls: bool,
    pub cert: Option<CertGrant>,
    /// The client sent the shared token.
    pub shared: bool,
    pub token: Option<TokenGrant>,
    /// Failed authentication attempts on this connection.
    pub failures: u32,
    pub user: Option<UserGrant>,
}

impl ConnAuth {
    pub fn new(peer: Option<IpAddr>) -> ConnAuth {
        ConnAuth {
            peer,
            ..ConnAuth::default()
        }
    }

    fn has(&self, f: Factor) -> bool {
        match f {
            Factor::Tls => self.tls,
            Factor::Mtls => self.cert.is_some(),
            Factor::Oauth2 => self.token.is_some(),
            Factor::SharedToken => self.shared,
            Factor::Password => self.user.is_some(),
        }
    }

    /// True when the connection meets the policy. `tls` alone never meets an `any` policy.
    pub fn authenticated(&self, policy: &Policy) -> bool {
        match policy {
            Policy::None => true,
            Policy::All(factors) => factors.iter().all(|f| self.has(*f)),
            Policy::Any(factors) => factors.iter().any(|f| *f != Factor::Tls && self.has(*f)),
        }
    }

    /// The scopes that one identity factor grants. `tls` grants nothing, and a user grant gives
    /// no scopes: its rules decide.
    fn grant(&self, f: Factor) -> Option<Scopes> {
        match f {
            Factor::Tls => None,
            Factor::Mtls => self.cert.as_ref().map(|c| c.scopes),
            Factor::Oauth2 => self.token.as_ref().map(|t| t.scopes),
            Factor::SharedToken => self.shared.then_some(Scopes::READ_WRITE),
            Factor::Password => None,
        }
    }

    /// The scopes from the certificate, the token, and the shared token. `None` when the policy is
    /// not met, or when none of them applies. A user grant gives no scopes: its rules decide.
    ///
    /// `all` takes the intersection of the identity factors, so the strictest factor wins. `any`
    /// takes the union of the factors that are present.
    pub fn scopes(&self, policy: &Policy) -> Option<Scopes> {
        match policy {
            Policy::None => Some(Scopes::READ_WRITE),
            Policy::All(factors) => {
                if !factors.iter().all(|f| self.has(*f)) {
                    return None;
                }
                factors
                    .iter()
                    .filter_map(|f| self.grant(*f))
                    .reduce(Scopes::intersect)
            }
            Policy::Any(factors) => factors
                .iter()
                .filter_map(|f| self.grant(*f))
                .reduce(Scopes::union),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(scopes: Scopes) -> TokenGrant {
        TokenGrant {
            raw: Bytes::new(),
            exp: 0,
            nbf: None,
            scopes,
            generation: None,
            recheck: None,
        }
    }

    #[test]
    fn tls_alone_never_meets_a_policy() {
        let a = ConnAuth {
            tls: true,
            ..ConnAuth::default()
        };
        let p = Policy::parse("all:tls,oauth2").unwrap();
        assert_eq!(a.scopes(&p), None);
    }

    #[test]
    fn all_takes_the_intersection() {
        let a = ConnAuth {
            cert: Some(CertGrant {
                identity: "app".into(),
                scopes: Scopes::READ,
            }),
            token: Some(token(Scopes::READ_WRITE)),
            ..ConnAuth::default()
        };
        let p = Policy::parse("all:mtls,oauth2").unwrap();
        assert_eq!(a.scopes(&p), Some(Scopes::READ));
    }

    #[test]
    fn any_takes_the_union() {
        let a = ConnAuth {
            cert: Some(CertGrant {
                identity: "app".into(),
                scopes: Scopes::READ,
            }),
            token: Some(token(Scopes::ADMIN)),
            ..ConnAuth::default()
        };
        let p = Policy::parse("any:mtls,oauth2").unwrap();
        assert_eq!(a.scopes(&p), Some(Scopes::READ.union(Scopes::ADMIN)));
        assert_eq!(ConnAuth::default().scopes(&p), None);
    }

    fn grant() -> UserGrant {
        UserGrant {
            user: Arc::new(User::new("u")),
            generation: 0,
        }
    }

    #[test]
    fn a_user_meets_the_password_factor_and_gives_no_scopes() {
        let a = ConnAuth {
            tls: true,
            user: Some(grant()),
            ..ConnAuth::default()
        };
        let all = Policy::parse("all:tls,password").unwrap();
        assert!(a.authenticated(&all));
        assert_eq!(a.scopes(&all), None);
        let tls_only = ConnAuth {
            tls: true,
            ..ConnAuth::default()
        };
        assert!(!tls_only.authenticated(&all));

        let any = Policy::parse("any:mtls,password").unwrap();
        assert!(a.authenticated(&any));
        assert!(!ConnAuth::default().authenticated(&any));
        let both = Policy::parse("all:mtls,password").unwrap();
        assert!(!a.authenticated(&both));
    }

    #[test]
    fn the_shared_token_grants_read_write() {
        let a = ConnAuth {
            shared: true,
            ..ConnAuth::default()
        };
        let p = Policy::parse("any:shared_token").unwrap();
        assert_eq!(a.scopes(&p), Some(Scopes::READ_WRITE));
    }
}
