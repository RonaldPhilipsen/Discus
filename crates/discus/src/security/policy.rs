//! Authentication factors, the client policy, and scopes (spec §14.2, §14.3).

use std::fmt;

/// One authentication factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Factor {
    /// The client presented a verified certificate.
    Mtls,
    /// The client sent a valid OAuth 2.0 token.
    Oauth2,
    /// The client sent the shared token.
    SharedToken,
    /// The client logged in as a user of the ACL file.
    Password,
}

impl Factor {
    pub fn name(self) -> &'static str {
        match self {
            Factor::Mtls => "mtls",
            Factor::Oauth2 => "oauth2",
            Factor::SharedToken => "shared_token",
            Factor::Password => "password",
        }
    }

    fn parse(s: &str) -> Option<Factor> {
        [
            Factor::Mtls,
            Factor::Oauth2,
            Factor::SharedToken,
            Factor::Password,
        ]
        .into_iter()
        .find(|f| f.name() == s)
    }
}

/// How the factors of a connection combine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Policy {
    /// Every TLS client is authenticated, with read-write access.
    None,
    /// At least one factor.
    Any(Vec<Factor>),
    /// Every factor.
    All(Vec<Factor>),
}

/// The result of `Policy::parse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed {
    pub policy: Policy,
    /// The list named the old factor `tls`, and the parser dropped it.
    pub dropped_tls: bool,
}

impl Policy {
    /// Parse `none`, `any:<factor>,<factor>`, or `all:<factor>,<factor>`.
    ///
    /// Every client uses TLS, so `tls` is no longer a factor. Old configurations still name it:
    /// `all:` drops it, and `any:` rejects it, because `tls` alone would meet that policy.
    pub fn parse(s: &str) -> Result<Parsed, String> {
        let s = s.trim();
        if s == "none" {
            return Ok(Parsed {
                policy: Policy::None,
                dropped_tls: false,
            });
        }
        let (kind, list) = s.split_once(':').ok_or_else(|| {
            format!("--auth-policy {s:?}: use none, any:<factors>, or all:<factors>")
        })?;
        let mut factors = Vec::new();
        let mut dropped_tls = false;
        for name in list.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            if name == "tls" {
                dropped_tls = true;
                continue;
            }
            let f = Factor::parse(name).ok_or_else(|| {
                format!(
                    "--auth-policy: unknown factor {name:?}, use mtls, oauth2, shared_token, or \
                     password"
                )
            })?;
            if !factors.contains(&f) {
                factors.push(f);
            }
        }
        let combine = match kind {
            "any" => Policy::Any,
            "all" => Policy::All,
            _ => {
                return Err(format!(
                    "--auth-policy: unknown combinator {kind:?}, use any or all"
                ))
            }
        };
        if dropped_tls && (kind == "any" || factors.is_empty()) {
            return Err(
                "--auth-policy: tls is no longer a factor, because every client uses TLS, so \
                 this policy would let every client in. For TLS without credentials, use none. \
                 To ask for credentials, remove tls from the list"
                    .into(),
            );
        }
        if factors.is_empty() {
            return Err("--auth-policy: list at least one factor".into());
        }
        Ok(Parsed {
            policy: combine(factors),
            dropped_tls,
        })
    }

    pub fn uses(&self, factor: Factor) -> bool {
        match self {
            Policy::None => false,
            Policy::Any(f) | Policy::All(f) => f.contains(&factor),
        }
    }
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, factors) = match self {
            Policy::None => return f.write_str("none"),
            Policy::Any(x) => ("any", x),
            Policy::All(x) => ("all", x),
        };
        let names: Vec<&str> = factors.iter().map(|x| x.name()).collect();
        write!(f, "{kind}:{}", names.join(","))
    }
}

/// A set of Discus scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Scopes(u8);

impl Scopes {
    pub const NONE: Scopes = Scopes(0);
    pub const READ: Scopes = Scopes(1);
    pub const WRITE: Scopes = Scopes(2);
    pub const ADMIN: Scopes = Scopes(4);
    pub const READ_WRITE: Scopes = Scopes(3);

    /// The scopes among `names`. Names other than the three Discus scopes are ignored.
    pub fn from_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Scopes {
        let mut s = Scopes::NONE;
        for n in names {
            s = s.union(match n {
                "discus.read" => Scopes::READ,
                "discus.write" => Scopes::WRITE,
                "discus.admin" => Scopes::ADMIN,
                _ => Scopes::NONE,
            });
        }
        s
    }

    /// Like `from_names`, but an unknown name is an error. For configuration files.
    pub fn parse_names(names: &[String]) -> Result<Scopes, String> {
        for n in names {
            if Scopes::from_names([n.as_str()]).is_empty() {
                return Err(format!(
                    "unknown scope {n:?}, use discus.read, discus.write, or discus.admin"
                ));
            }
        }
        Ok(Scopes::from_names(names.iter().map(String::as_str)))
    }

    pub fn union(self, other: Scopes) -> Scopes {
        Scopes(self.0 | other.0)
    }

    pub fn intersect(self, other: Scopes) -> Scopes {
        // Write implies read, so expand before intersecting.
        let expand = |s: Scopes| {
            if s.can_write() {
                s.union(Scopes::READ)
            } else {
                s
            }
        };
        Scopes(expand(self).0 & expand(other).0)
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// `discus.write` implies `discus.read`.
    pub fn can_read(self) -> bool {
        self.0 & (Scopes::READ.0 | Scopes::WRITE.0) != 0
    }

    pub fn can_write(self) -> bool {
        self.0 & Scopes::WRITE.0 != 0
    }

    pub fn is_admin(self) -> bool {
        self.0 & Scopes::ADMIN.0 != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Policy {
        let parsed = Policy::parse(s).unwrap();
        assert!(!parsed.dropped_tls, "{s}");
        parsed.policy
    }

    #[test]
    fn policies_parse_and_print() {
        let p = parse("all:mtls,oauth2");
        assert_eq!(p, Policy::All(vec![Factor::Mtls, Factor::Oauth2]));
        assert_eq!(p.to_string(), "all:mtls,oauth2");
        assert_eq!(parse("none"), Policy::None);
        assert!(parse("any:mtls,oauth2").uses(Factor::Mtls));
        assert!(Policy::parse("some:mtls").is_err());
        assert!(Policy::parse("some:tls,mtls").is_err());
        assert!(Policy::parse("any:").is_err());
        let p = parse("all:password");
        assert!(p.uses(Factor::Password));
        assert_eq!(p.to_string(), "all:password");
    }

    #[test]
    fn the_old_tls_factor_is_dropped_from_all() {
        for old in ["all:tls,oauth2", "all:oauth2,tls", "all: tls , oauth2,tls"] {
            let parsed = Policy::parse(old).unwrap();
            assert!(parsed.dropped_tls, "{old}");
            assert_eq!(parsed.policy, parse("all:oauth2"), "{old}");
            assert_eq!(parsed.policy.to_string(), "all:oauth2");
        }
    }

    #[test]
    fn a_policy_that_tls_alone_would_meet_is_rejected() {
        for bad in ["all:tls", "any:tls", "any:tls,oauth2", "any:oauth2,tls"] {
            let e = Policy::parse(bad).unwrap_err();
            assert!(e.contains("use none"), "{bad}: {e}");
        }
    }

    #[test]
    fn write_implies_read() {
        assert!(Scopes::WRITE.can_read());
        assert!(!Scopes::READ.can_write());
        assert!(!Scopes::ADMIN.can_read());
        assert_eq!(Scopes::WRITE.intersect(Scopes::READ), Scopes::READ);
        assert_eq!(
            Scopes::from_names(["discus.read", "openid", "discus.admin"]),
            Scopes::READ.union(Scopes::ADMIN)
        );
        assert!(Scopes::parse_names(&["discus.root".into()]).is_err());
    }
}
