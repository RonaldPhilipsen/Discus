//! Authentication factors, the client policy, and scopes (spec §14.2, §14.3).

use std::fmt;

/// One authentication factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Factor {
    /// The client connection uses TLS. Never an identity on its own.
    Tls,
    /// The client presented a verified certificate.
    Mtls,
    /// The client sent a valid OAuth 2.0 token.
    Oauth2,
    /// The client sent the shared token.
    SharedToken,
}

impl Factor {
    pub fn name(self) -> &'static str {
        match self {
            Factor::Tls => "tls",
            Factor::Mtls => "mtls",
            Factor::Oauth2 => "oauth2",
            Factor::SharedToken => "shared_token",
        }
    }

    fn parse(s: &str) -> Option<Factor> {
        [
            Factor::Tls,
            Factor::Mtls,
            Factor::Oauth2,
            Factor::SharedToken,
        ]
        .into_iter()
        .find(|f| f.name() == s)
    }
}

/// How the factors of a connection combine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Policy {
    /// Every client is authenticated, with read-write access. Development only.
    None,
    /// At least one factor.
    Any(Vec<Factor>),
    /// Every factor.
    All(Vec<Factor>),
}

impl Policy {
    /// Parse `none`, `any:<factor>,<factor>`, or `all:<factor>,<factor>`.
    pub fn parse(s: &str) -> Result<Policy, String> {
        let s = s.trim();
        if s == "none" {
            return Ok(Policy::None);
        }
        let (kind, list) = s.split_once(':').ok_or_else(|| {
            format!("--auth-policy {s:?}: use none, any:<factors>, or all:<factors>")
        })?;
        let mut factors = Vec::new();
        for name in list.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            let f = Factor::parse(name).ok_or_else(|| {
                format!(
                    "--auth-policy: unknown factor {name:?}, use tls, mtls, oauth2, or shared_token"
                )
            })?;
            if !factors.contains(&f) {
                factors.push(f);
            }
        }
        if factors.is_empty() {
            return Err("--auth-policy: list at least one factor".into());
        }
        let policy = match kind {
            "any" => Policy::Any(factors),
            "all" => Policy::All(factors),
            _ => {
                return Err(format!(
                    "--auth-policy: unknown combinator {kind:?}, use any or all"
                ))
            }
        };
        let tls_alone = match &policy {
            Policy::Any(f) => f.contains(&Factor::Tls),
            Policy::All(f) => f == &[Factor::Tls],
            Policy::None => false,
        };
        if tls_alone {
            return Err(
                "--auth-policy: tls is not an identity, so tls alone cannot authenticate a \
                 client. Combine it with all:, for example all:tls,oauth2"
                    .into(),
            );
        }
        Ok(policy)
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

    /// The scope names in the set.
    pub fn names(self) -> Vec<&'static str> {
        let mut out = Vec::new();
        for (bit, name) in [
            (Scopes::READ, "discus.read"),
            (Scopes::WRITE, "discus.write"),
            (Scopes::ADMIN, "discus.admin"),
        ] {
            if self.0 & bit.0 != 0 {
                out.push(name);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_parse_and_print() {
        let p = Policy::parse("all:tls,oauth2").unwrap();
        assert_eq!(p, Policy::All(vec![Factor::Tls, Factor::Oauth2]));
        assert_eq!(p.to_string(), "all:tls,oauth2");
        assert_eq!(Policy::parse("none").unwrap(), Policy::None);
        assert!(Policy::parse("any:mtls,oauth2").unwrap().uses(Factor::Mtls));
        assert!(Policy::parse("some:mtls").is_err());
        assert!(Policy::parse("any:").is_err());
    }

    #[test]
    fn tls_alone_is_rejected() {
        assert!(Policy::parse("all:tls").is_err());
        assert!(Policy::parse("any:tls,oauth2").is_err());
        assert!(Policy::parse("all:tls,mtls").is_ok());
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
