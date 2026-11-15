//! Authentication factors, the client policy, and scopes (spec §14.2, §14.3).

use std::fmt;

use crate::command::table::Access;

/// One authentication factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Factor {
    /// The client presented a verified certificate.
    Mtls,
    /// The client sent a valid OAuth 2.0 token.
    Oauth2,
    /// The client logged in as a user of the ACL file.
    Password,
}

impl Factor {
    pub fn name(self) -> &'static str {
        match self {
            Factor::Mtls => "mtls",
            Factor::Oauth2 => "oauth2",
            Factor::Password => "password",
        }
    }

    fn parse(s: &str) -> Option<Factor> {
        [Factor::Mtls, Factor::Oauth2, Factor::Password]
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
                format!("--auth-policy: unknown factor {name:?}, use mtls, oauth2, or password")
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
        if factors.is_empty() {
            return Err("--auth-policy: list at least one factor".into());
        }
        Ok(combine(factors))
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

    /// True when these scopes are enough for a command or a subcommand with this access.
    pub fn allows(self, access: Access) -> bool {
        match access {
            Access::Public | Access::Connection => true,
            Access::Read => self.can_read(),
            Access::Write => self.can_write(),
            Access::Admin => self.is_admin(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Policy {
        Policy::parse(s).unwrap()
    }

    #[test]
    fn policies_parse_and_print() {
        let p = parse("all:mtls,oauth2");
        assert_eq!(p, Policy::All(vec![Factor::Mtls, Factor::Oauth2]));
        assert_eq!(p.to_string(), "all:mtls,oauth2");
        assert_eq!(parse("none"), Policy::None);
        assert!(parse("any:mtls,oauth2").uses(Factor::Mtls));
        assert!(Policy::parse("some:mtls").is_err());
        assert!(Policy::parse("any:").is_err());
        let p = parse("all:password");
        assert!(p.uses(Factor::Password));
        assert_eq!(p.to_string(), "all:password");
    }

    #[test]
    fn tls_is_not_a_factor() {
        for bad in ["all:tls", "any:tls,oauth2", "all:oauth2,tls"] {
            let e = Policy::parse(bad).unwrap_err();
            assert!(e.contains("unknown factor \"tls\""), "{bad}: {e}");
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
