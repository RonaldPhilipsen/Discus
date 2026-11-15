use ctutils::CtEq;
use discus_resp::Command;

use crate::cluster::route::{key_perms, KeyPerm};
use crate::command::table::{CommandSpec, MAX_COMMANDS};
use crate::command::util::glob_match;

/// One bit for each command id.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommandSet([u64; MAX_COMMANDS / 64]);

impl CommandSet {
    pub fn set(&mut self, id: u16, allowed: bool) {
        let (word, bit) = (usize::from(id) / 64, id % 64);
        if allowed {
            self.0[word] |= 1 << bit;
        } else {
            self.0[word] &= !(1 << bit);
        }
    }

    pub fn contains(&self, id: u16) -> bool {
        self.0[usize::from(id) / 64] >> (id % 64) & 1 == 1
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPattern {
    pub pattern: Vec<u8>,
    pub perm: KeyPerm,
}

/// Why a user cannot run a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denied {
    Command,
    Key,
}

/// One user of the ACL file. A new user is off, with no password, no keys, and no commands.
#[derive(Clone, PartialEq, Eq)]
pub struct User {
    pub name: String,
    pub enabled: bool,
    pub nopass: bool,
    pub passwords: Vec<[u8; 32]>,
    pub commands: CommandSet,
    pub keys: Vec<KeyPattern>,
}

/// Leaves out the password hashes. An unsalted hash in a log helps an attacker guess.
impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("User")
            .field("name", &self.name)
            .field("enabled", &self.enabled)
            .field("nopass", &self.nopass)
            .field("commands", &self.commands)
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

impl User {
    pub fn new(name: &str) -> User {
        User {
            name: name.to_owned(),
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            commands: CommandSet::default(),
            keys: Vec::new(),
        }
    }

    /// True when the user is on and `hash` is the SHA-256 of one of its passwords, or the user
    /// has `nopass`.
    pub fn accepts(&self, hash: &[u8; 32]) -> bool {
        if !self.enabled {
            return false;
        }
        // No early return: the time must not reveal which stored hash matched.
        let mut found = false;
        for stored in &self.passwords {
            found |= bool::from(stored.as_slice().ct_eq(hash.as_slice()));
        }
        found || self.nopass
    }

    /// Check the command rule, then every key of `cmd`.
    ///
    /// Each key needs one pattern that matches it and grants the whole access. Two patterns do not
    /// add up: `%R~x` and `%W~x` together do not allow `INCR x`. This is the rule of Valkey.
    pub fn check(&self, spec: &CommandSpec, cmd: &Command) -> Result<(), Denied> {
        if !self.commands.contains(spec.id) {
            return Err(Denied::Command);
        }
        for (key, needed) in key_perms(spec, cmd) {
            let allowed = self
                .keys
                .iter()
                .any(|k| k.perm.contains(needed) && glob_match(&k.pattern, key));
            if !allowed {
                return Err(Denied::Key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use discus_resp::Command;

    use super::super::sha256;
    use super::*;
    use crate::cluster::route::KeyPerm;

    fn cmd(parts: &[&str]) -> Command {
        parts
            .iter()
            .map(|p| Bytes::copy_from_slice(p.as_bytes()))
            .collect()
    }

    fn check(user: &User, parts: &[&str]) -> Result<(), Denied> {
        let c = cmd(parts);
        user.check(crate::command::lookup(&c[0]).unwrap(), &c)
    }

    fn user(commands: &[&str], keys: &[(&str, KeyPerm)]) -> User {
        let mut u = User::new("u");
        u.enabled = true;
        for name in commands {
            u.commands
                .set(crate::command::lookup(name.as_bytes()).unwrap().id, true);
        }
        u.keys = keys
            .iter()
            .map(|(p, perm)| KeyPattern {
                pattern: p.as_bytes().to_vec(),
                perm: *perm,
            })
            .collect();
        u
    }

    #[test]
    fn the_command_rule_comes_first() {
        let u = user(&["GET"], &[("cache:*", KeyPerm::READ_WRITE)]);
        assert_eq!(check(&u, &["GET", "cache:a"]), Ok(()));
        assert_eq!(check(&u, &["SET", "cache:a", "1"]), Err(Denied::Command));
        assert_eq!(check(&u, &["GET", "other"]), Err(Denied::Key));
    }

    #[test]
    fn a_key_needs_one_pattern_with_the_whole_access() {
        let u = user(
            &["GET", "SET", "INCR", "MGET", "DBSIZE"],
            &[("r:*", KeyPerm::READ), ("w:*", KeyPerm::WRITE)],
        );
        assert_eq!(check(&u, &["GET", "r:1"]), Ok(()));
        assert_eq!(check(&u, &["SET", "r:1", "x"]), Err(Denied::Key));
        assert_eq!(check(&u, &["SET", "w:1", "x"]), Ok(()));
        assert_eq!(check(&u, &["SET", "w:1", "x", "GET"]), Err(Denied::Key));
        assert_eq!(check(&u, &["INCR", "w:1"]), Err(Denied::Key));
        assert_eq!(check(&u, &["MGET", "r:1", "w:1"]), Err(Denied::Key));
        assert_eq!(check(&u, &["DBSIZE"]), Ok(()));
    }

    #[test]
    fn keys_match_as_bytes() {
        let u = user(&["GET"], &[("bin:*", KeyPerm::READ)]);
        let c: Command = vec![
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"bin:\xff\x00"),
        ];
        assert_eq!(u.check(crate::command::lookup(b"GET").unwrap(), &c), Ok(()));
    }

    #[test]
    fn passwords_match_by_hash() {
        let mut u = User::new("u");
        u.passwords.push(sha256(b"first"));
        u.passwords.push(sha256(b"second"));
        assert!(!u.accepts(&sha256(b"first")), "the user is off");
        u.enabled = true;
        assert!(u.accepts(&sha256(b"first")));
        assert!(u.accepts(&sha256(b"second")));
        assert!(!u.accepts(&sha256(b"third")));
        u.passwords.clear();
        assert!(!u.accepts(&sha256(b"")));
        u.nopass = true;
        assert!(u.accepts(&sha256(b"anything")));
    }

    #[test]
    fn debug_hides_the_password_hashes() {
        let mut u = User::new("u");
        u.passwords.push(sha256(b"secret"));
        let text = format!("{u:?}");
        assert!(text.contains("\"u\""), "{text}");
        assert!(!text.contains(&hex::encode(sha256(b"secret"))), "{text}");
        let bytes = format!("{:?}", sha256(b"secret"));
        assert!(!text.contains(&bytes), "{text}");
    }
}
