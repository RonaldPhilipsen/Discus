use ctutils::CtEq;
use discus_resp::Command;

use crate::cluster::route::{key_perms, KeyPerm};
use crate::command::category::Categories;
use crate::command::table::{specs, CommandSpec, Subcommand, MAX_COMMANDS};
use crate::command::util::glob_match;

/// One bit for each id of a command or a subcommand.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct CommandSet([u64; MAX_COMMANDS / 64]);

impl CommandSet {
    fn set(&mut self, id: u16, allowed: bool) {
        let (word, bit) = (usize::from(id) / 64, id % 64);
        if allowed {
            self.0[word] |= 1 << bit;
        } else {
            self.0[word] &= !(1 << bit);
        }
    }

    fn contains(&self, id: u16) -> bool {
        self.0[usize::from(id) / 64] >> (id % 64) & 1 == 1
    }
}

/// The command rules of a user: the set of allowed commands and subcommands, and the rule text
/// that gives this set. Every change goes through one method, so the two never differ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRules {
    allowed: CommandSet,
    /// In the normal form that `ACL LIST` prints. The first rule is `-@all` or `+@all`.
    text: Vec<String>,
}

impl Default for CommandRules {
    /// The rules of a new user: no command.
    fn default() -> Self {
        CommandRules {
            allowed: CommandSet::default(),
            text: vec!["-@all".to_owned()],
        }
    }
}

impl CommandRules {
    /// True when the rules allow the command or subcommand with this id.
    pub fn contains(&self, id: u16) -> bool {
        self.allowed.contains(id)
    }

    /// The rules, as `ACL LIST` and `ACL GETUSER` print them.
    pub fn text(&self) -> String {
        self.text.join(" ")
    }

    /// `+@all` or `-@all`: every command and every subcommand.
    pub fn set_all(&mut self, allowed: bool) {
        for spec in specs() {
            self.set_bits(spec, allowed);
        }
        self.text = vec![if allowed { "+@all" } else { "-@all" }.to_owned()];
    }

    /// `+@category` or `-@category`, for one category. A subcommand follows its own categories
    /// and not those of its command. `+@connection` thus allows `ACL WHOAMI` and not `ACL LIST`.
    pub fn set_category(&mut self, category: Categories, allowed: bool) {
        for spec in specs() {
            if spec.categories.intersects(category) {
                self.allowed.set(spec.id, allowed);
            }
            for sub in spec.subcommands {
                if sub.categories.intersects(category) {
                    self.allowed.set(sub.id, allowed);
                }
            }
        }
        for name in category.names() {
            self.record(allowed, &format!("@{name}"));
        }
    }

    /// `+command` or `-command`: the command and each of its subcommands.
    pub fn set_command(&mut self, spec: &CommandSpec, allowed: bool) {
        self.set_bits(spec, allowed);
        self.record(allowed, &spec.rule_name(None));
    }

    /// `+command|subcommand` or `-command|subcommand`.
    pub fn set_subcommand(&mut self, spec: &CommandSpec, sub: &Subcommand, allowed: bool) {
        self.allowed.set(sub.id, allowed);
        self.record(allowed, &spec.rule_name(Some(sub)));
    }

    fn set_bits(&mut self, spec: &CommandSpec, allowed: bool) {
        self.allowed.set(spec.id, allowed);
        for sub in spec.subcommands {
            self.allowed.set(sub.id, allowed);
        }
    }

    /// Keep the rule in the normal form of Valkey: a new rule replaces an earlier rule for the
    /// same target, and a rule for a command also replaces the rules for its subcommands.
    fn record(&mut self, allowed: bool, target: &str) {
        self.text.retain(|rule| {
            let earlier = &rule[1..];
            earlier != target
                && !earlier
                    .strip_prefix(target)
                    .is_some_and(|rest| rest.starts_with('|'))
        });
        let sign = if allowed { '+' } else { '-' };
        self.text.push(format!("{sign}{target}"));
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
    pub commands: CommandRules,
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
            commands: CommandRules::default(),
            keys: Vec::new(),
        }
    }

    /// The flags of `ACL GETUSER`.
    pub fn flags(&self) -> Vec<&'static str> {
        let mut flags = vec![if self.enabled { "on" } else { "off" }];
        if self.nopass {
            flags.push("nopass");
        }
        flags
    }

    /// The command rules, as `ACL LIST` and `ACL GETUSER` print them.
    pub fn commands_text(&self) -> String {
        self.commands.text()
    }

    /// The key rules, as `ACL LIST` and `ACL GETUSER` print them.
    pub fn keys_text(&self) -> String {
        let rules: Vec<String> = self
            .keys
            .iter()
            .map(|key| {
                let prefix = match key.perm {
                    KeyPerm::READ => "%R~",
                    KeyPerm::WRITE => "%W~",
                    _ => "~",
                };
                format!("{prefix}{}", String::from_utf8_lossy(&key.pattern))
            })
            .collect();
        rules.join(" ")
    }

    /// The line of `ACL LIST`. It is a valid line of the ACL file, with the password hashes, and
    /// it gives this user again.
    pub fn line(&self) -> String {
        let mut words = vec!["user".to_owned(), self.name.clone()];
        words.extend(self.flags().into_iter().map(str::to_owned));
        words.extend(
            self.passwords
                .iter()
                .map(|hash| format!("#{}", hex::encode(hash))),
        );
        if !self.keys.is_empty() {
            words.push(self.keys_text());
        }
        // Discus has no pub/sub, so every user has the channel state of a new Valkey user.
        words.push("resetchannels".to_owned());
        words.push(self.commands_text());
        words.join(" ")
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

    /// Check the command rule, then every key of `cmd`. A subcommand with its own entry, such as
    /// `ACL LIST`, uses its own rule: `sub` is `spec.subcommand(cmd)`.
    ///
    /// Each key needs one pattern that matches it and grants the whole access. Two patterns do not
    /// add up: `%R~x` and `%W~x` together do not allow `INCR x`. This is the rule of Valkey.
    pub fn check(
        &self,
        spec: &CommandSpec,
        sub: Option<&Subcommand>,
        cmd: &Command,
    ) -> Result<(), Denied> {
        let id = sub.map_or(spec.id, |sub| sub.id);
        if !self.commands.contains(id) {
            return Err(Denied::Command);
        }
        for (key, needed) in key_perms(spec, cmd) {
            let allowed = self
                .keys
                .iter()
                // `allkeys` is the usual rule, and its match needs no walk over the key.
                .any(|k| {
                    k.perm.contains(needed) && (k.pattern == b"*" || glob_match(&k.pattern, key))
                });
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

    fn check(user: &User, parts: &[&str]) -> Result<(), Denied> {
        let c = crate::counter::cmd(parts);
        let spec = crate::command::lookup(&c[0]).unwrap();
        user.check(spec, spec.subcommand(&c), &c)
    }

    fn user(commands: &[&str], keys: &[(&str, KeyPerm)]) -> User {
        let mut u = User::new("u");
        u.enabled = true;
        for name in commands {
            u.commands
                .set_command(crate::command::lookup(name.as_bytes()).unwrap(), true);
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
        assert_eq!(
            u.check(crate::command::lookup(b"GET").unwrap(), None, &c),
            Ok(())
        );
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
