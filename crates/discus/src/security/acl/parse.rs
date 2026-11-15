//! The parser of the ACL file (spec "The file").

use std::collections::HashMap;
use std::sync::Arc;

use super::{sha256, Acl, KeyPattern, User};
use crate::cluster::route::KeyPerm;
use crate::command::category::Categories;
use crate::command::table::specs;

pub(super) fn parse(text: &str) -> Result<Acl, String> {
    let mut users: HashMap<String, Arc<User>> = HashMap::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let user = parse_line(line, &users).map_err(|e| format!("line {}: {e}", index + 1))?;
        users.insert(user.name.clone(), Arc::new(user));
    }
    if users.is_empty() {
        return Err("the file has no user".to_owned());
    }
    Ok(Acl { users })
}

/// A rule error gives the position of the rule and never its text, because a mistyped password
/// rule would put the password into the error and the log (spec "The file").
enum RuleError {
    Unknown,
    UnknownCommand,
    UnknownCategory,
    Subcommand,
    Invalid(String),
}

impl From<String> for RuleError {
    fn from(message: String) -> Self {
        RuleError::Invalid(message)
    }
}

impl From<&str> for RuleError {
    fn from(message: &str) -> Self {
        RuleError::Invalid(message.to_owned())
    }
}

fn parse_line(line: &str, seen: &HashMap<String, Arc<User>>) -> Result<User, String> {
    let mut words = line.split_ascii_whitespace();
    if words.next() != Some("user") {
        return Err("a line must start with \"user\"".to_owned());
    }
    let name = words.next().ok_or("the user has no name")?;
    if name.eq_ignore_ascii_case("oauth2") {
        return Err(format!(
            "the name {name:?} is reserved for AUTH OAUTH2 <token>"
        ));
    }
    if seen.contains_key(name) {
        return Err(format!("the user {name:?} is defined twice"));
    }
    let mut user = User::new(name);
    for (index, rule) in words.enumerate() {
        let word = index + 3;
        apply(&mut user, rule).map_err(|e| match e {
            RuleError::Unknown => format!("unknown rule at word {word}"),
            RuleError::UnknownCommand => format!("unknown command at word {word}"),
            RuleError::UnknownCategory => format!("unknown category at word {word}"),
            RuleError::Subcommand => {
                format!("rules for one subcommand are not supported (word {word})")
            }
            RuleError::Invalid(message) => message,
        })?;
    }
    Ok(user)
}

fn apply(user: &mut User, rule: &str) -> Result<(), RuleError> {
    match rule {
        "on" => user.enabled = true,
        "off" => user.enabled = false,
        "nopass" => {
            user.nopass = true;
            user.passwords.clear();
        }
        "resetpass" => {
            user.nopass = false;
            user.passwords.clear();
        }
        "allkeys" => add_key(user, "*", KeyPerm::READ_WRITE),
        "resetkeys" => user.keys.clear(),
        "allcommands" => set_all(user, true),
        "nocommands" => set_all(user, false),
        // Discus has no pub/sub, so channel rules are accepted with no effect (spec, "The file").
        "allchannels" | "resetchannels" => {}
        "reset" => *user = User::new(&user.name),
        _ => return apply_prefixed(user, rule),
    }
    Ok(())
}

fn apply_prefixed(user: &mut User, rule: &str) -> Result<(), RuleError> {
    if let Some(password) = rule.strip_prefix('>') {
        if password.is_empty() {
            return Err(RuleError::Unknown);
        }
        add_password(user, sha256(password.as_bytes()));
    } else if let Some(hash) = rule.strip_prefix('#') {
        let bytes = hex::decode(hash)
            .ok()
            .and_then(|b| <[u8; 32]>::try_from(b).ok())
            .ok_or("a password hash must be 64 hex digits")?;
        add_password(user, bytes);
    } else if let Some(pattern) = rule.strip_prefix('~') {
        add_key(user, pattern, KeyPerm::READ_WRITE);
    } else if let Some(rest) = rule.strip_prefix('%') {
        let (perm, pattern) = if let Some(p) = rest.strip_prefix("RW~") {
            (KeyPerm::READ_WRITE, p)
        } else if let Some(p) = rest.strip_prefix("R~") {
            (KeyPerm::READ, p)
        } else if let Some(p) = rest.strip_prefix("W~") {
            (KeyPerm::WRITE, p)
        } else {
            return Err(RuleError::Unknown);
        };
        add_key(user, pattern, perm);
    } else if rule.starts_with('&') {
        // Accepted with no effect, as `allchannels` is.
    } else if let Some(target) = rule.strip_prefix('+') {
        command_rule(user, target, true)?;
    } else if let Some(target) = rule.strip_prefix('-') {
        command_rule(user, target, false)?;
    } else {
        return Err(RuleError::Unknown);
    }
    Ok(())
}

fn add_password(user: &mut User, hash: [u8; 32]) {
    user.nopass = false;
    if !user.passwords.contains(&hash) {
        user.passwords.push(hash);
    }
}

fn add_key(user: &mut User, pattern: &str, perm: KeyPerm) {
    user.keys.push(KeyPattern {
        pattern: pattern.as_bytes().to_vec(),
        perm,
    });
}

fn set_all(user: &mut User, allowed: bool) {
    for spec in specs() {
        user.commands.set(spec.id, allowed);
    }
}

fn command_rule(user: &mut User, target: &str, allowed: bool) -> Result<(), RuleError> {
    if let Some(category) = target.strip_prefix('@') {
        if category.eq_ignore_ascii_case("all") {
            set_all(user, allowed);
            return Ok(());
        }
        let categories = Categories::parse(category).ok_or(RuleError::UnknownCategory)?;
        for spec in specs()
            .iter()
            .filter(|s| s.categories.intersects(categories))
        {
            user.commands.set(spec.id, allowed);
        }
        return Ok(());
    }
    if target.contains('|') {
        return Err(RuleError::Subcommand);
    }
    let spec = crate::command::lookup(target.as_bytes()).ok_or(RuleError::UnknownCommand)?;
    user.commands.set(spec.id, allowed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::sha256;
    use super::*;

    fn one(line: &str) -> Arc<User> {
        Acl::parse(line).unwrap().user(b"u").unwrap().clone()
    }

    fn allows(u: &User, name: &str) -> bool {
        u.commands
            .contains(crate::command::lookup(name.as_bytes()).unwrap().id)
    }

    fn error(text: &str) -> String {
        Acl::parse(text).unwrap_err()
    }

    #[test]
    fn a_new_user_is_off_and_empty() {
        let u = one("user u");
        assert_eq!(*u, User::new("u"));
    }

    #[test]
    fn passwords_are_kept_as_hashes() {
        let hash = hex::encode(sha256(b"second"));
        let u = one(&format!("user u on >first #{hash}"));
        assert!(u.enabled);
        assert_eq!(u.passwords, [sha256(b"first"), sha256(b"second")]);
        assert!(one("user u on >x nopass").passwords.is_empty());
        assert!(one("user u on >x nopass").nopass);
        let reset = one("user u on nopass >x resetpass");
        assert!(!reset.nopass && reset.passwords.is_empty());
    }

    #[test]
    fn command_rules_apply_from_left_to_right() {
        let u = one("user u +@all -@dangerous +flushdb");
        assert!(allows(&u, "GET") && allows(&u, "SET") && allows(&u, "FLUSHDB"));
        assert!(!allows(&u, "FLUSHALL") && !allows(&u, "CONFIG"));
        let u = one("user u allcommands -GET nocommands +@read -hget");
        assert!(allows(&u, "GET") && !allows(&u, "HGET") && !allows(&u, "SET"));
        assert!(!allows(&one("user u +@pubsub"), "GET"));
    }

    #[test]
    fn key_rules_keep_their_access() {
        use crate::cluster::route::KeyPerm as P;
        let pattern = |p: &str, perm| KeyPattern {
            pattern: p.as_bytes().to_vec(),
            perm,
        };
        let u = one("user u ~a:* %R~b:* %W~c:* %RW~d:* allkeys");
        assert_eq!(
            u.keys,
            [
                pattern("a:*", P::READ_WRITE),
                pattern("b:*", P::READ),
                pattern("c:*", P::WRITE),
                pattern("d:*", P::READ_WRITE),
                pattern("*", P::READ_WRITE),
            ]
        );
        assert!(one("user u ~a:* resetkeys").keys.is_empty());
    }

    #[test]
    fn channel_rules_have_no_effect() {
        assert_eq!(
            *one("user u &news:* allchannels resetchannels"),
            User::new("u")
        );
    }

    #[test]
    fn reset_gives_a_new_user() {
        assert_eq!(*one("user u on >x ~* +@all reset"), User::new("u"));
    }

    #[test]
    fn comments_blank_lines_and_crlf_are_fine() {
        let acl = Acl::parse("# users\r\n\r\nuser a on nopass\r\n  user b off  \r\n").unwrap();
        assert_eq!(acl.len(), 2);
        assert!(acl.user(b"a").unwrap().enabled);
        assert!(acl.user(b"A").is_none());
    }

    #[test]
    fn upper_case_hash_digits_give_the_same_bytes() {
        let hash = hex::encode(sha256(b"x")).to_ascii_uppercase();
        assert_eq!(one(&format!("user u #{hash}")).passwords, [sha256(b"x")]);
    }

    #[test]
    fn errors_name_the_line() {
        assert_eq!(
            error("user a on\nusr b on"),
            "line 2: a line must start with \"user\""
        );
        assert_eq!(error("user"), "line 1: the user has no name");
        assert_eq!(
            error("user a\nuser a"),
            "line 2: the user \"a\" is defined twice"
        );
        assert_eq!(
            error("user oauth2 on"),
            "line 1: the name \"oauth2\" is reserved for AUTH OAUTH2 <token>"
        );
        assert_eq!(error("user a +gte"), "line 1: unknown command at word 3");
        assert_eq!(
            error("user a on -eval"),
            "line 1: unknown command at word 4"
        );
        assert_eq!(
            error("user a +@strings"),
            "line 1: unknown category at word 3"
        );
        assert_eq!(
            error("user a #abc"),
            "line 1: a password hash must be 64 hex digits"
        );
        assert_eq!(
            error("user a on +config|get"),
            "line 1: rules for one subcommand are not supported (word 4)"
        );
        for rule in [
            "<pass",
            "!00",
            "(~a +get)",
            "clearselectors",
            "bogus",
            "%X~a",
            ">",
        ] {
            assert_eq!(
                error(&format!("user a {rule}")),
                "line 1: unknown rule at word 3"
            );
        }
        // A mistyped password can hold whitespace or start with a rule prefix. No error for a rule
        // holds the text of the rule.
        for (line, expected) in [
            ("user a on >my pass", "line 1: unknown rule at word 5"),
            ("user a on >my -pass", "line 1: unknown command at word 5"),
            ("user a on >my -@pass", "line 1: unknown category at word 5"),
            (
                "user a on >my +pa|ss",
                "line 1: rules for one subcommand are not supported (word 5)",
            ),
        ] {
            let e = error(line);
            assert_eq!(e, expected);
            assert!(!e.contains("pa"), "{e}");
        }
        assert_eq!(error("# nothing\n"), "the file has no user");
        assert_eq!(error("user a ON"), "line 1: unknown rule at word 3");
        assert_eq!(
            error("\u{feff}user a on"),
            "line 1: a line must start with \"user\""
        );
    }
}
