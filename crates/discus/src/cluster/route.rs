//! How each command routes (spec sections 7 and 7.3).

use std::iter::StepBy;
use std::slice;

use bytes::Bytes;
use discus_resp::Command;

use crate::command::table::{Access, CommandSpec};

/// The routing shape of a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// A connection command. It runs at once on the entry node with the connection state.
    Connection,
    /// One key at argument 1.
    Single { write: bool },
    /// `MGET` (merge in order) or `EXISTS` (sum).
    MultiRead { sum: bool },
    /// `MSET` (key-value pairs) or `DEL` (keys).
    MultiWrite { pairs: bool },
    /// `RENAME`: a source at argument 1 and a destination at argument 2.
    Rename,
    /// `COPY`: a source at argument 1 and a destination at argument 2, maybe in another database.
    Copy,
    /// `SCAN` and `RANDOMKEY`: the entry node only.
    Local,
    /// `DBSIZE`: a sum over every member.
    Fanout,
    /// `FLUSHDB` and `FLUSHALL`: every member, best-effort.
    Broadcast,
    /// `WAIT` and `WAITAOF`: the entry node only, after every earlier command of the connection.
    Wait,
}

impl Route {
    /// The key positions in the Redis `COMMAND` form: the first key, the last key (negative
    /// counts from the end), and the step. `(0, 0, 0)` for a route without keys.
    pub const fn key_spec(self) -> (i64, i64, i64) {
        match self {
            Route::Single { .. } => (1, 1, 1),
            Route::MultiRead { .. } | Route::MultiWrite { pairs: false } => (1, -1, 1),
            Route::MultiWrite { pairs: true } => (1, -1, 2),
            Route::Rename | Route::Copy => (1, 2, 1),
            Route::Connection | Route::Local | Route::Fanout | Route::Broadcast | Route::Wait => {
                (0, 0, 0)
            }
        }
    }
}

/// The key arguments of `cmd` for its route, borrowed, so a check of the keys copies nothing.
/// `None` for a route without keys. The arity is already checked.
pub fn key_args(route: Route, cmd: &Command) -> Option<StepBy<slice::Iter<'_, Bytes>>> {
    let (first, last, step) = route.key_spec();
    if step == 0 {
        return None;
    }
    let end = if last < 0 {
        cmd.len() as i64 + 1 + last
    } else {
        last + 1
    };
    Some(
        cmd[first as usize..end as usize]
            .iter()
            .step_by(step as usize),
    )
}

/// The access that a command needs on one key, as the `%R~` and `%W~` selectors of an ACL user
/// grant it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPerm(u8);

impl KeyPerm {
    pub const READ: KeyPerm = KeyPerm(1);
    pub const WRITE: KeyPerm = KeyPerm(2);
    pub const READ_WRITE: KeyPerm = KeyPerm(3);

    pub const fn contains(self, other: KeyPerm) -> bool {
        self.0 & other.0 == other.0
    }
}

/// Each key argument of `cmd` with the access that the command needs on it. Empty for a route
/// without keys. The arity is already checked.
pub fn key_perms<'a>(
    spec: &CommandSpec,
    cmd: &'a Command,
) -> impl Iterator<Item = (&'a Bytes, KeyPerm)> + 'a {
    let set_get = spec.name == "SET" && cmd.iter().skip(3).any(|a| a.eq_ignore_ascii_case(b"GET"));
    let default = match spec.access {
        Access::Write if spec.reads_old || set_get => KeyPerm::READ_WRITE,
        Access::Write => KeyPerm::WRITE,
        _ => KeyPerm::READ,
    };
    let route = spec.route;
    key_args(route, cmd)
        .into_iter()
        .flatten()
        .enumerate()
        .map(move |(i, key)| {
            let perm = match (route, i) {
                (Route::Copy, 0) => KeyPerm::READ,
                (Route::Copy, _) => KeyPerm::WRITE,
                (Route::Rename, 0) => KeyPerm::READ_WRITE,
                (Route::Rename, _) => KeyPerm::WRITE,
                _ => default,
            };
            (key, perm)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(args: &[&'static str]) -> Command {
        args.iter()
            .map(|a| Bytes::from_static(a.as_bytes()))
            .collect()
    }

    fn args(route: Route, c: &Command) -> Option<Vec<&[u8]>> {
        key_args(route, c).map(|it| it.map(|k| &k[..]).collect())
    }

    #[test]
    fn key_args_follow_the_route_shape() {
        let rename = cmd(&["RENAME", "a", "b"]);
        assert_eq!(
            args(Route::Rename, &rename),
            Some(vec![&b"a"[..], &b"b"[..]])
        );
        let set = cmd(&["SET", "k", "v", "EX", "10"]);
        assert_eq!(
            args(Route::Single { write: true }, &set),
            Some(vec![&b"k"[..]])
        );
        let mget = cmd(&["MGET", "a", "b", "c"]);
        let all = Some(vec![&b"a"[..], b"b", b"c"]);
        assert_eq!(args(Route::MultiRead { sum: false }, &mget), all);
        assert_eq!(args(Route::MultiWrite { pairs: false }, &mget), all);
        let mset = cmd(&["MSET", "a", "1", "b", "2"]);
        let pairs = Some(vec![&b"a"[..], b"b"]);
        assert_eq!(args(Route::MultiWrite { pairs: true }, &mset), pairs);
        for route in [
            Route::Connection,
            Route::Local,
            Route::Fanout,
            Route::Broadcast,
            Route::Wait,
        ] {
            assert_eq!(args(route, &mget), None);
        }
    }

    fn perms(parts: &[&str]) -> Vec<(String, KeyPerm)> {
        let cmd: Command = parts
            .iter()
            .map(|p| Bytes::copy_from_slice(p.as_bytes()))
            .collect();
        let spec = crate::command::lookup(&cmd[0]).unwrap();
        key_perms(spec, &cmd)
            .map(|(k, p)| (String::from_utf8_lossy(k).into_owned(), p))
            .collect()
    }

    #[test]
    fn each_key_gets_the_access_that_the_command_needs() {
        use KeyPerm as P;
        let k = |name: &str, p| (name.to_string(), p);
        assert_eq!(perms(&["GET", "a"]), [k("a", P::READ)]);
        assert_eq!(perms(&["SET", "a", "1"]), [k("a", P::WRITE)]);
        assert_eq!(
            perms(&["SET", "a", "1", "EX", "5", "get"]),
            [k("a", P::READ_WRITE)]
        );
        assert_eq!(perms(&["INCR", "a"]), [k("a", P::READ_WRITE)]);
        assert_eq!(perms(&["GETDEL", "a"]), [k("a", P::READ_WRITE)]);
        assert_eq!(
            perms(&["MGET", "a", "b"]),
            [k("a", P::READ), k("b", P::READ)]
        );
        assert_eq!(
            perms(&["MSET", "a", "1", "b", "2"]),
            [k("a", P::WRITE), k("b", P::WRITE)]
        );
        assert_eq!(
            perms(&["RENAME", "a", "b"]),
            [k("a", P::READ_WRITE), k("b", P::WRITE)]
        );
        assert_eq!(
            perms(&["COPY", "a", "b"]),
            [k("a", P::READ), k("b", P::WRITE)]
        );
        assert_eq!(perms(&["DBSIZE"]), []);
    }

    #[test]
    fn every_write_that_returns_old_data_is_marked() {
        for name in [
            "GETDEL",
            "GETEX",
            "INCR",
            "DECR",
            "INCRBY",
            "DECRBY",
            "INCRBYFLOAT",
            "HINCRBY",
            "ZINCRBY",
            "SPOP",
            "MOVE",
        ] {
            assert!(
                crate::command::lookup(name.as_bytes()).unwrap().reads_old,
                "{name}"
            );
        }
    }
}
