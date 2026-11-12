//! How each command routes (spec sections 7 and 7.3).

use std::iter::StepBy;
use std::slice;

use bytes::Bytes;
use discus_resp::Command;

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
    /// `SCAN` and `RANDOMKEY`: the entry node only.
    Local,
    /// `DBSIZE`: a sum over every member.
    Fanout,
    /// `FLUSHDB` and `FLUSHALL`: every member, best-effort.
    Broadcast,
}

/// The keys that a command orders on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keys {
    None,
    Some(Vec<Bytes>),
    /// Waits for every earlier command, and every later command waits for it.
    Barrier,
}

/// The keys of `cmd` for its route. The arity is already checked.
pub fn keys_of(route: Route, cmd: &Command) -> Keys {
    match route {
        Route::Connection => Keys::None,
        Route::Local | Route::Fanout | Route::Broadcast => Keys::Barrier,
        _ => Keys::Some(
            key_args(route, cmd)
                .into_iter()
                .flatten()
                .cloned()
                .collect(),
        ),
    }
}

/// The key arguments of `cmd` for its route, borrowed, so a check of the keys copies nothing.
/// `None` for a route without keys. The arity is already checked.
pub fn key_args(route: Route, cmd: &Command) -> Option<StepBy<slice::Iter<'_, Bytes>>> {
    let (end, step) = match route {
        Route::Single { .. } => (2, 1),
        Route::MultiRead { .. } | Route::MultiWrite { pairs: false } => (cmd.len(), 1),
        Route::MultiWrite { pairs: true } => (cmd.len(), 2),
        Route::Rename => (3, 1),
        Route::Connection | Route::Local | Route::Fanout | Route::Broadcast => return None,
    };
    Some(cmd[1..end].iter().step_by(step))
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
        ] {
            assert_eq!(args(route, &mget), None);
        }
    }

    #[test]
    fn keys_of_owns_the_key_args() {
        let mset = cmd(&["MSET", "a", "1", "b", "2"]);
        let route = Route::MultiWrite { pairs: true };
        let owned = Keys::Some(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
        assert_eq!(keys_of(route, &mset), owned);
        assert_eq!(keys_of(Route::Connection, &mset), Keys::None);
        assert_eq!(keys_of(Route::Broadcast, &mset), Keys::Barrier);
    }
}
