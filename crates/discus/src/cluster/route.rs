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
    /// `COPY`: a source at argument 1 and a destination at argument 2, maybe in another database.
    Copy,
    /// `SCAN` and `RANDOMKEY`: the entry node only.
    Local,
    /// `DBSIZE`: a sum over every member.
    Fanout,
    /// `FLUSHDB` and `FLUSHALL`: every member, best-effort.
    Broadcast,
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
            Route::Connection | Route::Local | Route::Fanout | Route::Broadcast => (0, 0, 0),
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
}
