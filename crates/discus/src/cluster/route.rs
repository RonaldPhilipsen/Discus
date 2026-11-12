//! How each command routes (spec sections 7 and 7.3).

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
        Route::Single { .. } => Keys::Some(vec![cmd[1].clone()]),
        Route::MultiRead { .. } | Route::MultiWrite { pairs: false } => {
            Keys::Some(cmd[1..].to_vec())
        }
        Route::MultiWrite { pairs: true } => {
            Keys::Some(cmd[1..].iter().step_by(2).cloned().collect())
        }
        Route::Rename => Keys::Some(vec![cmd[1].clone(), cmd[2].clone()]),
        Route::Local | Route::Fanout | Route::Broadcast => Keys::Barrier,
    }
}
