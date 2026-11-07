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
    /// `SCAN` and `RANDOMKEY`: the entry node only.
    Local,
    /// `DBSIZE`: a sum over every member.
    Fanout,
    /// `FLUSHDB` and `FLUSHALL`: every member, best-effort.
    Broadcast,
}

const CONNECTION: &[&str] = &[
    "PING", "ECHO", "QUIT", "AUTH", "HELLO", "SELECT", "COMMAND", "CLIENT", "INFO", "CONFIG",
    "CLUSTER",
];

const READS: &[&str] = &[
    "GET",
    "STRLEN",
    "TYPE",
    "TTL",
    "PTTL",
    "HGET",
    "HMGET",
    "HGETALL",
    "HEXISTS",
    "HLEN",
    "HKEYS",
    "HVALS",
    "SMEMBERS",
    "SISMEMBER",
    "SMISMEMBER",
    "SCARD",
    "SRANDMEMBER",
    "ZSCORE",
    "ZMSCORE",
    "ZCARD",
    "ZRANGE",
    "ZREVRANGE",
    "ZRANGEBYSCORE",
    "ZCOUNT",
];

/// The route of a supported command, by its upper-case name.
pub fn route_of(name: &str) -> Route {
    match name {
        "MGET" => Route::MultiRead { sum: false },
        "EXISTS" => Route::MultiRead { sum: true },
        "MSET" => Route::MultiWrite { pairs: true },
        "DEL" => Route::MultiWrite { pairs: false },
        "SCAN" | "RANDOMKEY" => Route::Local,
        "DBSIZE" => Route::Fanout,
        "FLUSHDB" | "FLUSHALL" => Route::Broadcast,
        n if CONNECTION.contains(&n) => Route::Connection,
        n => Route::Single {
            write: !READS.contains(&n),
        },
    }
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
        Route::Local | Route::Fanout | Route::Broadcast => Keys::Barrier,
    }
}
