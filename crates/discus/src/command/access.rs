//! The read or write class of every command, for scope checks (spec §14.3).
//!
//! The classes live here, keyed by name, so the command tables stay unchanged. A test makes sure
//! that every registered command has a class.

/// What a command needs from the scopes of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// No data access. Authentication is enough.
    Connection,
    /// Reads data or node state. Needs `discus.read` or `discus.write`.
    Read,
    /// Changes data. Needs `discus.write`.
    Write,
}

const CONNECTION: &[&str] = &[
    "PING", "ECHO", "QUIT", "AUTH", "HELLO", "SELECT", "COMMAND", "CLIENT", "CLUSTER",
];

const READ: &[&str] = &[
    "INFO",
    "CONFIG",
    "GET",
    "MGET",
    "STRLEN",
    "EXISTS",
    "TYPE",
    "TTL",
    "PTTL",
    "SCAN",
    "RANDOMKEY",
    "DBSIZE",
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

/// Listed for the class test. Every name outside the other two lists is a write anyway.
#[cfg_attr(not(test), allow(dead_code))]
const WRITE: &[&str] = &[
    "SET",
    "GETDEL",
    "GETEX",
    "MSET",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "INCRBYFLOAT",
    "APPEND",
    "DEL",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "PERSIST",
    "FLUSHDB",
    "FLUSHALL",
    "HSET",
    "HSETNX",
    "HDEL",
    "HINCRBY",
    "SADD",
    "SREM",
    "SPOP",
    "ZADD",
    "ZREM",
    "ZINCRBY",
];

/// The class of a command by its upper-case name. An unknown name is a write, so a command that
/// a later change forgets to classify fails closed for read-only clients.
pub fn access(name: &str) -> Access {
    if CONNECTION.contains(&name) {
        Access::Connection
    } else if READ.contains(&name) {
        Access::Read
    } else {
        Access::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_command_has_a_class() {
        for name in crate::command::names() {
            let lists = [CONNECTION, READ, WRITE]
                .iter()
                .filter(|l| l.contains(&name))
                .count();
            assert_eq!(lists, 1, "{name} must be in exactly one class list");
        }
    }

    #[test]
    fn get_reads_and_set_writes() {
        assert_eq!(access("GET"), Access::Read);
        assert_eq!(access("SET"), Access::Write);
        assert_eq!(access("PING"), Access::Connection);
        assert_eq!(access("NEWCOMMAND"), Access::Write);
    }
}
