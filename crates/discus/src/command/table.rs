//! The command table and the dispatcher.

use std::collections::HashMap;
use std::sync::OnceLock;

use discus_resp::{Command, Value};

use super::error::CommandError;
use crate::cluster::route::Route;
use crate::node::{ConnState, Node};

/// What a command needs from the scopes of a connection (spec §14.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Runs before authentication: `QUIT`, `AUTH`, and `HELLO`.
    Public,
    /// No data access. Authentication is enough.
    Connection,
    /// Reads data or node state. Needs `discus.read` or `discus.write`.
    Read,
    /// Changes data. Needs `discus.write`.
    Write,
}

pub type Handler = fn(&Node, &mut ConnState, &Command) -> Result<Value, CommandError>;

/// One command. A negative arity means "at least |arity|" arguments, counting the name.
#[derive(Debug)]
pub struct CommandSpec {
    pub name: &'static str,
    pub arity: i32,
    pub handler: Handler,
    pub access: Access,
    pub route: Route,
    /// The `fast` flag of `COMMAND`, as Valkey sets it.
    pub fast: bool,
}

impl CommandSpec {
    /// A connection command. It runs on the entry node with the connection state.
    pub const fn connection(name: &'static str, arity: i32, handler: Handler) -> CommandSpec {
        CommandSpec {
            name,
            arity,
            handler,
            access: Access::Connection,
            route: Route::Connection,
            fast: false,
        }
    }

    /// A read of the key at argument 1.
    pub const fn read(name: &'static str, arity: i32, handler: Handler) -> CommandSpec {
        CommandSpec {
            name,
            arity,
            handler,
            access: Access::Read,
            route: Route::Single { write: false },
            fast: false,
        }
    }

    /// A write of the key at argument 1.
    pub const fn write(name: &'static str, arity: i32, handler: Handler) -> CommandSpec {
        CommandSpec {
            name,
            arity,
            handler,
            access: Access::Write,
            route: Route::Single { write: true },
            fast: false,
        }
    }

    pub const fn with_access(self, access: Access) -> CommandSpec {
        CommandSpec { access, ..self }
    }

    pub const fn with_route(self, route: Route) -> CommandSpec {
        CommandSpec { route, ..self }
    }

    pub const fn with_fast(self) -> CommandSpec {
        CommandSpec { fast: true, ..self }
    }
}

/// Keyed by the upper-case name bytes, so a lookup needs no UTF-8 check. The names are fixed, so
/// the map needs no defense against crafted keys and uses a fast hash.
type Registry = HashMap<&'static [u8], &'static CommandSpec, foldhash::fast::FixedState>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        [
            super::connection::COMMANDS,
            super::string::COMMANDS,
            super::keys::COMMANDS,
            super::hash::COMMANDS,
            super::set::COMMANDS,
            super::zset::COMMANDS,
        ]
        .into_iter()
        .flatten()
        .map(|spec| (spec.name.as_bytes(), spec))
        .collect()
    })
}

/// The names of every supported command, sorted.
pub fn names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = registry().values().map(|spec| spec.name).collect();
    names.sort_unstable();
    names
}

/// No command name is longer than this, so a longer name is unknown without a lookup.
const MAX_NAME: usize = 16;

/// Look up a command by name, ignoring case.
pub fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    let mut buf = [0u8; MAX_NAME];
    let upper = buf.get_mut(..name.len())?;
    upper.copy_from_slice(name);
    upper.make_ascii_uppercase();
    registry().get(&*upper).copied()
}

/// Names that Discus recognizes and rejects on purpose.
const UNSUPPORTED: &[&str] = &[
    "BITFIELD",
    "BITOP",
    "BLMOVE",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "DEBUG",
    "DISCARD",
    "EVAL",
    "EVALSHA",
    "EXEC",
    "FUNCTION",
    "MIGRATE",
    "MULTI",
    "PFADD",
    "PFCOUNT",
    "PSUBSCRIBE",
    "PUBLISH",
    "PUBSUB",
    "RENAMENX",
    "SCRIPT",
    "SINTERSTORE",
    "SORT",
    "SUBSCRIBE",
    "SUNIONSTORE",
    "SDIFFSTORE",
    "UNWATCH",
    "WAIT",
    "WATCH",
];

/// True for a name that Discus rejects on purpose. `upper` is the upper case name.
pub fn is_unsupported(upper: &str) -> bool {
    UNSUPPORTED.contains(&upper) || upper.starts_with('X') || upper.starts_with("GEO")
}

/// Run a checked data command on this node with no authorization check. The entry node checked
/// it, or the command arrived over mTLS from another node.
pub fn execute_local(node: &Node, spec: &CommandSpec, db: u32, cmd: &Command) -> Value {
    let mut internal = ConnState::internal(db);
    (spec.handler)(node, &mut internal, cmd).unwrap_or_else(|e| e.to_value())
}

/// Check the arity of `cmd`, whose name looked up as `spec`.
pub fn check(
    spec: Option<&'static CommandSpec>,
    cmd: &Command,
) -> Result<&'static CommandSpec, CommandError> {
    let Some(spec) = spec else {
        let name = &cmd[0];
        let upper = String::from_utf8_lossy(name).to_ascii_uppercase();
        return Err(if is_unsupported(&upper) {
            CommandError::Unsupported(upper)
        } else {
            CommandError::UnknownCommand(String::from_utf8_lossy(name).into_owned())
        });
    };
    let argc = cmd.len() as i32;
    let arity_ok = if spec.arity >= 0 {
        argc == spec.arity
    } else {
        argc >= -spec.arity
    };
    if !arity_ok {
        return Err(CommandError::WrongArity(spec.name.into()));
    }
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_reads_and_set_writes() {
        let access = |name: &[u8]| lookup(name).unwrap().access;
        assert_eq!(access(b"GET"), Access::Read);
        assert_eq!(access(b"SET"), Access::Write);
        assert_eq!(access(b"PING"), Access::Connection);
    }
}
