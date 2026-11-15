//! The command table and the dispatcher.

use std::collections::HashMap;
use std::sync::OnceLock;

use discus_resp::{Command, Value};

use super::category::Categories;
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
#[derive(Debug, Clone, Copy)]
pub struct CommandSpec {
    pub name: &'static str,
    pub arity: i32,
    pub handler: Handler,
    pub access: Access,
    pub route: Route,
    /// The `fast` flag of `COMMAND`, as Valkey sets it.
    pub fast: bool,
    /// The position in `specs()`. 0 in a `COMMANDS` array: only the registry sets it.
    pub id: u16,
    pub categories: Categories,
    /// A write command that also returns or uses the old value of its key.
    pub reads_old: bool,
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
            id: 0,
            reads_old: false,
            categories: Categories::EMPTY,
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
            id: 0,
            reads_old: false,
            categories: Categories::EMPTY,
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
            id: 0,
            reads_old: false,
            categories: Categories::EMPTY,
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

    pub const fn with_reads_old(self) -> CommandSpec {
        CommandSpec {
            reads_old: true,
            ..self
        }
    }

    pub const fn with_categories(self, categories: Categories) -> CommandSpec {
        CommandSpec { categories, ..self }
    }
}

/// The ids fit the per-user bit set of the ACL, which has this many bits.
pub const MAX_COMMANDS: usize = 256;

/// Keyed by the upper-case name bytes, so a lookup needs no UTF-8 check. The names are fixed, so
/// the map needs no defense against crafted keys and uses a fast hash.
type ByName = HashMap<&'static [u8], &'static CommandSpec, foldhash::fast::FixedState>;

struct Registry {
    specs: &'static [CommandSpec],
    by_name: ByName,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let modules: [(&[CommandSpec], Categories); 6] = [
            (super::connection::COMMANDS, Categories::CONNECTION),
            (super::string::COMMANDS, Categories::STRING),
            (super::keys::COMMANDS, Categories::KEYSPACE),
            (super::hash::COMMANDS, Categories::HASH),
            (super::set::COMMANDS, Categories::SET),
            (super::zset::COMMANDS, Categories::SORTEDSET),
        ];
        let mut all: Vec<CommandSpec> = modules
            .into_iter()
            .flat_map(|(specs, module)| {
                specs.iter().map(move |spec| CommandSpec {
                    categories: categories_of(spec, module),
                    ..*spec
                })
            })
            .collect();
        all.sort_unstable_by_key(|spec| spec.name);
        assert!(
            all.len() <= MAX_COMMANDS,
            "more than {MAX_COMMANDS} commands"
        );
        for (i, spec) in all.iter_mut().enumerate() {
            spec.id = i as u16;
        }
        let specs: &'static [CommandSpec] = Vec::leak(all);
        Registry {
            specs,
            by_name: specs
                .iter()
                .map(|spec| (spec.name.as_bytes(), spec))
                .collect(),
        }
    })
}

/// `module` is the default of the module of `spec`, for a spec with no explicit categories.
fn categories_of(spec: &CommandSpec, module: Categories) -> Categories {
    let mut c = if spec.categories == Categories::EMPTY {
        module
    } else {
        spec.categories
    };
    if spec.access == Access::Write {
        c = c.union(Categories::WRITE);
    }
    // INFO and CONFIG read node state, not data. Valkey gives them no @read.
    if spec.access == Access::Read && spec.route != Route::Connection {
        c = c.union(Categories::READ);
    }
    c.union(if spec.fast {
        Categories::FAST
    } else {
        Categories::SLOW
    })
}

/// Every command, sorted by name. `specs()[i].id == i`.
pub fn specs() -> &'static [CommandSpec] {
    registry().specs
}

/// The names of every supported command, sorted.
pub fn names() -> Vec<&'static str> {
    registry().specs.iter().map(|spec| spec.name).collect()
}

/// No command name is longer than this, so a longer name is unknown without a lookup.
const MAX_NAME: usize = 16;

/// Look up a command by name, ignoring case.
pub fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    let mut buf = [0u8; MAX_NAME];
    let upper = buf.get_mut(..name.len())?;
    upper.copy_from_slice(name);
    upper.make_ascii_uppercase();
    registry().by_name.get(&*upper).copied()
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

    fn cats(name: &[u8]) -> Vec<&'static str> {
        lookup(name).unwrap().categories.names().collect()
    }

    #[test]
    fn commands_have_the_categories_of_valkey() {
        assert_eq!(cats(b"GET"), ["read", "string", "slow"]);
        assert_eq!(cats(b"SET"), ["write", "string", "slow"]);
        assert_eq!(cats(b"DEL"), ["keyspace", "write", "slow"]);
        assert_eq!(cats(b"HGET"), ["read", "hash", "slow"]);
        assert_eq!(cats(b"ZADD"), ["write", "sortedset", "slow"]);
        assert_eq!(
            cats(b"FLUSHALL"),
            ["keyspace", "write", "slow", "dangerous"]
        );
        assert_eq!(cats(b"SWAPDB"), ["keyspace", "write", "fast", "dangerous"]);
        assert_eq!(cats(b"PING"), ["fast", "connection"]);
        assert_eq!(cats(b"INFO"), ["slow", "dangerous"]);
        assert_eq!(cats(b"CONFIG"), ["admin", "slow", "dangerous"]);
        assert_eq!(cats(b"CLUSTER"), ["slow", "connection"]);
    }

    #[test]
    fn ids_are_the_positions_in_specs() {
        assert!(specs().len() <= MAX_COMMANDS);
        for (i, spec) in specs().iter().enumerate() {
            assert_eq!(usize::from(spec.id), i);
            assert_eq!(lookup(spec.name.as_bytes()).unwrap().id, spec.id);
        }
        assert!(specs().windows(2).all(|w| w[0].name < w[1].name));
    }
}
