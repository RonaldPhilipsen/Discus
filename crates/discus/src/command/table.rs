//! The command table and the dispatcher.

use std::collections::HashMap;
use std::sync::OnceLock;

use discus_resp::{Command, Value};

use super::error::CommandError;
use crate::node::{ConnState, Node};

pub type Handler = fn(&Node, &mut ConnState, &Command) -> Result<Value, CommandError>;

/// One command. A negative arity means "at least |arity|" arguments, counting the name.
pub struct CommandSpec {
    pub name: &'static str,
    pub arity: i32,
    pub handler: Handler,
    pub needs_auth: bool,
}

/// Every family of commands. Later tasks add their family here.
fn families() -> Vec<&'static [CommandSpec]> {
    vec![super::connection::COMMANDS, super::string::COMMANDS]
}

fn registry() -> &'static HashMap<&'static str, &'static CommandSpec> {
    static REGISTRY: OnceLock<HashMap<&'static str, &'static CommandSpec>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut map = HashMap::new();
        for family in families() {
            for spec in family {
                map.insert(spec.name, spec);
            }
        }
        map
    })
}

/// Look up a command by name, ignoring case.
pub fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    let upper = String::from_utf8_lossy(name).to_ascii_uppercase();
    registry().get(upper.as_str()).copied()
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
    "RENAME",
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

fn is_unsupported(upper: &str) -> bool {
    UNSUPPORTED.contains(&upper) || upper.starts_with('X') || upper.starts_with("GEO")
}

/// Run one command and return its reply.
pub fn dispatch(node: &Node, conn: &mut ConnState, cmd: &Command) -> Value {
    run(node, conn, cmd).unwrap_or_else(|e| e.to_value())
}

fn run(node: &Node, conn: &mut ConnState, cmd: &Command) -> Result<Value, CommandError> {
    let name = &cmd[0];
    let Some(spec) = lookup(name) else {
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
    if spec.needs_auth && !conn.authenticated {
        return Err(CommandError::NoAuth);
    }
    (spec.handler)(node, conn, cmd)
}
