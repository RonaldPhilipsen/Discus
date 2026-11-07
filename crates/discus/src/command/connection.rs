//! Connection-level commands.

use discus_resp::{Command, Value};

use super::error::CommandError;
use super::table::CommandSpec;
use crate::node::{ConnState, Node};

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "PING",
        arity: -1,
        handler: ping,
        needs_auth: true,
    },
    CommandSpec {
        name: "ECHO",
        arity: 2,
        handler: echo,
        needs_auth: true,
    },
    CommandSpec {
        name: "QUIT",
        arity: -1,
        handler: quit,
        needs_auth: false,
    },
];

fn ping(_: &Node, _: &mut ConnState, cmd: &Command) -> Result<Value, CommandError> {
    match cmd.len() {
        1 => Ok(Value::simple("PONG")),
        2 => Ok(Value::Bulk(cmd[1].clone())),
        _ => Err(CommandError::WrongArity("PING".into())),
    }
}

fn echo(_: &Node, _: &mut ConnState, cmd: &Command) -> Result<Value, CommandError> {
    Ok(Value::Bulk(cmd[1].clone()))
}

fn quit(_: &Node, conn: &mut ConnState, _: &Command) -> Result<Value, CommandError> {
    conn.closing = true;
    Ok(Value::ok())
}
