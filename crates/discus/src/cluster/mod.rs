//! Routing: plan each command as local or forwarded, and run it.

pub mod route;

use std::sync::Arc;

use discus_placement::Version;
use discus_resp::{Command, Value};
use futures::future::BoxFuture;

use crate::command;
use crate::node::{ConnState, Node};
use route::{keys_of, route_of, Keys, Route};

/// A reply and the HLC stamp that the command took, if it wrote.
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    pub value: Value,
    pub applied: Option<Version>,
}

impl Reply {
    pub fn value(value: Value) -> Reply {
        Reply {
            value,
            applied: None,
        }
    }
}

/// A data command that passed the entry checks, ready to run once its keys are free.
#[derive(Debug)]
pub struct Job {
    pub cmd: Command,
    pub route: Route,
    /// The per-connection round-robin position for reads.
    pub rr: u64,
}

impl Job {
    pub fn keys(&self) -> Keys {
        keys_of(self.route, &self.cmd)
    }
}

/// The outcome of the entry checks.
pub enum Prepared {
    /// A connection command or an error, already answered with the connection state.
    Done(Value),
    /// A data command to run.
    Data(Job),
}

/// The outcome of running a job.
pub enum Dispatch {
    Ready(Reply),
    Pending(BoxFuture<'static, Reply>),
}

/// Check a command on the entry node: lookup, arity, and authentication. Connection commands
/// run here, because they read and change the connection state.
pub fn prepare(node: &Node, conn: &mut ConnState, cmd: Command) -> Prepared {
    let spec = match command::check(conn, &cmd) {
        Ok(spec) => spec,
        Err(e) => return Prepared::Done(e.to_value()),
    };
    let route = route_of(spec.name);
    if route == Route::Connection {
        return Prepared::Done((spec.handler)(node, conn, &cmd).unwrap_or_else(|e| e.to_value()));
    }
    Prepared::Data(Job {
        cmd,
        route,
        rr: conn.next_rr(),
    })
}

/// Run a data command. A command that needs no other node runs inline.
pub fn run(node: &Arc<Node>, job: Job) -> Dispatch {
    Dispatch::Ready(Reply::value(command::execute_local(node, &job.cmd)))
}
