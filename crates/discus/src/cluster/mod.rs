//! Routing: plan each command as local or forwarded, and run it (spec section 7).

pub mod forward;
pub mod multi;
pub mod route;
pub mod service;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use discus_placement::{EpochId, NodeId, Version};
use discus_resp::{Command, Value};
use discus_transport::Transport;
use discus_wire::{ForwardMode, TraceContext};
use futures::future::BoxFuture;
use parking_lot::Mutex;

use crate::command::{self, CommandError};
use crate::node::{ConnState, Node};
use crate::Config;
use route::{keys_of, route_of, Keys, Route};

/// A forward may hop at most this many times (spec section 7.2).
pub const MAX_HOPS: u8 = 2;

/// The node-to-node side of a node: the transport and the forwarding settings.
pub struct ClusterLink {
    pub transport: Transport,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub read_from_replicas: bool,
    /// The epoch that each peer last reported.
    peer_epochs: Mutex<HashMap<NodeId, EpochId>>,
}

impl ClusterLink {
    pub fn new(transport: Transport, config: &Config) -> ClusterLink {
        ClusterLink {
            transport,
            read_timeout: config.forward_read_timeout(),
            write_timeout: config.forward_write_timeout(),
            read_from_replicas: config.read_from_replicas,
            peer_epochs: Mutex::new(HashMap::new()),
        }
    }

    pub fn peer_epoch(&self, peer: NodeId) -> Option<EpochId> {
        self.peer_epochs.lock().get(&peer).copied()
    }

    /// Record the epoch a peer reported. Only a newer one replaces the record.
    pub fn saw_peer_epoch(&self, peer: NodeId, epoch: EpochId) {
        let mut map = self.peer_epochs.lock();
        let entry = map.entry(peer).or_insert(epoch);
        if epoch > *entry {
            *entry = epoch;
        }
    }
}

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

    pub fn error(e: CommandError) -> Reply {
        Reply::value(e.to_value())
    }

    pub fn try_again() -> Reply {
        Reply::error(CommandError::ClusterTryAgain)
    }
}

/// Where a command is on its way: forwards so far, the nodes that forwarded it, and the trace.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Hop {
    pub hops: u8,
    pub path: Vec<NodeId>,
    pub trace: TraceContext,
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

impl Dispatch {
    pub async fn finish(self) -> Reply {
        match self {
            Dispatch::Ready(r) => r,
            Dispatch::Pending(f) => f.await,
        }
    }
}

/// Check a command on the entry node: lookup, arity, authentication, and authorization.
/// Connection commands run here, because they read and change the connection state. Only the
/// entry node authorizes. A forwarded command arrives over cluster mTLS and is not checked again.
pub fn prepare(node: &Node, conn: &mut ConnState, cmd: Command) -> Prepared {
    let spec = match command::check(&cmd).and_then(|spec| {
        command::authorize(node, conn, spec)?;
        Ok(spec)
    }) {
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

/// Execute on this node and report the stamp that the command took.
pub fn local(node: &Node, cmd: &Command) -> Reply {
    Node::take_applied();
    let value = command::execute_local(node, cmd);
    Reply {
        value,
        applied: Node::take_applied(),
    }
}

/// Run a data command from the entry node.
pub fn run(node: &Arc<Node>, job: Job) -> Dispatch {
    run_hop(node, job, Hop::default())
}

/// Run a data command at any hop. A command that needs no other node runs inline.
pub fn run_hop(node: &Arc<Node>, job: Job, hop: Hop) -> Dispatch {
    if node.link().is_none() {
        return Dispatch::Ready(local(node, &job.cmd));
    }
    match job.route {
        Route::Single { write } => single(node, job, hop, write),
        Route::MultiRead { sum } => {
            let merge = if sum {
                multi::Merge::Sum
            } else {
                multi::Merge::InOrder
            };
            multi::multi(node, job.cmd, hop, job.rr, merge, false)
        }
        Route::MultiWrite { pairs } => {
            let merge = if pairs {
                multi::Merge::AllOk
            } else {
                multi::Merge::Sum
            };
            multi::multi(node, job.cmd, hop, job.rr, merge, true)
        }
        // Only the entry node fans out. A forwarded copy runs locally.
        Route::Fanout if hop.hops == 0 => multi::dbsize(node, job.cmd),
        Route::Broadcast if hop.hops == 0 => multi::flush(node, job.cmd),
        Route::Connection | Route::Local | Route::Fanout | Route::Broadcast => {
            Dispatch::Ready(local(node, &job.cmd))
        }
    }
}

/// The nodes that may serve `key`, in the order to try them. Empty when no member is `NORMAL`.
pub fn candidates(node: &Node, key: &[u8], write: bool, rr: u64) -> Vec<NodeId> {
    let owners = node.owners(key);
    let replicas = !write && node.link().is_some_and(|l| l.read_from_replicas);
    if !replicas {
        return owners.into_iter().take(1).collect();
    }
    if owners.contains(&node.id) {
        return vec![node.id];
    }
    let mut owners = owners;
    if !owners.is_empty() {
        let n = owners.len();
        owners.rotate_left((rr % n as u64) as usize);
    }
    owners
}

/// The budget is spent or every target is on the path. Serve locally if this node was the
/// required owner in the previous epoch, else `-TRYAGAIN`.
fn spent(node: &Node, cmd: &Command, keys: &[&[u8]], write: bool) -> Reply {
    let replicas = !write && node.link().is_some_and(|l| l.read_from_replicas);
    let owned_before = keys.iter().all(|key| match node.previous_owners(key) {
        Some(owners) if replicas => owners.contains(&node.id),
        Some(owners) => owners.first() == Some(&node.id),
        None => false,
    });
    if owned_before {
        local(node, cmd)
    } else {
        Reply::try_again()
    }
}

/// The targets that this hop may still forward to.
fn allowed(node: &Node, hop: &Hop, targets: Vec<NodeId>) -> Vec<NodeId> {
    if hop.hops >= MAX_HOPS {
        return Vec::new();
    }
    targets
        .into_iter()
        .filter(|t| *t != node.id && !hop.path.contains(t))
        .collect()
}

fn single(node: &Arc<Node>, job: Job, hop: Hop, write: bool) -> Dispatch {
    let key = job.cmd[1].clone();
    let targets = candidates(node, &key, write, job.rr);
    if targets.is_empty() {
        return Dispatch::Ready(Reply::try_again());
    }
    if targets[0] == node.id {
        return Dispatch::Ready(local(node, &job.cmd));
    }
    let targets = allowed(node, &hop, targets);
    if targets.is_empty() {
        return Dispatch::Ready(spent(node, &job.cmd, &[&key], write));
    }
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        for target in targets {
            match forward::send(&node, target, &job.cmd, &hop, ForwardMode::Route, write).await {
                Ok(reply) => return reply,
                // A read tries the next owner. A write fails at once: plan 4 adds hold-and-retry.
                Err(_) if !write => continue,
                Err(_) => break,
            }
        }
        Reply::try_again()
    }))
}
