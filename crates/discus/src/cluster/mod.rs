//! Routing: plan each command as local or forwarded, and run it (spec section 7).

pub mod forward;
pub mod hold;
pub mod multi;
pub mod redirect;
pub mod route;
pub mod service;
pub mod topology;
pub mod writeset;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use discus_placement::{ClusterId, EpochId, NodeId, Version};
use discus_resp::{Command, Value};
use discus_transport::Transport;
use discus_wire::TraceContext;
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use tracing::Span;

use crate::command::{self, CommandError, CommandSpec};
use crate::node::{ConnState, Node};
use crate::telemetry;
use crate::Config;
use route::{keys_of, Keys, Route};

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
    health: ForwardHealth,
}

/// The peers whose last forward failed (spec "Target health").
#[derive(Debug, Default)]
struct ForwardHealth {
    down: RwLock<HashSet<NodeId>>,
}

impl ForwardHealth {
    fn note(&self, peer: NodeId, ok: bool) {
        // Most forwards succeed to a peer that is up, so they take only the read lock.
        if ok == !self.down.read().contains(&peer) {
            return;
        }
        let mut down = self.down.write();
        if ok {
            down.remove(&peer);
        } else {
            down.insert(peer);
        }
    }

    fn is_down(&self, peer: NodeId) -> bool {
        self.down.read().contains(&peer)
    }
}

impl ClusterLink {
    pub fn new(transport: Transport, config: &Config) -> ClusterLink {
        ClusterLink {
            transport,
            read_timeout: config.forward_read_timeout(),
            write_timeout: config.forward_write_timeout(),
            read_from_replicas: config.read_from_replicas,
            peer_epochs: Mutex::new(HashMap::new()),
            health: ForwardHealth::default(),
        }
    }

    /// Record the result of a forward or a replication batch to `peer`. A timeout or a transport
    /// error is `false`.
    pub fn note_forward(&self, peer: NodeId, ok: bool) {
        self.health.note(peer, ok);
    }

    /// True while the last forward to `peer` failed.
    pub fn forward_down(&self, peer: NodeId) -> bool {
        self.health.is_down(peer)
    }

    pub fn peer_epoch(&self, peer: NodeId) -> Option<EpochId> {
        self.peer_epochs.lock().get(&peer).copied()
    }

    /// Forget the peers that `epoch` does not list, so departed nodes leave no record.
    pub fn forget_non_members(&self, epoch: &discus_placement::Epoch) {
        self.peer_epochs
            .lock()
            .retain(|p, _| epoch.member(*p).is_some());
        self.health
            .down
            .write()
            .retain(|p| epoch.member(*p).is_some());
    }

    /// Forget every peer epoch and every failed forward. A new cluster history starts its
    /// generations again, so records of the old history are not comparable (spec "Epoch changes").
    pub fn reset(&self) {
        self.peer_epochs.lock().clear();
        self.health.down.write().clear();
    }

    /// Record the epoch a peer reported on a connection of cluster `cluster`. Only a newer one
    /// replaces the record. A report from another cluster history drops the record instead.
    pub fn saw_peer_epoch(&self, peer: NodeId, cluster: ClusterId, epoch: EpochId) {
        let mut map = self.peer_epochs.lock();
        if cluster != self.transport.cluster_id() {
            map.remove(&peer);
            return;
        }
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
    /// A tracked read: the version that the read saw.
    pub observed: Option<Version>,
    /// A tracked read: every replica holds that version.
    pub settled: bool,
    /// The forward of a write failed. Set only when no hold covers it.
    pub failed: Option<Failure>,
}

/// How a forwarded write failed (spec section 7.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    /// The request never left this node, so a resend is always safe.
    pub unsent: bool,
}

impl Failure {
    pub fn of(e: &discus_transport::TransportError) -> Failure {
        Failure {
            unsent: e.is_unsent(),
        }
    }

    pub fn reply(self) -> Reply {
        Reply {
            failed: Some(self),
            ..Reply::try_again()
        }
    }
}

impl Reply {
    pub fn value(value: Value) -> Reply {
        Reply {
            value,
            applied: None,
            observed: None,
            settled: false,
            failed: None,
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
    /// The entry node keeps the key in a write set and wants `observed` and `settled`.
    pub track: bool,
    /// A retry of a held command. It does not hold again.
    pub held: bool,
}

/// A data command that passed the entry checks, ready to run once its keys are free.
#[derive(Debug)]
pub struct Job {
    pub cmd: Command,
    pub spec: &'static CommandSpec,
    /// The per-connection round-robin position for reads.
    pub rr: u64,
    /// The write set of the client connection. Forwarded jobs have none.
    pub writes: Option<Writes>,
    /// From a redirect connection, so hop 0 may return `-MOVED`.
    pub redirect: bool,
    /// The connection sent `READONLY`.
    pub readonly: bool,
}

pub type Writes = Arc<Mutex<writeset::WriteSet>>;

impl Job {
    pub fn keys(&self) -> Keys {
        keys_of(self.spec.route, &self.cmd)
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
pub fn prepare(
    node: &Node,
    conn: &mut ConnState,
    spec: Option<&'static CommandSpec>,
    cmd: Command,
) -> Prepared {
    let spec = match command::check(spec, &cmd).and_then(|spec| {
        node.security.authorize(conn, spec)?;
        Ok(spec)
    }) {
        Ok(spec) => spec,
        Err(e) => return Prepared::Done(e.to_value()),
    };
    if spec.route == Route::Connection {
        return Prepared::Done((spec.handler)(node, conn, &cmd).unwrap_or_else(|e| e.to_value()));
    }
    Prepared::Data(Job {
        cmd,
        spec,
        rr: conn.next_rr(),
        writes: conn.writes.clone(),
        redirect: conn.redirect,
        readonly: conn.readonly,
    })
}

/// Execute on this node and report the stamp that the command took. The work runs in the `store`
/// span. `Node::write` queues each key that the command changed for the other owners.
pub fn local(node: &Node, spec: &CommandSpec, cmd: &Command) -> Reply {
    let _store = telemetry::store_span().entered();
    Node::take_applied();
    let value = command::execute_local(node, spec, cmd);
    Reply {
        value,
        applied: Node::take_applied(),
        observed: None,
        settled: false,
        failed: None,
    }
}

/// Fill `observed` and `settled` of a read of `key` that ran here.
fn track_here(node: &Node, key: &[u8], reply: &mut Reply) {
    if reply.observed.is_none() {
        reply.observed = node.store.version(key);
        reply.settled = node.replicator().is_some_and(|r| r.settled(key));
    }
}

/// Serve a single-key command on this node. A replica without an entry for the key first asks
/// the other owners (read repair). `owners` are the current owners of the key.
pub fn serve_here(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    cmd: Command,
    write: bool,
    owners: &[NodeId],
) -> Dispatch {
    let key = cmd[1].clone();
    let sources = crate::replication::repair::sources(node, &key, write, owners);
    if sources.is_empty() {
        return Dispatch::Ready(local(node, spec, &cmd));
    }
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        crate::replication::repair::fetch(&node, &key, sources).await;
        local(&node, spec, &cmd)
    }))
}

/// Run a data command from the entry node. `route` is the `route` span: the owner choice runs
/// in it, and it ends before the command runs or forwards.
pub fn run(node: &Arc<Node>, job: Job, route: Span) -> Dispatch {
    run_hop(node, job, Hop::default(), route)
}

/// Run a data command at any hop. A command that needs no other node runs inline. A command on
/// counter keys goes to its group leader first, so it never takes the replicated path.
pub fn run_hop(node: &Arc<Node>, job: Job, hop: Hop, route: Span) -> Dispatch {
    if let Some(dispatch) = crate::counter::route::intercept(node, &job, &hop) {
        drop(route);
        return dispatch;
    }
    if job.spec.route == Route::Broadcast && hop.hops == 0 && node.counters().is_some() {
        drop(route);
        return crate::counter::route::broadcast(node, job.spec, job.cmd);
    }
    if node.link().is_none() {
        drop(route);
        return Dispatch::Ready(local(node, job.spec, &job.cmd));
    }
    match job.spec.route {
        Route::Single { write } => single(node, job, hop, write, route),
        Route::MultiRead { .. } | Route::MultiWrite { .. } => multi::multi(node, job, hop, route),
        // Only the entry node fans out. A forwarded copy runs locally. These routes need no
        // owner, so the route span ends at once.
        Route::Fanout if redirect::applies(job.redirect, &hop) && job.spec.name == "DBSIZE" => {
            drop(route);
            let count = node.store.len() as i64 / multi::copies(node);
            Dispatch::Ready(Reply::value(Value::Integer(count)))
        }
        Route::Fanout if hop.hops == 0 => {
            drop(route);
            multi::dbsize(node, job.spec, job.cmd)
        }
        Route::Local if redirect::applies(job.redirect, &hop) && job.spec.name == "SCAN" => {
            drop(route);
            Dispatch::Ready(Reply::value(command::keys::scan_primary(node, &job.cmd)))
        }
        Route::Connection | Route::Local | Route::Fanout | Route::Broadcast => {
            drop(route);
            Dispatch::Ready(local(node, job.spec, &job.cmd))
        }
    }
}

/// The nodes that may serve `key`, in the order to try them. Empty when no member is `NORMAL`.
///
/// A read that must see `need` (a key in the write set, spec section 7.4) goes to the primary,
/// unless this node owns the key and already holds a version at least as new.
pub fn candidates(
    node: &Node,
    key: &[u8],
    owners: &[NodeId],
    write: bool,
    rr: u64,
    need: Option<Version>,
) -> Vec<NodeId> {
    if let Some(need) = need.filter(|_| !write) {
        if owners.contains(&node.id) && node.store.version(key).is_some_and(|v| v >= need) {
            return vec![node.id];
        }
        return owners.iter().take(1).copied().collect();
    }
    let replicas = !write && node.link().is_some_and(|l| l.read_from_replicas);
    if !replicas {
        return owners.iter().take(1).copied().collect();
    }
    if owners.contains(&node.id) {
        return vec![node.id];
    }
    let mut owners = owners.to_vec();
    if !owners.is_empty() {
        let n = owners.len();
        owners.rotate_left((rr % n as u64) as usize);
    }
    owners
}

/// The budget is spent or every target is on the path. Serve locally if this node was the
/// required owner in the previous epoch, else `-TRYAGAIN`.
fn spent(node: &Node, spec: &CommandSpec, cmd: &Command, keys: &[&[u8]], write: bool) -> Reply {
    let replicas = !write && node.link().is_some_and(|l| l.read_from_replicas);
    let owned_before = keys.iter().all(|key| match node.previous_owners(key) {
        Some(owners) if replicas => owners.contains(&node.id),
        Some(owners) => owners.first() == Some(&node.id),
        None => false,
    });
    if owned_before {
        local(node, spec, cmd)
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

/// Where a single-key command goes.
enum Plan {
    /// No member is `NORMAL`.
    Retry,
    Local,
    /// The targets that this hop may still forward to. Empty when the budget is spent.
    Forward(Vec<NodeId>),
    /// Hop 0 of a redirect connection: another healthy node serves the slot.
    Moved(CommandError),
}

fn single(node: &Arc<Node>, job: Job, hop: Hop, write: bool, route: Span) -> Dispatch {
    let key = job.cmd[1].clone();
    let need = match (&job.writes, write) {
        (Some(w), false) => w.lock().required(&key, std::time::Instant::now()),
        _ => None,
    };
    let readonly = redirect::applies(job.redirect, &hop).then_some(job.readonly);
    let dispatch = single_planned(
        node, job.spec, job.cmd, hop, write, job.rr, need, readonly, route,
    );
    let Some(writes) = job.writes else {
        return dispatch;
    };
    // The write set changes before the key order releases the next command on this key.
    let note = move |reply: &Reply| {
        let mut w = writes.lock();
        match (write, reply.applied) {
            (true, Some(v)) if !matches!(reply.value, Value::Error(_)) => {
                w.record(&key, v, std::time::Instant::now())
            }
            (false, _) if need.is_some() => w.observed(&key, reply.observed, reply.settled),
            _ => {}
        }
    };
    match dispatch {
        Dispatch::Ready(reply) => {
            note(&reply);
            Dispatch::Ready(reply)
        }
        Dispatch::Pending(f) => Dispatch::Pending(Box::pin(async move {
            let reply = f.await;
            note(&reply);
            reply
        })),
    }
}

#[allow(clippy::too_many_arguments)]
fn single_planned(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    cmd: Command,
    mut hop: Hop,
    write: bool,
    rr: u64,
    need: Option<Version>,
    redirect: Option<bool>,
    route: Span,
) -> Dispatch {
    let key = cmd[1].clone();
    let (plan, owners) = route.in_scope(|| {
        let owners = node.owners(&key);
        let forward = || {
            let targets = candidates(node, &key, &owners, write, rr, need);
            match targets.first() {
                None => Plan::Retry,
                Some(first) if *first == node.id => Plan::Local,
                Some(_) => Plan::Forward(allowed(node, &hop, targets)),
            }
        };
        let plan = match redirect.map(|ro| redirect::target(node, &key, &owners, write, need, ro)) {
            None => forward(),
            Some(None) => Plan::Retry,
            Some(Some(t)) if t == node.id => Plan::Local,
            Some(Some(t)) => match redirect::moved(node, &key, t) {
                Some(moved) => Plan::Moved(moved),
                None => forward(),
            },
        };
        (plan, owners)
    });
    drop(route);
    let targets = match plan {
        Plan::Retry => return Dispatch::Ready(Reply::try_again()),
        Plan::Moved(moved) => return Dispatch::Ready(Reply::error(moved)),
        Plan::Local if need.is_some() || hop.track => {
            let mut reply = local(node, spec, &cmd);
            track_here(node, &key, &mut reply);
            return Dispatch::Ready(reply);
        }
        Plan::Local => return serve_here(node, spec, cmd, write, &owners),
        Plan::Forward(targets) => targets,
    };
    if targets.is_empty() {
        return Dispatch::Ready(spent(node, spec, &cmd, &[&key], write));
    }
    hop.track |= need.is_some();
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        for target in targets {
            if let Some(reply) = hold::send_or_hold(&node, spec, target, &cmd, &hop, write).await {
                return reply;
            }
        }
        Reply::try_again()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_down_follows_the_last_result() {
        let health = ForwardHealth::default();
        let p = NodeId::random();
        assert!(!health.is_down(p));
        health.note(p, false);
        assert!(health.is_down(p));
        health.note(p, true);
        assert!(!health.is_down(p));
    }
}
