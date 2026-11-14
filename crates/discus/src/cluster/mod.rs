//! Routing: plan each command as local or forwarded, and run it (spec section 7).

pub mod forward;
pub mod hold;
pub mod multi;
pub mod redirect;
pub mod rename;
pub mod route;
pub mod service;
pub mod topology;
pub mod writeset;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use discus_placement::{ClusterId, EpochId, NodeId, Owners, Version};
use discus_resp::{Command, Value};
use discus_store::DbKey;
use discus_transport::Transport;
use discus_wire::TraceContext;
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use tracing::Span;

use crate::command::{self, CommandError, CommandSpec};
use crate::databases;
use crate::node::{ConnState, Node};
use crate::settings::Live;
use crate::telemetry;
use route::Route;
use writeset::SetKey;

/// A forward may hop at most this many times (spec section 7.2).
pub const MAX_HOPS: u8 = 2;

/// The node-to-node side of a node: the transport and the forwarding settings.
pub struct ClusterLink {
    pub transport: Transport,
    live_settings: Arc<Live>,
    /// The epoch that each peer last reported.
    peer_epochs: Mutex<HashMap<NodeId, EpochId>>,
    /// The peers whose last forward failed (spec "Target health").
    down: RwLock<HashSet<NodeId>>,
}

impl ClusterLink {
    pub fn new(transport: Transport, live_settings: Arc<Live>) -> ClusterLink {
        ClusterLink {
            transport,
            live_settings,
            peer_epochs: Mutex::new(HashMap::new()),
            down: RwLock::default(),
        }
    }

    pub fn read_timeout(&self) -> Duration {
        self.live_settings.get().forward_read_timeout
    }

    pub fn write_timeout(&self) -> Duration {
        self.live_settings.get().forward_write_timeout
    }

    pub fn read_from_replicas(&self) -> bool {
        self.live_settings.get().read_from_replicas
    }

    /// Record the result of a forward or a replication batch to `peer`. A timeout or a transport
    /// error is `false`.
    pub fn note_forward(&self, peer: NodeId, ok: bool) {
        // Most forwards succeed to a peer that is up, so they take only the read lock.
        if ok != self.forward_down(peer) {
            return;
        }
        let mut down = self.down.write();
        if ok {
            down.remove(&peer);
        } else {
            down.insert(peer);
        }
    }

    /// True while the last forward to `peer` failed.
    pub fn forward_down(&self, peer: NodeId) -> bool {
        self.down.read().contains(&peer)
    }

    pub fn peer_epoch(&self, peer: NodeId) -> Option<EpochId> {
        self.peer_epochs.lock().get(&peer).copied()
    }

    /// Forget the peers that `epoch` does not list, so departed nodes leave no record.
    pub fn forget_non_members(&self, epoch: &discus_placement::Epoch) {
        self.peer_epochs
            .lock()
            .retain(|p, _| epoch.member(*p).is_some());
        self.down.write().retain(|p| epoch.member(*p).is_some());
    }

    /// Forget every peer epoch and every failed forward. A new cluster history starts its
    /// generations again, so records of the old history are not comparable (spec "Epoch changes").
    pub fn reset(&self) {
        self.peer_epochs.lock().clear();
        self.down.write().clear();
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
    /// From a redirect connection, so hop 0 may return `-MOVED`.
    pub redirect: bool,
    /// The connection sent `READONLY`.
    pub readonly: bool,
}

pub type Writes = Arc<Mutex<writeset::WriteSet>>;

impl Job {
    /// A job without a client connection: a forwarded command or a held retry.
    pub fn plain(cmd: Command, spec: &'static CommandSpec) -> Job {
        Job {
            cmd,
            spec,
            rr: 0,
            redirect: false,
            readonly: false,
        }
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

    /// Run `f` on the reply once it is ready. `ctx` is cloned only when the reply is pending.
    pub fn then<T: Send + Sync + 'static>(
        self,
        ctx: &Arc<T>,
        f: impl FnOnce(&T, &mut Reply) + Send + 'static,
    ) -> Dispatch {
        match self {
            Dispatch::Ready(mut reply) => {
                f(ctx, &mut reply);
                Dispatch::Ready(reply)
            }
            Dispatch::Pending(fut) => {
                let ctx = ctx.clone();
                Dispatch::Pending(Box::pin(async move {
                    let mut reply = fut.await;
                    f(&ctx, &mut reply);
                    reply
                }))
            }
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
    // The cluster count can drop below the database that a connection selected earlier.
    if conn.db >= node.databases.cluster() {
        return Prepared::Done(Value::error(databases::OUT_OF_RANGE));
    }
    Prepared::Data(Job {
        cmd,
        spec,
        rr: conn.next_rr(),
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
        applied: Node::take_applied(),
        ..Reply::value(value)
    }
}

/// The keys of `cmd` in database `db` that must reach a secondary before the reply: the listed
/// keys of a command that deletes them. A deleted session must stay deleted when the primary
/// fails.
pub(crate) fn acked_delete_keys(
    node: &Node,
    spec: &CommandSpec,
    db: u32,
    cmd: &Command,
) -> Vec<DbKey> {
    if !matches!(spec.name, "DEL" | "GETDEL" | "DISCUS.TAKE") {
        return Vec::new();
    }
    cmd[1..]
        .iter()
        .filter(|k| node.is_primary_read_key(k))
        .map(|k| DbKey::new(db, k.clone()))
        .collect()
}

/// Run `cmd` here like [`local`]. A delete of a listed key then waits for one secondary. After
/// the wait it replies with success anyway (fail open), and the hints deliver the tombstone.
pub fn local_acked(node: &Arc<Node>, spec: &'static CommandSpec, cmd: Command) -> Dispatch {
    let reply = local(node, spec, &cmd);
    if reply.applied.is_none() {
        return Dispatch::Ready(reply);
    }
    let keys = acked_delete_keys(node, spec, 0, &cmd);
    let Some(replicator) = node.replicator().filter(|_| !keys.is_empty()).cloned() else {
        return Dispatch::Ready(reply);
    };
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let unacked = replicator.unacked_after_wait(&node, &keys).await;
        discus_telemetry::global().primary_read_delete_unacked(unacked);
        reply
    }))
}

/// Fill `observed` and `settled` of a read of `key` that ran here.
fn track_here(node: &Node, key: &[u8], reply: &mut Reply) {
    if reply.observed.is_none() {
        reply.observed = node.store.version(0, key);
        reply.settled = node.replicator().is_some_and(|r| r.settled(0, key));
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
    let sources = crate::replication::repair::sources(node, 0, &cmd[1], write, owners);
    if sources.is_empty() {
        return local_acked(node, spec, cmd);
    }
    let node = node.clone();
    let key = cmd[1].clone();
    Dispatch::Pending(Box::pin(async move {
        crate::replication::repair::fetch(&node, 0, &key, sources).await;
        local_acked(&node, spec, cmd).finish().await
    }))
}

/// Run a data command from the entry node. `route` is the `route` span: the owner choice runs
/// in it, and it ends before the command runs or forwards.
pub fn run(node: &Arc<Node>, job: Job, writes: Option<&Writes>, route: Span) -> Dispatch {
    // With one owner per key, a read reaches that owner whatever the write set requires.
    let writes = writes.filter(|_| node.config.replication_factor > 1);
    run_hop(node, job, Hop::default(), writes, route)
}

/// Run a data command at any hop. A command that needs no other node runs inline. A command on
/// counter keys goes to its group leader first, so it never takes the replicated path.
pub fn run_hop(
    node: &Arc<Node>,
    job: Job,
    hop: Hop,
    writes: Option<&Writes>,
    route: Span,
) -> Dispatch {
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
        Route::Single { write } => single(node, job, hop, write, writes, route),
        Route::MultiRead { .. } | Route::MultiWrite { .. } => {
            multi::multi(node, job, hop, writes, route)
        }
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
            Dispatch::Ready(Reply::value(command::keys::scan_primary(node, 0, &job.cmd)))
        }
        Route::Rename => {
            drop(route);
            rename::run(node, job, hop, writes)
        }
        Route::Connection | Route::Local | Route::Fanout | Route::Broadcast => {
            drop(route);
            Dispatch::Ready(local(node, job.spec, &job.cmd))
        }
    }
}

/// The nodes that may serve `key`, in the order to try them. Empty when no member is `NORMAL`.
///
/// A read of a primary-read key (`--primary-read-prefix`) goes to the primary only.
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
        if owners.contains(&node.id) && node.store.version(0, key).is_some_and(|v| v >= need) {
            return vec![node.id];
        }
        return owners.iter().take(1).copied().collect();
    }
    if !replica_reads(node, write, key) {
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

/// A read may go to any owner, with `--read-from-replicas`, unless the key reads from the primary
/// only (`--primary-read-prefix`).
pub(crate) fn replica_reads(node: &Node, write: bool, key: &[u8]) -> bool {
    !write && node.link().is_some_and(|l| l.read_from_replicas()) && !node.is_primary_read_key(key)
}

/// The budget is spent or every target is on the path. Serve locally if this node was the
/// required owner in the previous epoch, else `-TRYAGAIN`.
fn spent(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    cmd: Command,
    keys: &[&[u8]],
    write: bool,
) -> Dispatch {
    let replicas = keys.iter().all(|k| replica_reads(node, write, k));
    let owned_before = keys.iter().all(|key| match node.previous_owners(key) {
        Some(owners) if replicas => owners.contains(&node.id),
        Some(owners) => owners.first() == Some(&node.id),
        None => false,
    });
    if owned_before {
        local_acked(node, spec, cmd)
    } else {
        Dispatch::Ready(Reply::try_again())
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

fn single(
    node: &Arc<Node>,
    job: Job,
    hop: Hop,
    write: bool,
    writes: Option<&Writes>,
    route: Span,
) -> Dispatch {
    let owners = route.in_scope(|| node.owners(&job.cmd[1]));
    // A primary-read key keeps no write-set entry: every read of it goes to the primary.
    let tracked = writes.is_some() && !node.is_primary_read_key(&job.cmd[1]);
    // With one owner, every read goes to that owner whatever the write set requires, so the
    // write set is not asked.
    let need = match (writes, write) {
        (Some(w), false) if tracked && owners.len() > 1 => w
            .lock()
            .required(SetKey::of(&job.cmd[1]), std::time::Instant::now),
        _ => None,
    };
    // Only a write, or a read that must see an earlier write, changes the write set.
    let key = (tracked && (write || need.is_some())).then(|| SetKey::of(&job.cmd[1]));
    let dispatch = single_planned(node, job, owners, hop, write, need, route);
    let (Some(writes), Some(key)) = (writes, key) else {
        return dispatch;
    };
    // The write set changes before the key order releases the next command on this key.
    dispatch.then(writes, move |writes, reply| {
        let mut w = writes.lock();
        match (write, reply.applied) {
            (true, Some(v)) if !matches!(reply.value, Value::Error(_)) => {
                w.record(key, v, std::time::Instant::now())
            }
            (false, _) if need.is_some() => w.observed(key, reply.observed, reply.settled),
            _ => {}
        }
    })
}

fn single_planned(
    node: &Arc<Node>,
    job: Job,
    owners: Owners,
    mut hop: Hop,
    write: bool,
    need: Option<Version>,
    route: Span,
) -> Dispatch {
    let Job {
        cmd,
        spec,
        rr,
        redirect,
        readonly,
    } = job;
    let redirect = redirect::applies(redirect, &hop).then_some(readonly);
    let key: &[u8] = &cmd[1];
    let plan = route.in_scope(|| {
        let forward = || {
            let targets = candidates(node, key, &owners, write, rr, need);
            match targets.first() {
                None => Plan::Retry,
                Some(first) if *first == node.id => Plan::Local,
                Some(_) => Plan::Forward(allowed(node, &hop, targets)),
            }
        };
        match redirect.map(|ro| redirect::target(node, key, &owners, write, need, ro)) {
            None => forward(),
            Some(None) => Plan::Retry,
            Some(Some(t)) if t == node.id => Plan::Local,
            Some(Some(t)) => match redirect::moved(node, key, t) {
                Some(moved) => Plan::Moved(moved),
                None => forward(),
            },
        }
    });
    drop(route);
    let targets = match plan {
        Plan::Retry => return Dispatch::Ready(Reply::try_again()),
        Plan::Moved(moved) => return Dispatch::Ready(Reply::error(moved)),
        Plan::Local if need.is_some() || hop.track => {
            let owned = cmd[1].clone();
            return local_acked(node, spec, cmd)
                .then(node, move |node, reply| track_here(node, &owned, reply));
        }
        Plan::Local => return serve_here(node, spec, cmd, write, &owners),
        Plan::Forward(targets) => targets,
    };
    if targets.is_empty() {
        let owned = cmd[1].clone();
        return spent(node, spec, cmd, &[&owned], write);
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
    use std::sync::Weak;

    use discus_placement::{ClusterId, NodeId};
    use discus_transport::testing::TestCa;
    use discus_transport::TransportConfig;

    use super::*;
    use crate::settings::Reloadable;
    use crate::Config;

    #[test]
    fn a_data_command_in_a_database_above_the_cluster_count_is_refused() {
        let config = Config::parse_from(["discus"]).unwrap();
        let live = Live::new(Reloadable::from_config(&config));
        let security = Arc::new(crate::security::tests::security(&[]));
        let node = Node::new(config, security, live);
        let mut conn = ConnState::new(
            1,
            crate::security::ConnAuth {
                tls: true,
                ..Default::default()
            },
        );
        let get = |conn: &mut ConnState| {
            let cmd = crate::counter::cmd(&["GET", "k"]);
            prepare(&node, conn, command::lookup(&cmd[0]), cmd)
        };
        assert!(matches!(get(&mut conn), Prepared::Data(_)));
        conn.db = 1;
        let Prepared::Done(reply) = get(&mut conn) else {
            panic!("GET in database 1 ran");
        };
        assert_eq!(reply, Value::error(databases::OUT_OF_RANGE));
    }

    #[tokio::test]
    async fn the_link_reads_the_live_timeouts() {
        let base = Reloadable::from_config(&Config::parse_from(["discus"]).unwrap());
        let live_settings = Live::new(base.clone());
        let ca = TestCa::new();
        let id = NodeId::random();
        let config = TransportConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            ca.material("spiffe://discus.test/node"),
            id,
            ClusterId::mint(),
        );
        let transport = Transport::bind(config, Arc::new(service::NodeService(Weak::new())))
            .await
            .unwrap();
        let link = ClusterLink::new(transport, live_settings.clone());
        live_settings.apply(Reloadable {
            forward_read_timeout: Duration::from_millis(200),
            ..base
        });
        assert_eq!(link.read_timeout(), Duration::from_millis(200));
    }
}
