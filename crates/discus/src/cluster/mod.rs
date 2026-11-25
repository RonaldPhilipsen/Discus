//! Routing: plan each command as local or forwarded, and run it (spec section 7).

pub mod copy;
pub mod forward;
pub mod hold;
pub mod multi;
pub mod record;
pub mod redirect;
pub mod rename;
pub mod route;
pub mod service;
pub mod topology;
pub mod wait;
pub mod writeset;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use discus_placement::{ClusterId, EpochId, NodeId, Owners, Version, NTP64};
use discus_resp::{Command, Value};
use discus_store::DbKey;
use discus_transport::Transport;
use discus_wire::TraceContext;
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use tracing::Span;

use crate::command::table::Access;
use crate::command::{self, CommandError, CommandSpec};
use crate::config::AckTimeout;
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
    /// A multi-key write and `MOVE`: the database, key, and version of each key that the command
    /// changed. Empty for any other command, whose one key has `applied`.
    pub versions: Vec<(u32, Bytes, Version)>,
    /// The node that ran the command itself, with its boot id. `None` for a reply that merges
    /// parts or that a node split again.
    pub served: Option<(NodeId, u64)>,
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
            versions: Vec::new(),
            served: None,
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
    /// The database of the connection when the command arrived. A later `SELECT` in the same
    /// pipeline does not change it.
    pub db: u32,
}

/// What a client connection keeps about its writes: the write set for its reads, and the record
/// for `WAIT`.
#[derive(Debug)]
pub struct ConnWrites {
    pub set: writeset::WriteSet,
    pub record: record::Record,
}

pub type Writes = Arc<Mutex<ConnWrites>>;

impl Job {
    /// A job without a client connection: a forwarded command or a held retry.
    pub fn plain(cmd: Command, spec: &'static CommandSpec, db: u32) -> Job {
        Job {
            cmd,
            spec,
            rr: 0,
            redirect: false,
            readonly: false,
            db,
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
        node.security.authorize(conn, spec, &cmd)?;
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
        db: conn.db,
    })
}

/// Execute on this node and report the stamp that the command took. The work runs in the `store`
/// span. `Node::write` queues each key that the command changed for the other owners.
pub fn local(node: &Node, spec: &CommandSpec, db: u32, cmd: &Command) -> Reply {
    let _store = telemetry::store_span().entered();
    Node::take_applied();
    let value = command::execute_local(node, spec, db, cmd);
    Reply {
        applied: Node::take_applied(),
        served: Some((node.id, node.boot)),
        ..Reply::value(value)
    }
}

/// The keys of `cmd` in database `db` that must reach a secondary before the reply: the listed
/// keys of a command that deletes them. A deleted session must stay deleted when the primary
/// fails.
fn acked_delete_keys(node: &Node, spec: &CommandSpec, db: u32, cmd: &Command) -> Vec<DbKey> {
    if !matches!(spec.name, "DEL" | "GETDEL" | "DISCUS.TAKE") {
        return Vec::new();
    }
    cmd[1..]
        .iter()
        .filter(|k| node.is_primary_read_key(k))
        .map(|k| DbKey::new(db, k.clone()))
        .collect()
}

/// The keys of a write that wait for secondaries before the reply (spec "Which keys wait").
pub(crate) struct AckPlan {
    pub keys: Vec<DbKey>,
    /// The other owners that must store each key.
    pub need: usize,
    /// `--write-acks 0`: only a primary-read delete waits, and it fails open.
    pub legacy: bool,
    pub mode: AckTimeout,
}

/// The wait of `cmd` in database `db`, or `None` when nothing waits. It reads the live settings
/// once.
pub(crate) fn ack_plan(node: &Node, spec: &CommandSpec, db: u32, cmd: &Command) -> Option<AckPlan> {
    // Every command of `acked_delete_keys` is a write too, so a read skips the settings.
    if spec.access != Access::Write {
        return None;
    }
    let (need, mode) = {
        let live = node.live_settings.get();
        (live.write_acks, live.write_ack_timeout)
    };
    if need == 0 {
        let keys = acked_delete_keys(node, spec, db, cmd);
        return (!keys.is_empty()).then_some(AckPlan {
            keys,
            need: 1,
            legacy: true,
            mode,
        });
    }
    let keys = written_keys(node, spec, db, cmd);
    (!keys.is_empty()).then_some(AckPlan {
        keys,
        need,
        legacy: false,
        mode,
    })
}

/// The keys that `cmd` in database `db` writes, in argument order: the key arguments, and for a
/// `MOVE` its key in the target database last. A forward reply aligns its versions with them.
fn key_slots<'a>(
    spec: &CommandSpec,
    db: u32,
    cmd: &'a Command,
) -> impl Iterator<Item = (u32, &'a Bytes)> {
    let target = (spec.name == "MOVE").then(|| move_db(cmd)).flatten();
    route::key_args(spec.route, cmd)
        .into_iter()
        .flatten()
        .map(move |k| (db, k))
        .chain(target.map(|t| (t, &cmd[1])))
}

/// The [`key_slots`] of `cmd` without counter keys.
fn written_keys(node: &Node, spec: &CommandSpec, db: u32, cmd: &Command) -> Vec<DbKey> {
    key_slots(spec, db, cmd)
        .filter(|(_, k)| !node.is_counter_key(k))
        .map(|(db, k)| DbKey::new(db, k.clone()))
        .collect()
}

/// A command that reports the version of each key it changed in [`Reply::versions`]. A
/// single-key command has only `applied`, so its reply does not grow.
pub(crate) fn per_key(spec: &CommandSpec) -> bool {
    matches!(spec.route, Route::MultiWrite { .. }) || spec.name == "MOVE"
}

fn key_versions(changed: &[(DbKey, Version)]) -> Vec<(u32, Bytes, Version)> {
    changed
        .iter()
        .map(|(k, v)| (k.db, k.key.clone(), *v))
        .collect()
}

/// Run `cmd` here like [`local`], and fill [`Reply::versions`] with the [`changed`] rule.
fn local_versions(node: &Node, spec: &CommandSpec, db: u32, cmd: &Command) -> Reply {
    if !per_key(spec) {
        return local(node, spec, db, cmd);
    }
    let before = node.hlc.last();
    let mut reply = local(node, spec, db, cmd);
    if reply.applied.is_some() {
        let keys = written_keys(node, spec, db, cmd);
        reply.versions = key_versions(&changed(node, &keys, before));
    }
    reply
}

/// [`Reply::versions`] as a forward reply carries them: one entry per [`key_slots`] of `cmd`,
/// `None` where the command changed nothing. Empty when `versions` is.
pub(crate) fn aligned(
    spec: &CommandSpec,
    db: u32,
    cmd: &Command,
    versions: &[(u32, Bytes, Version)],
) -> Vec<Option<Version>> {
    if versions.is_empty() {
        return Vec::new();
    }
    // A part that this node split again lists its versions by group, not in argument order.
    let by_key: HashMap<(u32, &[u8]), Version> = versions
        .iter()
        .map(|(db, k, v)| ((*db, &k[..]), *v))
        .collect();
    key_slots(spec, db, cmd)
        .map(|(db, k)| by_key.get(&(db, &k[..])).copied())
        .collect()
}

/// The inverse of [`aligned`] on the entry node.
pub(crate) fn unaligned(
    spec: &CommandSpec,
    db: u32,
    cmd: &Command,
    versions: Vec<Option<Version>>,
) -> Vec<(u32, Bytes, Version)> {
    if versions.is_empty() {
        return Vec::new();
    }
    key_slots(spec, db, cmd)
        .zip(versions)
        .filter_map(|((db, k), v)| Some((db, k.clone(), v?)))
        .collect()
}

/// Run `cmd` here like [`local`], then wait as [`ack_plan`] says. With `--write-acks 0`, a delete
/// of a listed key waits for one secondary and then replies with success anyway (fail open): the
/// hints deliver the tombstone. With `--write-acks` above 0, a write that changed something waits
/// for that many secondaries. A wait that ends short replies normally in `open` mode and with
/// `-NOREPLICAS` in `closed` mode. Both keep the `applied` stamp, because the write applied here.
pub fn local_acked(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    db: u32,
    cmd: Command,
) -> Dispatch {
    let Some(plan) = ack_plan(node, spec, db, &cmd) else {
        return Dispatch::Ready(local_versions(node, spec, db, &cmd));
    };
    let Some(replicator) = node.replicator().cloned() else {
        if plan.legacy {
            return Dispatch::Ready(local_versions(node, spec, db, &cmd));
        }
        return Dispatch::Ready(unreplicated(node, spec, db, &cmd, &plan));
    };
    if plan.legacy {
        let reply = local_versions(node, spec, db, &cmd);
        if reply.applied.is_none() {
            return Dispatch::Ready(reply);
        }
        let node = node.clone();
        return Dispatch::Pending(Box::pin(async move {
            let unacked = replicator.unacked_after_wait(&node, &plan.keys).await;
            discus_telemetry::global().primary_read_delete_unacked(unacked);
            reply
        }));
    }
    // Watched before the store write, so no acknowledged batch carries the new version unseen
    // (spec "When a peer acknowledged a write").
    let watch = replicator.watch(plan.keys);
    let before = node.hlc.last();
    let mut reply = local(node, spec, db, &cmd);
    if reply.applied.is_none() {
        return Dispatch::Ready(reply);
    }
    // The watched keys are the `written_keys` of the command.
    let keys = changed(node, watch.keys(), before);
    if per_key(spec) {
        reply.versions = key_versions(&keys);
    }
    if keys.is_empty() {
        return Dispatch::Ready(reply);
    }
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let short = replicator.wait_acks(&node, &keys, plan.need).await;
        drop(watch);
        ended_short(reply, short, plan.mode)
    }))
}

/// Run a write with a plan that is not legacy on a node without secondaries. No other owner can
/// acknowledge it, so the wait ends at once and the mode decides the reply (spec "Owners that
/// cannot acknowledge").
fn unreplicated(node: &Node, spec: &CommandSpec, db: u32, cmd: &Command, plan: &AckPlan) -> Reply {
    let before = node.hlc.last();
    let mut reply = local(node, spec, db, cmd);
    if reply.applied.is_none() {
        return reply;
    }
    let keys = changed(node, &plan.keys, before);
    if per_key(spec) {
        reply.versions = key_versions(&keys);
    }
    ended_short(reply, keys.len() as u64, plan.mode)
}

/// The keys of `keys` with a version newer than `before`, the HLC time before the command ran.
/// Every stamp of the command is newer, so a key that the command did not change, such as an old
/// tombstone in a multi-key `DEL`, does not wait.
fn changed(node: &Node, keys: &[DbKey], before: NTP64) -> Vec<(DbKey, Version)> {
    keys.iter()
        .filter_map(|k| {
            let v = node.store.version(k.db, &k.key)?;
            (v.time() > before).then(|| (k.clone(), v))
        })
        .collect()
}

/// The reply of a write whose wait left `short` keys without enough acknowledgments. An error
/// reply keeps its own error: it says more than `-NOREPLICAS`.
fn ended_short(reply: Reply, short: u64, mode: AckTimeout) -> Reply {
    if short == 0 {
        return reply;
    }
    discus_telemetry::global().write_unacked(short, mode.as_str());
    match mode {
        AckTimeout::Closed if !matches!(reply.value, Value::Error(_)) => Reply {
            value: CommandError::NoReplicas.to_value(),
            ..reply
        },
        _ => reply,
    }
}

/// Fill `observed` and `settled` of a read of `key` that ran here.
fn track_here(node: &Node, db: u32, key: &[u8], reply: &mut Reply) {
    if reply.observed.is_none() {
        reply.observed = node.store.version(db, key);
        reply.settled = node.replicator().is_some_and(|r| r.settled(db, key));
    }
}

/// Serve a single-key command on this node. A replica without an entry for the key, or any owner
/// while a handoff toward it is incomplete, first asks the other owners (read repair, spec
/// sections 5.2 and 8.2). `owners` are the current owners of the key. With `track`, the reply
/// carries `observed` and `settled`.
pub fn serve_here(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    db: u32,
    cmd: Command,
    write: bool,
    owners: &[NodeId],
    track: bool,
) -> Dispatch {
    let sources = crate::replication::repair::sources(node, db, &cmd[1], write, owners);
    if sources.is_empty() {
        return local_tracked(node, spec, db, cmd, track);
    }
    let node = node.clone();
    let key = cmd[1].clone();
    Dispatch::Pending(Box::pin(async move {
        crate::replication::repair::fetch(&node, db, &key, sources).await;
        local_tracked(&node, spec, db, cmd, track).finish().await
    }))
}

/// [`local_acked`], and with `track` [`track_here`] on the reply.
fn local_tracked(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    db: u32,
    cmd: Command,
    track: bool,
) -> Dispatch {
    if !track {
        return local_acked(node, spec, db, cmd);
    }
    let owned = cmd[1].clone();
    local_acked(node, spec, db, cmd)
        .then(node, move |node, reply| track_here(node, db, &owned, reply))
}

/// Run a multi-key command or part with every key here, like [`local_acked`]. The keys that
/// [`crate::replication::repair::plan`] names are fetched first.
pub(crate) fn local_part(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    db: u32,
    cmd: Command,
    write: bool,
) -> Dispatch {
    let keys = route::key_args(spec.route, &cmd).into_iter().flatten();
    let plan = crate::replication::repair::plan(node, db, keys, write);
    if plan.is_empty() {
        return local_acked(node, spec, db, cmd);
    }
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        crate::replication::repair::fetch_all(&node, db, plan).await;
        local_acked(&node, spec, db, cmd).finish().await
    }))
}

/// Run a data command from the entry node. `route` is the `route` span: the owner choice runs
/// in it, and it ends before the command runs or forwards.
pub fn run(node: &Arc<Node>, job: Job, writes: Option<&Writes>, route: Span) -> Dispatch {
    // With one owner per key, a read reaches that owner whatever the write set requires.
    let writes = writes.filter(|_| node.config.replication_factor > 1);
    // `WAIT` needs no owner and no link, so it never forwards and never gets `-MOVED`.
    if job.spec.route == Route::Wait {
        drop(route);
        return wait::run(node, job, writes);
    }
    let set_reply = job.spec.set_reply;
    let null_array = job.spec.null_array && job.cmd.len() > 2;
    let flushed = (job.spec.route == Route::Broadcast).then_some((job.spec.name, job.db));
    let mut dispatch = run_hop(node, job, Hop::default(), writes, route);
    if let (Some((name, db)), Some(writes)) = (flushed, writes) {
        dispatch = dispatch.then(writes, move |writes, reply| {
            if matches!(reply.value, Value::Error(_)) {
                return;
            }
            let record = &mut writes.lock().record;
            match name {
                "FLUSHALL" => record.clear(),
                _ => record.clear_db(db),
            }
        });
    }
    if !set_reply && !null_array {
        return dispatch;
    }
    // A forwarded reply arrives as an array or a plain nil, so the entry node restores the
    // shape here, for local and forwarded replies alike.
    let reshape = move |mut reply: Reply| {
        reply.value = match reply.value {
            Value::Array(items) if set_reply => Value::Set(items),
            Value::Null if null_array => Value::NullArray,
            other => other,
        };
        reply
    };
    match dispatch {
        Dispatch::Ready(reply) => Dispatch::Ready(reshape(reply)),
        Dispatch::Pending(f) => Dispatch::Pending(Box::pin(async move { reshape(f.await) })),
    }
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
    // A job that did not pass `prepare` on this node, such as a held retry, checks the database
    // against the databases that this node holds.
    if !node.databases.holds(job.db) {
        drop(route);
        return Dispatch::Ready(Reply::value(Value::error(databases::OUT_OF_RANGE)));
    }
    if let Some(dispatch) = crate::counter::route::intercept(node, &job, &hop) {
        drop(route);
        return dispatch;
    }
    if job.spec.route == Route::Broadcast && hop.hops == 0 && node.counters().is_some() {
        drop(route);
        return crate::counter::route::broadcast(node, job.spec, job.db, job.cmd);
    }
    if node.link().is_none() {
        drop(route);
        let reply = match ack_plan(node, job.spec, job.db, &job.cmd).filter(|p| !p.legacy) {
            Some(plan) => unreplicated(node, job.spec, job.db, &job.cmd, &plan),
            None => local(node, job.spec, job.db, &job.cmd),
        };
        return Dispatch::Ready(reply);
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
            let count = node.store.db_len(job.db) as i64 / multi::copies(node);
            Dispatch::Ready(Reply::value(Value::Integer(count)))
        }
        Route::Fanout if hop.hops == 0 => {
            drop(route);
            multi::dbsize(node, job.spec, job.db, job.cmd)
        }
        Route::Local if redirect::applies(job.redirect, &hop) && job.spec.name == "SCAN" => {
            drop(route);
            Dispatch::Ready(Reply::value(command::keys::scan_primary(
                node, job.db, &job.cmd,
            )))
        }
        // Only the counter part of a flush from an entry node without the gate reaches here: the
        // cache broadcast uses LOCAL mode, and with the gate the counter part is
        // `DISCUS.COUNTERFLUSH`. It names no group, so it clears nothing, and it must not clear
        // this node's store.
        Route::Broadcast if hop.hops > 0 => {
            drop(route);
            Dispatch::Ready(Reply::value(Value::ok()))
        }
        Route::Rename => {
            drop(route);
            rename::run(node, job, hop, writes)
        }
        Route::Copy => {
            drop(route);
            copy::run(node, job, hop, writes)
        }
        Route::Connection | Route::Local | Route::Fanout | Route::Broadcast | Route::Wait => {
            drop(route);
            Dispatch::Ready(local(node, job.spec, job.db, &job.cmd))
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
    db: u32,
    key: &[u8],
    owners: &[NodeId],
    write: bool,
    rr: u64,
    need: Option<Version>,
) -> Vec<NodeId> {
    if let Some(need) = need.filter(|_| !write) {
        if owners.contains(&node.id) && node.store.version(db, key).is_some_and(|v| v >= need) {
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
    db: u32,
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
        local_acked(node, spec, db, cmd)
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
            .set
            .required(SetKey::of(job.db, &job.cmd[1]), std::time::Instant::now),
        _ => None,
    };
    // Only a write, or a read that must see an earlier write, changes the write set.
    let key = (tracked && (write || need.is_some())).then(|| SetKey::of(job.db, &job.cmd[1]));
    let target = tracked.then(|| move_target(&job)).flatten();
    // The record also takes a primary-read key, but never a counter key.
    let noted = (write && writes.is_some() && !node.is_counter_key(&job.cmd[1]))
        .then(|| (job.db, job.cmd[1].clone()));
    let by_key = write && per_key(job.spec);
    let dispatch = single_planned(node, job, owners, hop, write, need, route);
    let Some(writes) = writes.filter(|_| key.is_some() || noted.is_some()) else {
        return dispatch;
    };
    // The write set and the record change before the key order releases the next command on
    // this key.
    dispatch.then(writes, move |writes, reply| {
        let mut w = writes.lock();
        match (write, reply.applied) {
            (true, Some(v)) if records(&reply.value) => {
                let now = std::time::Instant::now();
                if let Some(key) = key {
                    w.set.record(key, v, now);
                }
                if let Some(target) = target {
                    w.set.record(target, v, now);
                }
                match noted {
                    // `MOVE` changes its key in two databases, each with its own version.
                    Some(_) if by_key => {
                        for (db, k, v) in &reply.versions {
                            w.record.note(*db, k, *v, reply.served);
                        }
                    }
                    Some((db, k)) => w.record.note(db, &k, v, reply.served),
                    None => {}
                }
            }
            (false, _) if need.is_some() => {
                if let Some(key) = key {
                    w.set.observed(key, reply.observed, reply.settled);
                }
            }
            _ => {}
        }
    })
}

/// A write with this reply enters the write set. A `-NOREPLICAS` write applied on its primary, so
/// a later read on the connection must see it (spec section 7.4).
pub(crate) fn records(value: &Value) -> bool {
    !matches!(value, Value::Error(_)) || command::error::is_no_replicas(value)
}

/// The key in the target database of a `MOVE`, which writes its key in two databases. A `COPY`
/// needs no such key: its put records the destination as a write of its own.
fn move_target(job: &Job) -> Option<SetKey> {
    if job.spec.name != "MOVE" {
        return None;
    }
    Some(SetKey::of(move_db(&job.cmd)?, &job.cmd[1]))
}

/// The target database of a `MOVE`.
fn move_db(cmd: &Command) -> Option<u32> {
    command::util::parse_i64(&cmd[2])
        .ok()
        .and_then(|n| u32::try_from(n).ok())
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
        db,
    } = job;
    let redirect = redirect::applies(redirect, &hop).then_some(readonly);
    let key: &[u8] = &cmd[1];
    let plan = route.in_scope(|| {
        let forward = || {
            let targets = candidates(node, db, key, &owners, write, rr, need);
            match targets.first() {
                None => Plan::Retry,
                Some(first) if *first == node.id => Plan::Local,
                Some(_) => Plan::Forward(allowed(node, &hop, targets)),
            }
        };
        match redirect.map(|ro| redirect::target(node, db, key, &owners, write, need, ro)) {
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
        Plan::Local => {
            let track = need.is_some() || hop.track;
            return serve_here(node, spec, db, cmd, write, &owners, track);
        }
        Plan::Forward(targets) => targets,
    };
    if targets.is_empty() {
        let owned = cmd[1].clone();
        return spent(node, spec, db, cmd, &[&owned], write);
    }
    hop.track |= need.is_some();
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        for target in targets {
            if let Some(reply) =
                hold::send_or_hold(&node, spec, db, target, &cmd, &hop, write).await
            {
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
    fn closed_mode_keeps_an_error_reply_and_replaces_success() {
        let applied = Some(Version::zero());
        let err = Reply {
            applied,
            ..Reply::value(Value::error("OOM command not allowed"))
        };
        let kept = ended_short(err, 1, AckTimeout::Closed);
        assert_eq!(kept.value, Value::error("OOM command not allowed"));
        let ok = Reply {
            applied,
            ..Reply::value(Value::ok())
        };
        let swapped = ended_short(ok, 1, AckTimeout::Closed);
        assert_eq!(swapped.value, CommandError::NoReplicas.to_value());
        assert_eq!(swapped.applied, applied);
        let open = ended_short(Reply::value(Value::ok()), 1, AckTimeout::Open);
        assert_eq!(open.value, Value::ok());
    }

    #[test]
    fn only_keys_that_the_command_changed_wait() {
        let config = Config::parse_from(["discus"]).unwrap();
        let live = Live::new(Reloadable::from_config(&config));
        let security = Arc::new(crate::security::tests::security(&[]));
        let node = Arc::new(Node::new(config, security, live));
        let run_cmd = |parts: &[&str]| {
            let cmd = crate::counter::cmd(parts);
            local(&node, command::lookup(&cmd[0]).unwrap(), 0, &cmd)
        };
        run_cmd(&["SET", "old", "v"]);
        run_cmd(&["DEL", "old"]);
        assert!(node.store.version(0, b"old").is_some());
        run_cmd(&["SET", "new", "v"]);
        let before = node.hlc.last();
        run_cmd(&["DEL", "old", "new", "missing"]);
        let keys: Vec<DbKey> = ["old", "new", "missing"]
            .iter()
            .map(|k| DbKey::new(0, bytes::Bytes::from_static(k.as_bytes())))
            .collect();
        let waits: Vec<DbKey> = changed(&node, &keys, before)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(waits, [keys[1].clone()]);
    }

    #[test]
    fn versions_align_with_the_key_arguments() {
        let v = |t: u64| Version::new(NTP64(t), NodeId::from_u128(1));
        let key = |k: &'static str| bytes::Bytes::from_static(k.as_bytes());
        let mset = crate::counter::cmd(&["MSET", "a", "1", "b", "2", "c", "3"]);
        let spec = command::lookup(b"MSET").unwrap();
        let versions = vec![(0, key("c"), v(3)), (0, key("a"), v(1))];
        let wire = aligned(spec, 0, &mset, &versions);
        assert_eq!(wire, [Some(v(1)), None, Some(v(3))]);
        let back = unaligned(spec, 0, &mset, wire);
        assert_eq!(back, [(0, key("a"), v(1)), (0, key("c"), v(3))]);

        let mv = crate::counter::cmd(&["MOVE", "k", "3"]);
        let spec = command::lookup(b"MOVE").unwrap();
        let versions = vec![(2, key("k"), v(4)), (3, key("k"), v(4))];
        let wire = aligned(spec, 2, &mv, &versions);
        assert_eq!(wire, [Some(v(4)), Some(v(4))]);
        assert_eq!(unaligned(spec, 2, &mv, wire), versions);
    }

    #[test]
    fn only_a_multi_key_write_and_move_report_versions() {
        let config = Config::parse_from(["discus", "--databases", "4"]).unwrap();
        let live = Live::new(Reloadable::from_config(&config));
        let security = Arc::new(crate::security::tests::security(&[]));
        let node = Arc::new(Node::new(config, security, live));
        let run_cmd = |parts: &[&str]| {
            let cmd = crate::counter::cmd(parts);
            let spec = command::lookup(&cmd[0]).unwrap();
            let Dispatch::Ready(reply) = local_acked(&node, spec, 0, cmd) else {
                panic!("the write waited");
            };
            reply
        };
        let set = run_cmd(&["SET", "k", "v"]);
        assert!(set.applied.is_some());
        assert!(set.versions.is_empty());
        let moved = run_cmd(&["MOVE", "k", "1"]);
        let keys: Vec<(u32, &[u8])> = moved
            .versions
            .iter()
            .map(|(d, k, _)| (*d, &k[..]))
            .collect();
        assert_eq!(keys, [(0, &b"k"[..]), (1, &b"k"[..])]);
        assert_eq!(moved.versions[1].2, node.store.version(1, b"k").unwrap());
    }

    #[test]
    fn a_data_command_in_a_database_above_the_cluster_count_is_refused() {
        let config = Config::parse_from(["discus"]).unwrap();
        let live = Live::new(Reloadable::from_config(&config));
        let security = Arc::new(crate::security::tests::security(&[]));
        let node = Node::new(config, security, live);
        let mut conn = ConnState::new(1, crate::security::ConnAuth::default());
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

    #[test]
    fn a_job_in_a_database_that_this_node_does_not_hold_runs_nothing() {
        let config = Config::parse_from(["discus"]).unwrap();
        let live = Live::new(Reloadable::from_config(&config));
        let security = Arc::new(crate::security::tests::security(&[]));
        let node = Arc::new(Node::new(config, security, live));
        let cmd = crate::counter::cmd(&["SET", "k", "v"]);
        let spec = command::lookup(&cmd[0]).unwrap();
        let Dispatch::Ready(reply) = run(&node, Job::plain(cmd, spec, 1), None, Span::none())
        else {
            panic!("the job did not finish at once");
        };
        assert_eq!(reply.value, Value::error(databases::OUT_OF_RANGE));
        assert!(!node.store.read(1, b"k", |e| e.is_some()));
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
