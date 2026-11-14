//! Strong counters (spec section 10): keys under `--counter-prefix` served linearizably by
//! counter groups.
//!
//! The keyspace splits into `G` groups by `slot(key) % G`. Each node runs one controller task
//! per group, which decides the role of the node in the group. The router sends every command on
//! a counter key to the leader of its group, so a counter key never enters the store or the
//! replication queues.

pub mod budget;
pub mod group;
pub mod lf;
pub mod raft;
pub mod raft_group;
pub mod route;
pub mod state;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use discus_membership::{LeaseHolder, LeaseProvider};
use discus_placement::{Epoch, MemberState, NodeId};
use discus_resp::{Command, Value};
use discus_wire::{GroupHint, Message, Regime};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{Config, Node};
use budget::Budget;
use group::{Group, Role};
use raft::Replica;
use state::{Kind, Limits};

#[cfg(test)]
pub(crate) fn cmd(parts: &[&str]) -> Command {
    parts
        .iter()
        .map(|p| bytes::Bytes::copy_from_slice(p.as_bytes()))
        .collect()
}

/// How often each group controller runs.
pub const TICK: Duration = Duration::from_millis(100);

/// How often the tier reports its gauges.
const METRICS_EVERY: Duration = Duration::from_secs(1);

/// How often a leader looks for expired keys to purge.
const PURGE_EVERY: Duration = Duration::from_secs(10);

/// How long a `RAFT_LEADER` question waits for each answer.
const ASK_TIMEOUT: Duration = Duration::from_millis(500);

/// The group of `key` when there are `groups` groups: the Redis Cluster slot modulo the count, so
/// keys that share a hash tag share a group.
pub fn group_for(key: &[u8], groups: u16) -> u16 {
    discus_placement::slot(key) % groups
}

/// The HRW key of group `g`: its target voters are the top members for this key.
pub fn group_key(g: u16) -> Vec<u8> {
    let mut key = b"\0counter\0".to_vec();
    key.extend_from_slice(&g.to_be_bytes());
    key
}

/// The counter settings from the flags.
#[derive(Debug, Clone)]
pub struct Settings {
    pub groups: u16,
    pub timeout: Duration,
    pub limits: Limits,
    pub reserve: u64,
    pub lease_duration: Duration,
    pub lease_renew: Duration,
    pub lease_margin: Duration,
    pub follower_timeout: Duration,
    pub dwell: Duration,
    pub raft: raft::RaftTiming,
    pub catchup_entries: u64,
    pub voter_timeout: Duration,
    pub settle_timeout: Duration,
}

impl Settings {
    pub fn from_config(c: &Config) -> Settings {
        Settings {
            groups: c.counter_groups.max(1),
            timeout: Duration::from_millis(c.counter_timeout_ms.max(1)),
            limits: Limits {
                max_value_bytes: c.max_value_bytes,
            },
            reserve: c.max_memory / 100 * c.counter_memory_percent,
            lease_duration: Duration::from_millis(c.counter_lease_duration_ms.max(1)),
            lease_renew: Duration::from_millis(c.counter_lease_renew_ms.max(1)),
            lease_margin: Duration::from_millis(c.counter_lease_margin_ms),
            follower_timeout: Duration::from_millis(c.counter_follower_timeout_ms),
            dwell: Duration::from_millis(c.counter_dwell_ms),
            raft: raft::RaftTiming {
                heartbeat_ms: c.raft_heartbeat_ms,
                election_ms: c.raft_election_timeout_ms,
                snapshot_entries: c.raft_snapshot_entries,
            },
            catchup_entries: c.raft_catchup_entries,
            voter_timeout: Duration::from_millis(c.raft_voter_timeout_ms),
            settle_timeout: Duration::from_millis(c.counter_settle_timeout_ms),
        }
    }
}

/// Why this node did not answer a counter command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The node does not serve the group. Nothing was applied, so another leader can take it.
    NotLeader,
    /// The command may have applied.
    InDoubt,
}

/// What a node sees of the cluster when it decides a counter role.
pub struct View {
    pub epoch: Arc<Epoch>,
    pub me: NodeId,
    /// This node is `NORMAL` and not `LEAVING`. Only such a node holds counter roles.
    pub me_normal: bool,
    /// This node is `LEAVING`: it keeps a voter slot until the leader moves it away.
    pub leaving: bool,
}

/// `id` is a `NORMAL` member of `epoch`.
pub fn normal(epoch: &Epoch, id: NodeId) -> bool {
    epoch
        .member(id)
        .is_some_and(|m| m.state == MemberState::Normal)
}

impl View {
    pub fn normal(&self, id: NodeId) -> bool {
        normal(&self.epoch, id)
    }

    pub fn normal_count(&self) -> usize {
        discus_membership::normal_count(&self.epoch)
    }

    /// Every `NORMAL` member, best first by HRW with the key of group `g`.
    pub fn ranked(&self, g: u16) -> Vec<NodeId> {
        discus_placement::hrw::owners(&group_key(g), self.epoch.members(), usize::MAX)
    }
}

/// The lease of each counter group, `discus-counter-<g>` (spec section 10.1).
pub type CounterLeases = Arc<dyn Fn(u16) -> Arc<dyn LeaseProvider> + Send + Sync>;

/// How long decoded member hints stay fresh.
const HINTS_FRESH: Duration = Duration::from_millis(50);

/// The state of one group on this node, for tests and `GET /admin/counters`.
#[derive(Debug, Clone, Default)]
pub struct GroupStatus {
    pub group: u16,
    pub role: &'static str,
    pub regime: Option<Regime>,
    pub leader: Option<NodeId>,
    pub follower: Option<NodeId>,
    pub synced: bool,
    pub lease_until: Option<Instant>,
    pub since: Option<Instant>,
    /// Raft mode: the voters and the learners that this replica knows.
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    /// Raft mode: snapshots that this replica installed from a leader.
    pub installed: u64,
    /// Raft mode: the replica ID of this node.
    pub replica: Option<u64>,
    /// The first role this node had in the current regime.
    pub first_role: Option<&'static str>,
}

pub struct Counters {
    node: Weak<Node>,
    pub id: NodeId,
    pub settings: Settings,
    budget: Arc<Budget>,
    groups: Vec<Arc<Group>>,
    hints: Mutex<(Option<Instant>, Vec<GroupHint>)>,
    published: Mutex<Vec<u8>>,
    /// Since when this node sees three `NORMAL` members without a break.
    dwell_since: Mutex<Option<Instant>>,
    /// The last membership change that this node started, in any group.
    last_change: Mutex<Option<Instant>>,
    settled: AtomicBool,
    /// Since when this node waits for the counter tier to settle.
    settle_since: Mutex<Option<Instant>>,
}

/// The shortest time between two membership changes of this node across all groups (spec
/// section 10.2).
const CHANGE_EVERY: Duration = Duration::from_millis(200);

impl Counters {
    /// The tier of `node`. Without `leases`, the epoch names each leader (spec section 10.1).
    pub fn new(node: &Arc<Node>, leases: Option<CounterLeases>) -> Arc<Counters> {
        let settings = Settings::from_config(&node.config);
        let groups = (0..settings.groups)
            .map(|g| Arc::new(Group::new(g, leases.as_ref().map(|f| f(g)))))
            .collect();
        Arc::new(Counters {
            dwell_since: Mutex::new(None),
            last_change: Mutex::new(None),
            settled: AtomicBool::new(false),
            settle_since: Mutex::new(None),
            node: Arc::downgrade(node),
            id: node.id,
            budget: Arc::new(Budget::new(settings.reserve, Some(node.store.clone()))),
            settings,
            groups,
            hints: Mutex::new((None, Vec::new())),
            published: Mutex::new(Vec::new()),
        })
    }

    /// A new replica of group `g` for `regime`, on the transport of this node.
    pub(super) async fn start_replica(
        self: &Arc<Self>,
        g: u16,
        regime: Regime,
    ) -> Result<Replica, String> {
        Replica::start(
            &self.settings.raft,
            self.settings.limits,
            g,
            regime,
            raft::new_replica_id(),
            Arc::new(raft_group::Link(Arc::downgrade(self))),
            self.budget.clone(),
        )
        .await
    }

    /// The wall clock of the node in Unix milliseconds. The leader stamps commands with it.
    pub fn now_ms(&self) -> u64 {
        self.node().map_or(0, |n| n.store.now_ms())
    }

    /// True when this node may start a membership change now. It records the start.
    pub fn may_change(&self) -> bool {
        let mut last = self.last_change.lock();
        if last.is_some_and(|t| t.elapsed() < CHANGE_EVERY) {
            return false;
        }
        *last = Some(Instant::now());
        true
    }

    /// Three `NORMAL` members for the dwell time (spec section 10.3). The clock starts when the
    /// count reached three, as membership saw it, or at the start of a node without gossip.
    pub fn raft_ready(&self, view: &View) -> bool {
        let n = view.normal_count();
        let mut since = self.dwell_since.lock();
        if n < 3 {
            *since = None;
            return false;
        }
        let start = *since.get_or_insert_with(|| {
            let Some(node) = self.node() else {
                return Instant::now();
            };
            let Some(m) = node.membership() else {
                return node.started_at;
            };
            match m.normal_members() {
                (count, at) if count >= 3 => at,
                _ => Instant::now(),
            }
        });
        start.elapsed() >= self.settings.dwell
    }

    /// The holder of a counter lease: this node, its address, and its cluster.
    pub fn lease_holder(&self) -> LeaseHolder {
        let node = self.node();
        LeaseHolder {
            identity: self.id.to_string(),
            address: node
                .as_ref()
                .and_then(|n| n.membership().map(|m| m.local().rpc_addr))
                .or_else(|| node.as_ref().map(|n| n.config.cluster_listen))
                .unwrap_or_else(|| std::net::SocketAddr::from(([0, 0, 0, 0], 0))),
            cluster_id: node.map_or(discus_placement::ClusterId::NIL, |n| n.cluster_id()),
        }
    }

    /// The current hints of the `NORMAL` members, decoded at most every [`HINTS_FRESH`].
    fn member_hints(&self, view: &View) -> Vec<GroupHint> {
        let mut cache = self.hints.lock();
        if cache.0.is_some_and(|t| t.elapsed() < HINTS_FRESH) {
            return cache.1.clone();
        }
        let members = self
            .node()
            .and_then(|n| n.membership().map(|m| m.members()))
            .unwrap_or_default();
        let hints: Vec<GroupHint> = members
            .into_iter()
            .filter(|(info, live)| {
                *live != discus_wire::Liveness::Dead && info.id != view.me && view.normal(info.id)
            })
            .flat_map(|(info, _)| discus_wire::decode_hints(&info.leader_hints).unwrap_or_default())
            .filter(|h| h.leader != view.me && view.normal(h.leader))
            .collect();
        *cache = (Some(Instant::now()), hints.clone());
        hints
    }

    /// Publish the hints of the groups this node leads, when they changed.
    fn publish_hints(&self) {
        let Some(membership) = self.node().and_then(|n| n.membership().cloned()) else {
            return;
        };
        let bytes = discus_wire::encode_hints(&self.own_hints());
        let mut published = self.published.lock();
        if *published != bytes {
            *published = bytes.clone();
            membership.set_leader_hints(bytes);
        }
    }

    /// One step of group `g`: merge the member hints, run the controller, publish the hints.
    async fn step(self: &Arc<Self>, group: &Arc<Group>, view: &View) {
        let mut hints = self.member_hints(view);
        hints.extend(group.own_hint(view.me));
        let gossip = self.node().is_some_and(|n| n.membership().is_some());
        group.observe(view, &hints, gossip);
        group.step(self, view).await;
        self.publish_hints();
    }

    pub fn status(&self) -> Vec<GroupStatus> {
        (0..self.settings.groups)
            .map(|g| self.status_of(g))
            .collect()
    }

    pub fn status_of(&self, g: u16) -> GroupStatus {
        let group = self.group(g);
        let leader = self.leader(g);
        let inner = group.inner();
        let regime = inner.role.regime();
        let mut s = GroupStatus {
            group: group.g,
            role: inner.role.name(),
            regime,
            leader,
            first_role: inner
                .first_role
                .filter(|(r, _)| Some(*r) == regime)
                .map(|(_, name)| name),
            ..GroupStatus::default()
        };
        match &inner.role {
            Role::Idle => {}
            Role::Follower(f) => {
                s.synced = f.synced();
                s.since = Some(f.since);
            }
            Role::Leader(l) => {
                s.follower = l.follower();
                s.synced = true;
                s.lease_until = l.lease_until();
                s.since = Some(l.since);
            }
            Role::Raft(r) => {
                s.synced = r.is_voter();
                s.since = Some(r.replica.started);
                s.voters = r.replica.voter_nodes();
                s.learners = r.replica.learner_nodes();
                s.installed = r.replica.sm.lock().installed;
                s.replica = Some(r.replica.replica);
            }
        }
        s
    }

    /// Reconfigure every Raft group in which this node is a voter, or only `group`, to this node
    /// as the only voter. Returns the groups that it reconfigured.
    pub async fn force_reconfigure(self: &Arc<Self>, group: Option<u16>) -> Vec<u16> {
        let Some(view) = self.view() else {
            return Vec::new();
        };
        let mut done = Vec::new();
        for g in &self.groups {
            if group.is_some_and(|x| x != g.g) {
                continue;
            }
            if g.force_reconfigure(self, &view).await {
                done.push(g.g);
            }
        }
        done
    }

    /// The counter state that this node holds for group `g`, in any role.
    pub fn held_state(&self, g: u16) -> Option<state::CounterState> {
        self.group(g).role().state()
    }

    pub fn node(&self) -> Option<Arc<Node>> {
        self.node.upgrade()
    }

    pub fn budget(&self) -> &Arc<Budget> {
        &self.budget
    }

    /// Run one controller task per group until `stop`.
    pub fn spawn(self: &Arc<Self>, stop: CancellationToken) {
        for group in &self.groups {
            let (me, group, stop) = (self.clone(), group.clone(), stop.clone());
            tokio::spawn(async move {
                let mut epochs = me.node().map(|n| n.subscribe_epoch());
                let mut every = tokio::time::interval(TICK);
                every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    let changed = async {
                        match epochs.as_mut() {
                            Some(rx) => rx.changed().await.is_ok(),
                            None => futures::future::pending().await,
                        }
                    };
                    tokio::select! {
                        _ = stop.cancelled() => break,
                        _ = every.tick() => {}
                        ok = changed => if !ok { epochs = None },
                    }
                    if let Some(view) = me.view() {
                        me.step(&group, &view).await;
                    }
                }
                group.clear();
            });
        }
        let me = self.clone();
        let s = stop.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(METRICS_EVERY);
            while s.run_until_cancelled(every.tick()).await.is_some() {
                me.record_metrics();
            }
        });
        let me = self.clone();
        let s = stop.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(PURGE_EVERY);
            every.tick().await;
            while s.run_until_cancelled(every.tick()).await.is_some() {
                me.purge().await;
                if let Some(node) = me.node() {
                    me.drop_databases_from(&node, node.databases.cluster())
                        .await;
                }
            }
        });
        let Some(membership) = self.node().and_then(|n| n.membership().cloned()) else {
            return;
        };
        let me = self.clone();
        let events = membership.subscribe();
        tokio::spawn(discus_store::on_events(events, stop, move |event| {
            use discus_membership::Event;
            // A rejoining node and a node of a lost cluster discard their counters (spec
            // section 6.3).
            if matches!(event, Event::Rejoining { .. } | Event::ClusterLost { .. }) {
                me.discard();
            }
        }));
    }

    /// Drop every counter role and state of this node. A later role starts with new replica
    /// IDs, so the node enters a Raft group again only as a learner.
    pub fn discard(&self) {
        tracing::warn!("discarding the counter state");
        for g in &self.groups {
            g.clear();
        }
        self.published.lock().clear();
    }

    fn record_metrics(&self) {
        use discus_telemetry::attrs::CounterRole;
        let metrics = discus_telemetry::global();
        let roles: Vec<Role> = self.groups.iter().map(|g| g.role()).collect();
        let names: Vec<&str> = roles.iter().map(Role::name).collect();
        for role in CounterRole::ALL {
            let count = names.iter().filter(|n| **n == role.as_str()).count();
            metrics.counter_roles(role, count as u64);
        }
        let single = roles
            .iter()
            .filter(|r| matches!(r, Role::Raft(r) if r.leads() && r.replica.voters().len() == 1))
            .count();
        metrics.counter_memory(self.budget.used());
        metrics.raft_single_voter_groups(single as u64);
    }

    /// The counter tier is settled (spec section 15.2): in every group this node is the leader,
    /// a synced follower, a voter of a Raft group that targets it, or not needed. Once true, it
    /// stays true, so a later change of targets never fails the startup probe. The wait ends
    /// after `settle_timeout`: a restart gives the pod a new node ID and starts its promotion
    /// again, so a probe that waits forever can keep a promotion from ever finishing.
    pub fn settled(self: &Arc<Self>) -> bool {
        if self.settled.load(Ordering::Acquire) {
            return true;
        }
        let Some(node) = self.node() else {
            return true;
        };
        let done = node.link().is_none()
            || self.view().is_some_and(|view| {
                view.me_normal
                    && (self.groups.iter().all(|g| {
                        let settled = g.settled_for(&view);
                        if settled.is_none() {
                            self.ask_leader(g.g);
                        }
                        settled == Some(true)
                    }) || self.settle_timed_out())
            });
        if done {
            self.settled.store(true, Ordering::Release);
        }
        done
    }

    fn settle_timed_out(&self) -> bool {
        let since = *self.settle_since.lock().get_or_insert_with(Instant::now);
        let timed_out = since.elapsed() >= self.settings.settle_timeout;
        if timed_out {
            tracing::warn!(
                "the counter tier did not settle in time, the node starts without it and its \
                 promotion continues"
            );
        }
        timed_out
    }

    /// Wait until this node holds no leadership and no voter slot, so a drain leaves every
    /// group served (spec section 15.3).
    pub async fn drained(&self) {
        loop {
            let busy = self.groups.iter().any(|g| match g.role() {
                Role::Leader(l) => l.serving(),
                Role::Raft(r) => r.is_voter(),
                _ => false,
            });
            if !busy {
                return;
            }
            tokio::time::sleep(TICK).await;
        }
    }

    /// The group of a counter key.
    pub fn group_of(&self, key: &[u8]) -> u16 {
        group_for(key, self.settings.groups)
    }

    pub fn group(&self, g: u16) -> &Arc<Group> {
        &self.groups[usize::from(g) % self.groups.len()]
    }

    pub fn view(&self) -> Option<View> {
        let node = self.node()?;
        let epoch = node.epochs().current().clone();
        let me = epoch.member(node.id).map(|m| m.state);
        Some(View {
            me_normal: node.is_normal() && me.is_some(),
            leaving: me == Some(MemberState::Leaving),
            epoch,
            me: node.id,
        })
    }

    /// The node that leads group `g`, as far as this node knows. `None` when it knows no
    /// `NORMAL` leader.
    pub fn leader(&self, g: u16) -> Option<NodeId> {
        let node = self.node()?;
        let epoch = node.epochs().current().clone();
        self.group(g).leader(node.id, &epoch)
    }

    /// Execute a counter command in database `db` on this node, which believes it leads group
    /// `g`.
    pub async fn execute(
        &self,
        g: u16,
        db: u32,
        kind: Kind,
        cmd: &Command,
    ) -> Result<Value, Refusal> {
        if !kind.is_read() && !kind.is_shrinking() && self.budget.full() {
            return Ok(Value::error(
                "OOM command not allowed when the counter reserve is full",
            ));
        }
        let now_ms = self.now_ms();
        match self.group(g).role() {
            Role::Leader(l) => {
                l.execute(
                    kind,
                    db,
                    cmd,
                    now_ms,
                    &self.settings.limits,
                    self.settings.follower_timeout,
                )
                .await
            }
            Role::Raft(r) if r.leads() => {
                r.execute(kind, db, cmd, now_ms, self.settings.timeout)
                    .await
            }
            Role::Idle | Role::Follower(_) | Role::Raft(_) => Err(Refusal::NotLeader),
        }
    }

    /// Propose a `PURGE` in every group that this node leads and that holds expired keys.
    /// Expired keys use the counter reserve until a write touches them, so without a purge they
    /// can fill it.
    pub async fn purge(&self) {
        let purge: Command = vec![bytes::Bytes::from_static(state::PURGE)];
        for (g, group) in self.groups.iter().enumerate() {
            if !group.role().leads_expired(self.now_ms()) {
                continue;
            }
            let g = g as u16;
            if let Err(refusal) = self.execute(g, 0, Kind::Purge, &purge).await {
                tracing::debug!(group = g, ?refusal, "the purge did not apply");
            }
        }
    }

    /// Remove the counters of every database from `count` up in each group that `node` leads:
    /// one `DISCUS.FLUSHDB` log entry for each such database, so the followers drop them too. A
    /// group led elsewhere drops them on its own leader. A drop can fail or miss a group whose
    /// leader moves, so the purge tick calls this again with the cluster count, and a later
    /// leader finishes the drop. The tick never uses the local count: a joiner can hold fewer
    /// databases than the cluster serves, and clients still reach the ones below the cluster
    /// count. A group with a replica that does not advertise `MULTI_DB` keeps them until a later
    /// tick.
    pub async fn drop_databases_from(self: &Arc<Self>, node: &Arc<Node>, count: u32) {
        let flush: Command = vec![bytes::Bytes::from_static(state::FLUSH_ONE_DB)];
        for group in &self.groups {
            if self.leader(group.g) != Some(node.id) {
                continue;
            }
            let role = group.role();
            let databases: Vec<u32> = role
                .led_databases()
                .into_iter()
                .filter(|db| *db >= count)
                .collect();
            if databases.is_empty() {
                continue;
            }
            // A replica of an older build cannot decode `DISCUS.FLUSHDB` in its log. The next
            // purge tick tries again, when the replicas may have changed.
            if let Some(old) = role.replicas().into_iter().find(|r| !node.multi_db(*r)) {
                tracing::warn!(
                    group = group.g,
                    replica = %old,
                    "the counters of a dropped database stay: a replica does not support multiple databases"
                );
                continue;
            }
            for db in databases {
                if let Err(refusal) = self.execute(group.g, db, Kind::FlushOneDb, &flush).await {
                    tracing::warn!(
                        group = group.g,
                        db,
                        ?refusal,
                        "the counters of a dropped database stay until the next try"
                    );
                }
            }
        }
    }

    /// Ask every other `NORMAL` member who leads group `g`, at most once per
    /// [`group::ASK_EVERY`]. The answers arrive in the background.
    pub fn ask_leader(self: &Arc<Self>, g: u16) {
        if !self.group(g).may_ask() {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            me.ask_now(g).await;
        });
    }

    /// Ask now and wait for the answers. Returns the best hint with a `NORMAL` leader.
    pub async fn ask_now(&self, g: u16) -> Option<GroupHint> {
        let view = self.view()?;
        let answers = self.ask_all(&view, g).await;
        let (best, denied) = group::pick(g, &view, answers);
        self.group(g).learn(best.clone(), &denied);
        best
    }

    /// Ask every other `NORMAL` member about group `g`. Returns the members that answered, and
    /// the ones among them that hold a Raft replica of the group.
    pub async fn holders(&self, g: u16) -> (Vec<NodeId>, Vec<NodeId>) {
        let Some(view) = self.view() else {
            return (Vec::new(), Vec::new());
        };
        let (mut answered, mut holders) = (Vec::new(), Vec::new());
        for (peer, hints) in self.ask_all(&view, g).await {
            answered.push(peer);
            if hints
                .iter()
                .any(|h| h.group == g && h.mode == discus_wire::CounterMode::Raft)
            {
                holders.push(peer);
            }
        }
        (answered, holders)
    }

    /// `RAFT_LEADER` to every other `NORMAL` member, best first for group `g`: the members that
    /// answered, with their hints.
    async fn ask_all(&self, view: &View, g: u16) -> Vec<(NodeId, Vec<GroupHint>)> {
        let peers: Vec<NodeId> = view
            .ranked(g)
            .into_iter()
            .filter(|p| *p != view.me)
            .collect();
        let answers = futures::future::join_all(
            peers
                .iter()
                .map(|p| self.send(*p, Message::RaftLeader(Vec::new()), ASK_TIMEOUT)),
        )
        .await;
        peers
            .into_iter()
            .zip(answers)
            .filter_map(|(peer, answer)| match answer {
                Ok(Message::RaftLeader(hints)) => Some((peer, hints)),
                _ => None,
            })
            .collect()
    }

    /// Send one counter message on the bulk lane. A bulk timeout is not data-path evidence, so a
    /// slow follower never speeds up a `DEAD` (spec section 6.1).
    pub async fn send(
        &self,
        to: NodeId,
        message: Message,
        timeout: Duration,
    ) -> Result<Message, String> {
        let node = self.node().ok_or("the node is gone")?;
        let build = |_: &_| Ok(message);
        node.request(to, discus_transport::Lane::Bulk, build, timeout, true)
            .await
            .map_err(|e| e.to_string())
    }

    /// The hints of every group that this node leads.
    pub fn own_hints(&self) -> Vec<discus_wire::GroupHint> {
        self.groups
            .iter()
            .filter_map(|g| g.own_hint(self.id))
            .collect()
    }

    /// Answer an inbound counter message.
    pub async fn handle(self: &Arc<Self>, from: NodeId, message: Message) -> Message {
        match message {
            Message::RaftLeader(_) => Message::RaftLeader(
                self.groups
                    .iter()
                    .filter_map(|g| g.answer_hint(self.id))
                    .collect(),
            ),
            Message::RaftConfig(c) => {
                let Some(view) = self.view() else {
                    return Message::RaftConfig(discus_wire::RaftConfig { replica: 0, ..c });
                };
                let reply = self.group(c.group).on_raft_config(self, &view, &c).await;
                Message::RaftConfig(reply)
            }
            m @ (Message::RaftVote(_) | Message::RaftAppend(_) | Message::RaftSnapshot(_)) => {
                let header = match &m {
                    Message::RaftVote(v) => v.header,
                    Message::RaftAppend(a) => a.header,
                    Message::RaftSnapshot(s) => s.header,
                    _ => return Message::RaftReply(discus_wire::RaftReply::refused()),
                };
                self.group(header.group).on_raft(&header, m).await
            }
            Message::CounterSync(s) => {
                let Some(view) = self.view() else {
                    return Message::Unsupported {
                        message_type: Message::CounterSync(s).type_code(),
                    };
                };
                let ack = self.group(s.group).on_sync(self, &view, from, &s);
                Message::CounterAck(ack)
            }
            other => Message::Unsupported {
                message_type: other.type_code(),
            },
        }
    }
}

#[cfg(test)]
mod slot_tests {
    use super::group_for;

    #[test]
    fn counter_keys_with_one_tag_share_a_group() {
        for groups in 1..=1024 {
            assert_eq!(group_for(b"{t}a", groups), group_for(b"{t}b", groups));
        }
    }

    #[test]
    fn group_of_is_the_slot_modulo_the_groups() {
        for groups in 1..=1024u16 {
            assert_eq!(group_for(b"foo", groups), 12182 % groups);
        }
    }
}
