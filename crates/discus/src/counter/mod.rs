//! Strong counters (spec section 10): keys under `--counter-prefix` served linearizably by
//! counter groups.
//!
//! The keyspace splits into `G` groups by `xxh3(key) % G`. Each node runs one controller task
//! per group, which decides the role of the node in the group. The router sends every command on
//! a counter key to the leader of its group, so a counter key never enters the store or the
//! replication queues.

pub mod budget;
pub mod group;
pub mod lf;
pub mod raft;
pub mod route;
pub mod state;

use std::sync::{Arc, Weak};
use std::time::Duration;

use discus_placement::{Epoch, MemberState, NodeId};
use discus_resp::{Command, Value};
use discus_wire::Message;
use tokio_util::sync::CancellationToken;

use crate::{Config, Node};
use budget::Budget;
use group::{Group, Role};
use state::Limits;

/// The seed of the group hash. A change is a re-shard (spec section 10.4).
pub const COUNTER_SEED: u64 = 0x4449_5343_5553_0005;

/// How often each group controller runs.
pub const TICK: Duration = Duration::from_millis(100);

/// How long a `RAFT_LEADER` question waits for each answer.
const ASK_TIMEOUT: Duration = Duration::from_millis(500);

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
    pub max_key_bytes: usize,
    pub reserve: u64,
}

impl Settings {
    pub fn from_config(c: &Config) -> Settings {
        Settings {
            groups: c.counter_groups.max(1),
            timeout: Duration::from_millis(c.counter_timeout_ms.max(1)),
            limits: Limits {
                max_value_bytes: c.max_value_bytes,
            },
            max_key_bytes: c.max_key_bytes,
            reserve: c.max_memory / 100 * c.counter_memory_percent,
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
#[derive(Clone)]
pub struct View {
    pub epoch: Arc<Epoch>,
    pub me: NodeId,
    /// This node is `NORMAL` and not `LEAVING`. Only such a node holds counter roles.
    pub me_normal: bool,
}

impl View {
    pub fn normal(&self, id: NodeId) -> bool {
        self.epoch
            .member(id)
            .is_some_and(|m| m.state == MemberState::Normal)
    }

    pub fn normal_count(&self) -> usize {
        discus_membership::normal_count(&self.epoch)
    }

    /// Every `NORMAL` member, best first by HRW with the key of group `g`.
    pub fn ranked(&self, g: u16) -> Vec<NodeId> {
        self.epoch.owners(&group_key(g), usize::MAX)
    }
}

pub struct Counters {
    node: Weak<Node>,
    pub id: NodeId,
    pub settings: Settings,
    budget: Arc<Budget>,
    groups: Vec<Arc<Group>>,
}

impl Counters {
    pub fn new(node: &Arc<Node>) -> Arc<Counters> {
        let settings = Settings::from_config(&node.config);
        let groups = (0..settings.groups)
            .map(|g| Arc::new(Group::new(g)))
            .collect();
        Arc::new(Counters {
            node: Arc::downgrade(node),
            id: node.id,
            budget: Arc::new(Budget::new(settings.reserve, Some(node.store.clone()))),
            settings,
            groups,
        })
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
                        group.step(&me, &view).await;
                    }
                }
                group.clear();
            });
        }
    }

    /// The group of a counter key.
    pub fn group_of(&self, key: &[u8]) -> u16 {
        let h = xxhash_rust::xxh3::xxh3_64_with_seed(key, COUNTER_SEED);
        (h % u64::from(self.settings.groups)) as u16
    }

    pub fn group(&self, g: u16) -> &Arc<Group> {
        &self.groups[usize::from(g) % self.groups.len()]
    }

    /// Counter bytes that this node holds over every group and role.
    pub fn bytes(&self) -> u64 {
        self.budget.used()
    }

    pub fn view(&self) -> Option<View> {
        let node = self.node()?;
        let epoch = node.epochs().current().clone();
        Some(View {
            me_normal: node.is_normal() && epoch.member(node.id).is_some(),
            epoch,
            me: node.id,
        })
    }

    /// The node that leads group `g`, as far as this node knows. `None` when it knows no
    /// `NORMAL` leader.
    pub fn leader(&self, g: u16) -> Option<NodeId> {
        let view = self.view()?;
        self.group(g).leader(&view)
    }

    /// Execute a counter command on this node, which believes it leads group `g`.
    pub async fn execute(&self, g: u16, cmd: &Command) -> Result<Value, Refusal> {
        let write = !state::is_read(cmd);
        if write && !state::is_shrinking(cmd) && self.budget.full() {
            return Ok(Value::error(
                "OOM command not allowed when the counter reserve is full",
            ));
        }
        let now_ms = self.node().map_or(0, |n| n.store.now_ms());
        match self.group(g).role() {
            Role::Leader(l) => l.execute(cmd, now_ms, &self.settings.limits).await,
            Role::Idle => Err(Refusal::NotLeader),
        }
    }

    pub fn distrust(&self, g: u16, leader: NodeId) {
        self.group(g).distrust(leader);
    }

    /// Ask every other `NORMAL` member who leads group `g`, at most once per
    /// [`group::ASK_EVERY`]. The answers arrive in the background.
    pub fn ask_leader(self: &Arc<Self>, g: u16) {
        let group = self.group(g).clone();
        if !group.may_ask() {
            return;
        }
        let Some(view) = self.view() else {
            return;
        };
        let me = self.clone();
        tokio::spawn(async move {
            let peers: Vec<NodeId> = view
                .ranked(g)
                .into_iter()
                .filter(|p| *p != view.me)
                .collect();
            let answers = futures::future::join_all(
                peers
                    .iter()
                    .map(|p| me.send(*p, Message::RaftLeader(Vec::new()), ASK_TIMEOUT)),
            )
            .await;
            let best = answers
                .into_iter()
                .filter_map(|a| match a {
                    Ok(Message::RaftLeader(hints)) => Some(hints),
                    _ => None,
                })
                .flatten()
                .filter(|h| h.group == g && view.normal(h.leader))
                .reduce(|a, b| if group::newer(&b, &a) { b } else { a });
            group.learn(best);
        });
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
        let link = node.link().ok_or("this node has no transport")?;
        let addr = node
            .epochs()
            .current()
            .member(to)
            .map(|m| m.addr)
            .ok_or_else(|| format!("node {to} is not a member"))?;
        let frame = link
            .transport
            .request_on(
                discus_transport::Lane::Bulk,
                to,
                addr,
                message,
                discus_wire::TraceContext::default(),
                timeout,
                true,
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(frame.message)
    }

    /// The hints of every group that this node leads.
    pub fn own_hints(&self) -> Vec<discus_wire::GroupHint> {
        self.groups
            .iter()
            .filter_map(|g| g.own_hint(self.id))
            .collect()
    }

    /// Answer an inbound counter message.
    pub async fn handle(&self, _from: NodeId, message: Message) -> Message {
        match message {
            Message::RaftLeader(_) => Message::RaftLeader(self.own_hints()),
            other => Message::Unsupported {
                message_type: other.type_code(),
            },
        }
    }
}
