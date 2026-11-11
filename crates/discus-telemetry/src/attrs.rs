//! Metric attributes and the cardinality guard (spec §15.4).
//!
//! Every metric attribute is one of the eleven typed values of [`Attribute`], under one of the
//! seven keys of [`ALLOWED_KEYS`]. A `command` value is a `&'static str` from the command table or
//! a [`TopologyCommand`], so it comes from the code, never from a client. A node or peer label is a short hex node ID, so the
//! number of values is bounded by the cluster size. [`check`] validates raw pairs against the
//! same rules. The metrics handle runs it in debug builds, and the tests run it on every exported
//! data point.

use std::sync::Arc;

/// The only attribute keys that a metric can carry.
pub const ALLOWED_KEYS: [&str; 7] = [
    "command", "status", "node", "peer", "state", "channel", "reason",
];

const MAX_COMMAND_LEN: usize = 32;
/// The longest node label: a 128-bit node ID in hex.
const MAX_NODE_LEN: usize = 32;

/// A command name from the command table, or one of the two special labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommandLabel(&'static str);

impl CommandLabel {
    /// A command name that is not in the table.
    pub const UNKNOWN: CommandLabel = CommandLabel("unknown");
    /// A command that Discus rejects on purpose.
    pub const UNSUPPORTED: CommandLabel = CommandLabel("unsupported");

    /// A label for a command from the command table.
    pub const fn new(name: &'static str) -> CommandLabel {
        CommandLabel(name)
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

/// A node ID as a label: 1 to 32 lower case hex digits.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeLabel(Arc<str>);

impl NodeLabel {
    pub fn new(id: &str) -> Result<NodeLabel, GuardError> {
        if valid_node(id) {
            Ok(NodeLabel(id.into()))
        } else {
            Err(GuardError::InvalidValue { key: "node" })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The shared string, so a metric attribute needs no copy.
    pub fn shared(&self) -> Arc<str> {
        self.0.clone()
    }
}

macro_rules! label_enum {
    ($(#[$doc:meta])* $name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name { $($variant),+ }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: [$name; [$($text),+].len()] = [$($name::$variant),+];

            pub fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text),+ }
            }

            fn parse(s: &str) -> Option<$name> {
                match s { $($text => Some($name::$variant),)+ _ => None }
            }
        }
    };
}

label_enum! {
    /// The outcome of an operation. Never a raw error message.
    Status {
        Ok => "ok",
        Error => "error",
        Success => "success",
        Failure => "failure",
        Timeout => "timeout",
        Reset => "reset",
        Handshake => "handshake",
        Verify => "verify",
        Dropped => "dropped",
    }
}

label_enum! {
    /// A member state (spec §6.1).
    MemberState {
        Joining => "joining",
        Normal => "normal",
        Leaving => "leaving",
        Dead => "dead",
    }
}

label_enum! {
    /// The role of a node in one counter group (spec section 10).
    CounterRole {
        Leader => "leader",
        Follower => "follower",
        Voter => "voter",
        Learner => "learner",
        Idle => "idle",
    }
}

label_enum! {
    /// The mode of a counter group (spec section 10).
    CounterMode {
        LeaderFollower => "leader_follower",
        Raft => "raft",
    }
}

label_enum! {
    /// The transport endpoint that a transport metric comes from: the gossip-only endpoint, the
    /// RPC-only endpoint, or the single endpoint of a static cluster.
    TransportChannel {
        Gossip => "gossip",
        Rpc => "rpc",
        Cluster => "cluster",
    }
}

label_enum! {
    /// Why a redirect connection forwarded a command instead of answering `-MOVED`.
    MisrouteReason {
        Epoch => "epoch",
        Down => "down",
        NoAddress => "no_address",
    }
}

label_enum! {
    /// A `CLUSTER` subcommand that returns the topology. Lower case, so it cannot collide with
    /// a command label.
    TopologyCommand {
        Slots => "slots",
        Shards => "shards",
        Nodes => "nodes",
        Info => "info",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Attribute {
    Command(CommandLabel),
    Status(Status),
    Node(NodeLabel),
    Peer(NodeLabel),
    State(MemberState),
    /// A counter role under the key `state`.
    Role(CounterRole),
    /// A counter mode under the key `state`.
    Mode(CounterMode),
    Channel(TransportChannel),
    Reason(MisrouteReason),
    /// A topology subcommand under the key `command`.
    Topology(TopologyCommand),
}

impl Attribute {
    pub fn key(&self) -> &'static str {
        match self {
            Attribute::Command(_) | Attribute::Topology(_) => "command",
            Attribute::Status(_) => "status",
            Attribute::Node(_) => "node",
            Attribute::Peer(_) => "peer",
            Attribute::Channel(_) => "channel",
            Attribute::Reason(_) => "reason",
            Attribute::State(_) | Attribute::Role(_) | Attribute::Mode(_) => "state",
        }
    }

    pub fn value(&self) -> &str {
        match self {
            Attribute::Command(c) => c.as_str(),
            Attribute::Status(s) => s.as_str(),
            Attribute::Node(n) | Attribute::Peer(n) => n.as_str(),
            Attribute::State(s) => s.as_str(),
            Attribute::Role(r) => r.as_str(),
            Attribute::Mode(m) => m.as_str(),
            Attribute::Channel(c) => c.as_str(),
            Attribute::Reason(r) => r.as_str(),
            Attribute::Topology(t) => t.as_str(),
        }
    }
}

/// Why the guard rejected an attribute.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GuardError {
    /// The key is not in [`ALLOWED_KEYS`].
    #[error("metric attribute key {0:?} is not allowed")]
    DisallowedKey(String),
    /// The value is outside the domain of its key.
    #[error("metric attribute {key:?} has a value outside its domain")]
    InvalidValue { key: &'static str },
}

fn valid_node(id: &str) -> bool {
    (1..=MAX_NODE_LEN).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_command(name: &str) -> bool {
    if name == CommandLabel::UNKNOWN.0 || name == CommandLabel::UNSUPPORTED.0 {
        return true;
    }
    (1..=MAX_COMMAND_LEN).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// Check raw attribute pairs against the allowlist and the value domains.
pub fn check<'a>(attrs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Result<(), GuardError> {
    for (key, value) in attrs {
        let (key, ok) = match key {
            "command" => (
                "command",
                valid_command(value) || TopologyCommand::parse(value).is_some(),
            ),
            "status" => ("status", Status::parse(value).is_some()),
            "node" => ("node", valid_node(value)),
            "peer" => ("peer", valid_node(value)),
            "state" => (
                "state",
                MemberState::parse(value).is_some()
                    || CounterRole::parse(value).is_some()
                    || CounterMode::parse(value).is_some(),
            ),
            "channel" => ("channel", TransportChannel::parse(value).is_some()),
            "reason" => ("reason", MisrouteReason::parse(value).is_some()),
            other => return Err(GuardError::DisallowedKey(other.to_owned())),
        };
        if !ok {
            return Err(GuardError::InvalidValue { key });
        }
    }
    Ok(())
}
