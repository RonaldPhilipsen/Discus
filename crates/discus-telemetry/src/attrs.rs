//! Metric attributes and the cardinality guard (spec §15.4).
//!
//! Every metric attribute is one of five typed values. A command label is a `&'static str`, so it
//! comes from the code, never from a client. A node or peer label is a short hex node ID, so the
//! number of values is bounded by the cluster size. [`check`] validates raw pairs against the
//! same rules. The metrics handle runs it in debug builds, and the tests run it on every exported
//! data point.

use std::fmt;
use std::sync::Arc;

/// The only attribute keys that a metric can carry.
pub const ALLOWED_KEYS: [&str; 5] = ["command", "status", "node", "peer", "state"];

/// The longest command label.
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

/// One metric attribute.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Attribute {
    Command(CommandLabel),
    Status(Status),
    Node(NodeLabel),
    Peer(NodeLabel),
    State(MemberState),
}

impl Attribute {
    pub fn key(&self) -> &'static str {
        match self {
            Attribute::Command(_) => "command",
            Attribute::Status(_) => "status",
            Attribute::Node(_) => "node",
            Attribute::Peer(_) => "peer",
            Attribute::State(_) => "state",
        }
    }

    pub fn value(&self) -> &str {
        match self {
            Attribute::Command(c) => c.as_str(),
            Attribute::Status(s) => s.as_str(),
            Attribute::Node(n) | Attribute::Peer(n) => n.as_str(),
            Attribute::State(s) => s.as_str(),
        }
    }
}

/// Why the guard rejected an attribute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GuardError {
    /// The key is not in [`ALLOWED_KEYS`].
    DisallowedKey(String),
    /// The value is outside the domain of its key.
    InvalidValue { key: &'static str },
}

impl fmt::Display for GuardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GuardError::DisallowedKey(k) => write!(f, "metric attribute key {k:?} is not allowed"),
            GuardError::InvalidValue { key } => {
                write!(f, "metric attribute {key:?} has a value outside its domain")
            }
        }
    }
}

impl std::error::Error for GuardError {}

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
            "command" => ("command", valid_command(value)),
            "status" => ("status", Status::parse(value).is_some()),
            "node" => ("node", valid_node(value)),
            "peer" => ("peer", valid_node(value)),
            "state" => ("state", MemberState::parse(value).is_some()),
            other => return Err(GuardError::DisallowedKey(other.to_owned())),
        };
        if !ok {
            return Err(GuardError::InvalidValue { key });
        }
    }
    Ok(())
}
