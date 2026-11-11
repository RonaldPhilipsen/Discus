//! Command errors and their wire text.

use discus_resp::Value;

use crate::cluster::redirect::Endpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    UnknownCommand(String),
    WrongArity(String),
    Syntax,
    WrongType,
    NotInteger,
    NotFloat,
    Overflow,
    NoAuth,
    KeyTooLarge,
    ValueTooLarge,
    Unsupported(String),
    /// The HLC is pinned at its drift bound, so the node refuses writes for now.
    TryAgain,
    /// Routing failed: an owner is unreachable, or the hop budget ran out during an epoch change.
    ClusterTryAgain,
    /// The scopes of the connection do not allow the command (lower-case name).
    NoPerm(String),
    /// Another node serves the slot: the client sends the command there (spec "The redirect rule").
    Moved {
        slot: u16,
        to: Endpoint,
    },
    /// Raw error text, including its prefix such as `ERR` or `OOM`.
    Custom(String),
}

impl CommandError {
    pub fn text(&self) -> String {
        match self {
            CommandError::UnknownCommand(n) => format!("ERR unknown command '{n}'"),
            CommandError::WrongArity(n) => {
                format!(
                    "ERR wrong number of arguments for '{}' command",
                    n.to_lowercase()
                )
            }
            CommandError::Syntax => "ERR syntax error".into(),
            CommandError::WrongType => {
                "WRONGTYPE Operation against a key holding the wrong kind of value".into()
            }
            CommandError::NotInteger => "ERR value is not an integer or out of range".into(),
            CommandError::NotFloat => "ERR value is not a valid float".into(),
            CommandError::Overflow => "ERR increment or decrement would overflow".into(),
            CommandError::NoAuth => "NOAUTH Authentication required.".into(),
            CommandError::KeyTooLarge => "ERR key too large".into(),
            CommandError::ValueTooLarge => "ERR value too large".into(),
            CommandError::Unsupported(_) => "ERR command not supported by Discus".into(),
            CommandError::TryAgain => {
                "TRYAGAIN the node clock is at its drift bound, try again later".into()
            }
            CommandError::ClusterTryAgain => {
                "TRYAGAIN the cluster is changing, try again later".into()
            }
            CommandError::NoPerm(n) => {
                format!("NOPERM this user has no permissions to run the '{n}' command")
            }
            CommandError::Moved { slot, to } => format!("MOVED {slot} {}:{}", to.host, to.port),
            CommandError::Custom(s) => s.clone(),
        }
    }

    pub fn to_value(&self) -> Value {
        Value::error(&self.text())
    }
}
