//! Command errors and their wire text.

use bytes::Bytes;
use discus_resp::Value;

use super::util::{c_prefix, error_text, ERROR_TEXT_BYTES};

use crate::cluster::redirect::Endpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    /// The name, and the quoted start of the arguments (empty when there are none).
    UnknownCommand(String, String),
    WrongArity(String),
    Syntax,
    NoSuchKey,
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
    /// The scopes of the connection or the rules of its user do not allow the command (lower-case
    /// name).
    NoPerm(String),
    /// The user rules do not allow one of the keys of the command. The text names no key.
    NoPermKey,
    /// Another node serves the slot: the client sends the command there (spec "The redirect rule").
    Moved {
        slot: u16,
        to: Endpoint,
    },
    /// The write applied on the primary, but fewer secondaries than `--write-acks` stored it
    /// before the replication timeout, with `--write-ack-timeout closed`.
    NoReplicas,
    /// Raw error text, including its prefix such as `ERR` or `OOM`.
    Custom(String),
}

const NOREPLICAS_PREFIX: &[u8] = b"NOREPLICAS ";

/// True for a `-NOREPLICAS` reply: the write applied, so it counts as a write of the connection.
pub fn is_no_replicas(value: &Value) -> bool {
    matches!(value, Value::Error(e) if e.starts_with(NOREPLICAS_PREFIX))
}

impl CommandError {
    /// The error for `cmd`, whose name is not a command, with the text of Valkey.
    pub fn unknown_command(cmd: &[Bytes]) -> CommandError {
        // Valkey stops adding arguments once the quoted text reaches the limit.
        let mut args = Vec::new();
        for arg in &cmd[1..] {
            let Some(room) = ERROR_TEXT_BYTES.checked_sub(args.len()).filter(|r| *r > 0) else {
                break;
            };
            args.push(b'\'');
            args.extend_from_slice(c_prefix(arg, room));
            args.extend_from_slice(b"' ");
        }
        CommandError::UnknownCommand(
            error_text(&cmd[0]),
            String::from_utf8_lossy(&args).into_owned(),
        )
    }

    pub fn text(&self) -> String {
        match self {
            CommandError::UnknownCommand(n, args) if args.is_empty() => {
                format!("ERR unknown command '{n}'")
            }
            CommandError::UnknownCommand(n, args) => {
                format!("ERR unknown command '{n}', with args beginning with: {args}")
            }
            CommandError::WrongArity(n) => {
                format!(
                    "ERR wrong number of arguments for '{}' command",
                    n.to_lowercase()
                )
            }
            CommandError::Syntax => "ERR syntax error".into(),
            CommandError::NoSuchKey => "ERR no such key".into(),
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
            CommandError::NoPermKey => "NOPERM this user has no permissions to access one of the \
                                          keys used as arguments"
                .into(),
            CommandError::NoReplicas => "NOREPLICAS Not enough good replicas to write.".into(),
            CommandError::Moved { slot, to } => format!("MOVED {slot} {}:{}", to.host, to.port),
            CommandError::Custom(s) => s.clone(),
        }
    }

    pub fn to_value(&self) -> Value {
        Value::error(&self.text())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_replicas_is_recognized_by_its_prefix() {
        assert!(is_no_replicas(&CommandError::NoReplicas.to_value()));
        assert!(!is_no_replicas(&CommandError::ClusterTryAgain.to_value()));
        assert!(!is_no_replicas(&Value::error("NOREPLICASX")));
        assert!(!is_no_replicas(&Value::bulk(
            "NOREPLICAS Not enough good replicas to write."
        )));
    }
}
