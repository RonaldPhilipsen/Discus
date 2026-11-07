//! Command errors and their wire text.

use discus_resp::Value;

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
            CommandError::Custom(s) => s.clone(),
        }
    }

    pub fn to_value(&self) -> Value {
        Value::error(&self.text())
    }
}
