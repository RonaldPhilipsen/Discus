//! The value types that a key can hold.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;

use crate::ZSet;

/// A stored value. A collection is one value, mutated under the shard lock.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(Bytes),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
    ZSet(ZSet),
}

impl Value {
    /// The name that `TYPE` reports.
    pub fn kind(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }
}
