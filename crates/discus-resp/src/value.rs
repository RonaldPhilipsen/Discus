//! The RESP value model.

use bytes::Bytes;

/// The protocol flavor of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Resp2,
    Resp3,
}

/// A reply value. RESP3-only variants downgrade when encoded as RESP2.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Boolean(bool),
    Double(f64),
}

impl Value {
    pub fn ok() -> Value {
        Value::Simple(Bytes::from_static(b"OK"))
    }
    pub fn simple(s: &str) -> Value {
        Value::Simple(Bytes::copy_from_slice(s.as_bytes()))
    }
    pub fn error(s: &str) -> Value {
        Value::Error(Bytes::copy_from_slice(s.as_bytes()))
    }
    pub fn bulk(b: impl Into<Bytes>) -> Value {
        Value::Bulk(b.into())
    }
}
