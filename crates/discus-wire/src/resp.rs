//! RESP values on the wire: commands as RESP multibulk bytes, replies as `RespValue` tables.

use std::fmt::Write;

use bytes::{BufMut, Bytes, BytesMut};
use discus_resp::Value;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::codec::{bytes_of, DecodeError};
use crate::fb;

/// Encode a command as a RESP multibulk, the raw form that a forward carries.
pub fn encode_command(cmd: &[Bytes]) -> Bytes {
    let size: usize = cmd.iter().map(|a| a.len() + 16).sum::<usize>() + 16;
    let mut out = BytesMut::with_capacity(size);
    // Writing to a `BytesMut` cannot fail.
    let _ = write!(out, "*{}\r\n", cmd.len());
    for arg in cmd {
        let _ = write!(out, "${}\r\n", arg.len());
        out.put_slice(arg);
        out.put_slice(b"\r\n");
    }
    out.freeze()
}

pub(crate) fn to_wire<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    value: &Value,
) -> WIPOffset<fb::RespValue<'a>> {
    let mut args = fb::RespValueArgs::default();
    let items = |fbb: &mut FlatBufferBuilder<'a>, values: &mut dyn Iterator<Item = &Value>| {
        let offsets: Vec<_> = values.map(|v| to_wire(fbb, v)).collect();
        fbb.create_vector(&offsets)
    };
    match value {
        Value::Simple(b) => {
            args.kind = fb::RespKind::SIMPLE;
            args.bytes = Some(fbb.create_vector(b));
        }
        Value::Error(b) => {
            args.kind = fb::RespKind::ERROR;
            args.bytes = Some(fbb.create_vector(b));
        }
        Value::Integer(i) => {
            args.kind = fb::RespKind::INTEGER;
            args.integer = *i;
        }
        Value::Bulk(b) => {
            args.kind = fb::RespKind::BULK;
            args.bytes = Some(fbb.create_vector(b));
        }
        Value::Null => args.kind = fb::RespKind::NULL,
        Value::Array(vs) => {
            args.kind = fb::RespKind::ARRAY;
            args.items = Some(items(fbb, &mut vs.iter()));
        }
        Value::Map(pairs) => {
            args.kind = fb::RespKind::MAP;
            args.items = Some(items(fbb, &mut pairs.iter().flat_map(|(k, v)| [k, v])));
        }
        Value::Boolean(b) => {
            args.kind = fb::RespKind::BOOLEAN;
            args.boolean = *b;
        }
        Value::Double(d) => {
            args.kind = fb::RespKind::DOUBLE;
            args.double = *d;
        }
    }
    fb::RespValue::create(fbb, &args)
}

pub(crate) fn from_wire(v: fb::RespValue<'_>) -> Result<Value, DecodeError> {
    let items = || v.items().into_iter().flatten().map(from_wire);
    Ok(match v.kind() {
        fb::RespKind::SIMPLE => Value::Simple(bytes_of(v.bytes())),
        fb::RespKind::ERROR => Value::Error(bytes_of(v.bytes())),
        fb::RespKind::INTEGER => Value::Integer(v.integer()),
        fb::RespKind::BULK => Value::Bulk(bytes_of(v.bytes())),
        fb::RespKind::NULL => Value::Null,
        fb::RespKind::ARRAY => Value::Array(items().collect::<Result<_, _>>()?),
        fb::RespKind::MAP => {
            let flat: Vec<Value> = items().collect::<Result<_, _>>()?;
            if !flat.len().is_multiple_of(2) {
                return Err(DecodeError::Invalid("a map has an odd item count".into()));
            }
            let mut it = flat.into_iter();
            let mut pairs = Vec::new();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                pairs.push((k, v));
            }
            Value::Map(pairs)
        }
        fb::RespKind::BOOLEAN => Value::Boolean(v.boolean()),
        fb::RespKind::DOUBLE => Value::Double(v.double()),
        other => {
            return Err(DecodeError::Invalid(format!(
                "unknown RESP kind {}",
                other.0
            )))
        }
    })
}
