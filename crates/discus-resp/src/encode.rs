//! Encode a [`Value`] to wire bytes.

use bytes::{BufMut, BytesMut};

use crate::{Protocol, Value};

fn line(out: &mut BytesMut, prefix: u8, body: &[u8]) {
    out.put_u8(prefix);
    out.put_slice(body);
    out.put_slice(b"\r\n");
}

fn header(out: &mut BytesMut, prefix: u8, n: impl itoa::Integer) {
    line(out, prefix, itoa::Buffer::new().format(n).as_bytes());
}

fn bulk(out: &mut BytesMut, body: &[u8]) {
    header(out, b'$', body.len());
    out.put_slice(body);
    out.put_slice(b"\r\n");
}

/// Encode `value` for `protocol` and append it to `out`.
pub fn encode(value: &Value, protocol: Protocol, out: &mut BytesMut) {
    let resp3 = protocol == Protocol::Resp3;
    match value {
        Value::Simple(s) => line(out, b'+', s),
        Value::Error(s) => line(out, b'-', s),
        Value::Integer(i) => header(out, b':', *i),
        Value::Bulk(b) => bulk(out, b),
        Value::Null if resp3 => out.put_slice(b"_\r\n"),
        Value::Null => out.put_slice(b"$-1\r\n"),
        Value::Array(items) => {
            header(out, b'*', items.len());
            for item in items {
                encode(item, protocol, out);
            }
        }
        Value::Map(pairs) => {
            let (prefix, n) = if resp3 {
                (b'%', pairs.len())
            } else {
                (b'*', pairs.len() * 2)
            };
            header(out, prefix, n);
            for (k, v) in pairs {
                encode(k, protocol, out);
                encode(v, protocol, out);
            }
        }
        Value::Boolean(b) if resp3 => out.put_slice(if *b { b"#t\r\n" } else { b"#f\r\n" }),
        Value::Boolean(b) => line(out, b':', if *b { b"1" } else { b"0" }),
        Value::Double(d) if resp3 => line(out, b',', format_double(*d).as_bytes()),
        Value::Double(d) => bulk(out, format_double(*d).as_bytes()),
    }
}

fn format_double(d: f64) -> String {
    if d.is_infinite() {
        return if d > 0.0 { "inf".into() } else { "-inf".into() };
    }
    format!("{d}")
}
