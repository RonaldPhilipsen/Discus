//! Encode a [`Value`] to wire bytes.

use bytes::{BufMut, BytesMut};

use crate::{Protocol, Value};

fn line(out: &mut BytesMut, prefix: u8, body: &[u8]) {
    out.put_u8(prefix);
    out.put_slice(body);
    out.put_slice(b"\r\n");
}

/// A simple string or an error ends at its first CRLF, so a CR or LF inside it, which can come
/// from client bytes, becomes a space as in Valkey. Otherwise one reply reads as several.
fn text_line(out: &mut BytesMut, prefix: u8, body: &[u8]) {
    if !body.iter().any(|&b| b == b'\r' || b == b'\n') {
        return line(out, prefix, body);
    }
    out.put_u8(prefix);
    out.extend(
        body.iter()
            .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
    );
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
        Value::Simple(s) => text_line(out, b'+', s),
        Value::Error(s) => text_line(out, b'-', s),
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

/// Rust prints infinities as `inf` and `-inf`, as Redis does.
fn format_double(d: f64) -> String {
    format!("{d}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// A line reply ends at its first CRLF, so CR and LF inside it become spaces, as in Valkey.
    #[test]
    fn a_line_reply_never_carries_cr_or_lf() {
        for protocol in [Protocol::Resp2, Protocol::Resp3] {
            let mut out = BytesMut::new();
            encode(
                &Value::Error(Bytes::from_static(b"ERR a\r\n+OK\r\nb")),
                protocol,
                &mut out,
            );
            encode(
                &Value::Simple(Bytes::from_static(b"x\ny\rz")),
                protocol,
                &mut out,
            );
            assert_eq!(&out[..], b"-ERR a  +OK  b\r\n+x y z\r\n");
        }
    }
}
