//! An incremental parser for client commands (arrays of bulk strings).

use bytes::{Bytes, BytesMut};

/// A parsed command: the command name followed by its arguments.
pub type Command = Vec<Bytes>;

/// Size limits that the parser enforces before it allocates.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_bulk_bytes: usize,
    pub max_array_len: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { max_bulk_bytes: 67_108_864, max_array_len: 1_048_576 }
    }
}

/// A protocol violation. The connection must close after one.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("ERR Protocol error: {0}")]
    Protocol(String),
    #[error("ERR Protocol error: bulk length {len} exceeds the limit of {limit}")]
    BulkTooLarge { len: usize, limit: usize },
    #[error("ERR Protocol error: array length {len} exceeds the limit of {limit}")]
    ArrayTooLarge { len: usize, limit: usize },
}

/// Longest header line (`*N` or `$N`) that the parser waits for.
const MAX_HEADER_LINE: usize = 32;

/// Feed bytes in, pull complete commands out.
pub struct Parser {
    buf: BytesMut,
    limits: Limits,
}

impl Parser {
    pub fn new(limits: Limits) -> Parser {
        Parser { buf: BytesMut::new(), limits }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Bytes that are buffered and not yet parsed.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Return the next complete command, or `None` when more bytes are needed.
    pub fn next(&mut self) -> Result<Option<Command>, ParseError> {
        loop {
            match self.try_parse()? {
                Attempt::Incomplete => return Ok(None),
                Attempt::Empty(consumed) => {
                    let _ = self.buf.split_to(consumed);
                }
                Attempt::Done { consumed, ranges } => {
                    let frame = self.buf.split_to(consumed).freeze();
                    return Ok(Some(ranges.into_iter().map(|(s, e)| frame.slice(s..e)).collect()));
                }
            }
        }
    }

    fn try_parse(&self) -> Result<Attempt, ParseError> {
        let buf = &self.buf[..];
        let Some((first, mut pos)) = read_line(buf, 0)? else {
            return Ok(Attempt::Incomplete);
        };
        if first.first() != Some(&b'*') {
            return Err(ParseError::Protocol(
                "expected '*' to start a command array".to_string(),
            ));
        }
        let n = parse_len(&first[1..])?;
        if n < 0 {
            return Err(ParseError::Protocol("invalid multibulk length".to_string()));
        }
        let n = n as usize;
        if n > self.limits.max_array_len {
            return Err(ParseError::ArrayTooLarge { len: n, limit: self.limits.max_array_len });
        }
        if n == 0 {
            return Ok(Attempt::Empty(pos));
        }
        let mut ranges = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            let Some((head, next)) = read_line(buf, pos)? else {
                return Ok(Attempt::Incomplete);
            };
            if head.first() != Some(&b'$') {
                return Err(ParseError::Protocol(format!(
                    "expected '$', got '{}'",
                    head.first().map(|b| *b as char).unwrap_or(' ')
                )));
            }
            let len = parse_len(&head[1..])?;
            if len < 0 {
                return Err(ParseError::Protocol("invalid bulk length".to_string()));
            }
            let len = len as usize;
            if len > self.limits.max_bulk_bytes {
                return Err(ParseError::BulkTooLarge { len, limit: self.limits.max_bulk_bytes });
            }
            let end = next + len;
            if buf.len() < end + 2 {
                return Ok(Attempt::Incomplete);
            }
            if &buf[end..end + 2] != b"\r\n" {
                return Err(ParseError::Protocol("bulk string is not CRLF terminated".to_string()));
            }
            ranges.push((next, end));
            pos = end + 2;
        }
        Ok(Attempt::Done { consumed: pos, ranges })
    }
}

enum Attempt {
    Incomplete,
    Empty(usize),
    Done { consumed: usize, ranges: Vec<(usize, usize)> },
}

/// Read one CRLF-terminated line that starts at `from`. Returns the line and the next position.
fn read_line(buf: &[u8], from: usize) -> Result<Option<(&[u8], usize)>, ParseError> {
    let rest = &buf[from..];
    match rest.windows(2).position(|w| w == b"\r\n") {
        Some(i) => Ok(Some((&rest[..i], from + i + 2))),
        None if rest.len() > MAX_HEADER_LINE => {
            Err(ParseError::Protocol("header line too long".to_string()))
        }
        None => Ok(None),
    }
}

fn parse_len(digits: &[u8]) -> Result<i64, ParseError> {
    std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| ParseError::Protocol("invalid length".to_string()))
}
