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
        Limits {
            max_bulk_bytes: 67_108_864,
            max_array_len: 1_048_576,
        }
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
    /// The progress on the command at the start of `buf`, kept between reads so a large command
    /// that arrives in many reads costs time in proportion to its size, not its size squared.
    pending: Option<Pending>,
}

/// A command array whose header was read, with the arguments parsed so far.
struct Pending {
    len: usize,
    /// Where the next header starts, or the body of `bulk` when it is set.
    pos: usize,
    ranges: Vec<(usize, usize)>,
    /// The start and length of the bulk string whose header was read and whose body is not
    /// complete yet.
    bulk: Option<(usize, usize)>,
}

impl Parser {
    pub fn new(limits: Limits) -> Parser {
        Parser {
            buf: BytesMut::new(),
            limits,
            pending: None,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Bytes that are buffered and not yet parsed.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Return the next complete command, or `None` when more bytes are needed.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Command>, ParseError> {
        loop {
            match self.try_parse()? {
                Attempt::Incomplete => return Ok(None),
                Attempt::Empty(consumed) => {
                    let _ = self.buf.split_to(consumed);
                }
                Attempt::Done { consumed, ranges } => {
                    let frame = self.buf.split_to(consumed).freeze();
                    return Ok(Some(
                        ranges.into_iter().map(|(s, e)| frame.slice(s..e)).collect(),
                    ));
                }
            }
        }
    }

    fn try_parse(&mut self) -> Result<Attempt, ParseError> {
        let buf = &self.buf[..];
        let p = match &mut self.pending {
            Some(p) => p,
            None => {
                let Some((first, pos)) = read_line(buf, 0)? else {
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
                    return Err(ParseError::ArrayTooLarge {
                        len: n,
                        limit: self.limits.max_array_len,
                    });
                }
                if n == 0 {
                    return Ok(Attempt::Empty(pos));
                }
                self.pending.insert(Pending {
                    len: n,
                    pos,
                    ranges: Vec::with_capacity(n.min(1024)),
                    bulk: None,
                })
            }
        };
        while p.ranges.len() < p.len {
            let (start, len) = match p.bulk {
                Some(bulk) => bulk,
                None => {
                    let Some((head, next)) = read_line(buf, p.pos)? else {
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
                        return Err(ParseError::BulkTooLarge {
                            len,
                            limit: self.limits.max_bulk_bytes,
                        });
                    }
                    p.bulk = Some((next, len));
                    (next, len)
                }
            };
            let end = start + len;
            if buf.len() < end + 2 {
                return Ok(Attempt::Incomplete);
            }
            if &buf[end..end + 2] != b"\r\n" {
                return Err(ParseError::Protocol(
                    "bulk string is not CRLF terminated".to_string(),
                ));
            }
            p.ranges.push((start, end));
            p.pos = end + 2;
            p.bulk = None;
        }
        let done = Attempt::Done {
            consumed: p.pos,
            ranges: std::mem::take(&mut p.ranges),
        };
        self.pending = None;
        Ok(done)
    }
}

enum Attempt {
    Incomplete,
    Empty(usize),
    Done {
        consumed: usize,
        ranges: Vec<(usize, usize)>,
    },
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// The headers of a partial command are read once. The parser keeps its progress between
    /// reads, so it never looks at the bytes it parsed already. This test overwrites those
    /// bytes: a parser that starts over sees garbage.
    #[test]
    fn a_partial_command_is_not_parsed_again() {
        let mut p = Parser::new(Limits::default());
        p.feed(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nva");
        assert!(p.next().unwrap().is_none());
        p.buf[0] = b'!';
        p.buf[4] = b'!';
        p.buf[13] = b'!';
        p.feed(b"lue\r\n");
        let cmd = p.next().unwrap().unwrap();
        assert_eq!(cmd, vec!["SET", "k", "value"]);
        p.feed(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(p.next().unwrap().unwrap(), vec!["PING"]);
    }

    /// A command of many arguments that arrives one argument per read reads each header once.
    #[test]
    fn a_long_command_in_many_reads_keeps_every_argument() {
        let mut p = Parser::new(Limits::default());
        p.feed(b"*1000\r\n");
        for i in 0..1000 {
            assert!(p.next().unwrap().is_none());
            p.buf[0] = b'!';
            if i > 0 {
                p.buf[7 + 7 * (i - 1)] = b'!';
            }
            p.feed(b"$1\r\nx\r\n");
        }
        let cmd = p.next().unwrap().unwrap();
        assert_eq!(cmd.len(), 1000);
        assert!(cmd.iter().all(|a| a.as_ref() == b"x"));
    }
}
