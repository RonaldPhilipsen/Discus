//! An incremental parser for client commands: arrays of bulk strings, and inline commands (one
//! line of words, as `redis-cli` and `telnet` send), as Valkey reads them.

use bytes::{Buf, Bytes, BytesMut};

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

/// Longest inline command, as `PROTO_INLINE_MAX_SIZE` of Valkey.
pub const MAX_INLINE: usize = 64 * 1024;

/// The free room that [`Parser::buffer`] makes for one read.
const READ_MIN: usize = 64 * 1024;

/// A read buffer above this capacity is replaced once the parser consumes the command that grew
/// it, so one large command does not keep its memory for the life of the connection.
pub const MAX_IDLE_BUFFER: usize = 1 << 20;

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

    /// Replace the limits. They apply from the next header that the parser reads.
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// The buffer that new bytes go into, with room for at least `READ_MIN` of them.
    pub fn buffer(&mut self) -> &mut BytesMut {
        self.buf.reserve(READ_MIN);
        &mut self.buf
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
                    self.buf.advance(consumed);
                }
                Attempt::Inline { consumed, args } => {
                    self.buf.advance(consumed);
                    return Ok(Some(args));
                }
                // Each argument gets its own allocation. A slice of the read buffer would keep
                // the whole buffer alive while a stored value lives, outside the memory budget.
                Attempt::Done { consumed, ranges } => {
                    let cmd = ranges
                        .into_iter()
                        .map(|(s, e)| Bytes::copy_from_slice(&self.buf[s..e]))
                        .collect();
                    let large = self.buf.capacity() > MAX_IDLE_BUFFER;
                    self.buf.advance(consumed);
                    if large && self.buf.len() < MAX_IDLE_BUFFER {
                        let mut fresh = BytesMut::with_capacity(self.buf.len().max(READ_MIN));
                        fresh.extend_from_slice(&self.buf);
                        self.buf = fresh;
                    }
                    return Ok(Some(cmd));
                }
            }
        }
    }

    fn try_parse(&mut self) -> Result<Attempt, ParseError> {
        let buf = &self.buf[..];
        let p = match &mut self.pending {
            Some(p) => p,
            None => {
                match buf.first() {
                    None => return Ok(Attempt::Incomplete),
                    Some(b'*') => {}
                    Some(_) => return self.inline(),
                }
                let Some((first, pos)) = read_line(buf, 0)? else {
                    return Ok(Attempt::Incomplete);
                };
                let n = parse_len(&first[1..])
                    .ok_or_else(|| ParseError::Protocol("invalid multibulk length".to_string()))?;
                // Valkey skips an array of zero or fewer items.
                if n <= 0 {
                    return Ok(Attempt::Empty(pos));
                }
                let n = n as usize;
                if n > self.limits.max_array_len {
                    return Err(ParseError::ArrayTooLarge {
                        len: n,
                        limit: self.limits.max_array_len,
                    });
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
                    let len = parse_len(&head[1..])
                        .filter(|n| *n >= 0)
                        .ok_or_else(|| ParseError::Protocol("invalid bulk length".to_string()))?;
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
    Inline {
        consumed: usize,
        args: Command,
    },
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

/// A length as `string2ll` of Valkey reads it: no sign `+`, no leading zero, and no `-0`.
fn parse_len(text: &[u8]) -> Option<i64> {
    let digits = text.strip_prefix(b"-").unwrap_or(text);
    if digits.first() == Some(&b'0') && (digits.len() > 1 || digits.len() < text.len()) {
        return None;
    }
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(text).ok()?.parse().ok()
}

impl Parser {
    /// An inline command: one line that ends at LF, with an optional CR before it.
    fn inline(&self) -> Result<Attempt, ParseError> {
        let buf = &self.buf[..];
        let Some(end) = buf.iter().position(|&b| b == b'\n') else {
            if buf.len() > MAX_INLINE {
                return Err(ParseError::Protocol("too big inline request".to_string()));
            }
            return Ok(Attempt::Incomplete);
        };
        if end > MAX_INLINE {
            return Err(ParseError::Protocol("too big inline request".to_string()));
        }
        let line = buf[..end].strip_suffix(b"\r").unwrap_or(&buf[..end]);
        let args = split_args(line)
            .ok_or_else(|| ParseError::Protocol("unbalanced quotes in request".to_string()))?;
        if args.len() > self.limits.max_array_len {
            return Err(ParseError::ArrayTooLarge {
                len: args.len(),
                limit: self.limits.max_array_len,
            });
        }
        if let Some(arg) = args.iter().find(|a| a.len() > self.limits.max_bulk_bytes) {
            return Err(ParseError::BulkTooLarge {
                len: arg.len(),
                limit: self.limits.max_bulk_bytes,
            });
        }
        if args.is_empty() {
            return Ok(Attempt::Empty(end + 1));
        }
        Ok(Attempt::Inline {
            consumed: end + 1,
            args,
        })
    }
}

/// Split a line into words as `sdssplitargs` of Valkey does. A word can be in double quotes,
/// with `\xHH` and C escapes, or in single quotes, with `\'`. A closing quote must end the word.
/// Returns `None` for unbalanced quotes. Valkey reads the line as a C string, so it ends at the
/// first NUL byte.
fn split_args(line: &[u8]) -> Option<Command> {
    let line = &line[..line.iter().position(|&b| b == 0).unwrap_or(line.len())];
    let at = |i: usize| line.get(i).copied();
    let is_space =
        |b: Option<u8>| matches!(b, Some(b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r'));
    let mut args = Vec::new();
    let mut i = 0;
    loop {
        while is_space(at(i)) {
            i += 1;
        }
        if i >= line.len() {
            return Some(args);
        }
        let (mut in_double, mut in_single) = (false, false);
        let mut word = Vec::new();
        loop {
            let c = at(i);
            if in_double {
                let hex = |j: usize| at(j).filter(u8::is_ascii_hexdigit);
                match c? {
                    b'\\'
                        if at(i + 1) == Some(b'x')
                            && hex(i + 2).is_some()
                            && hex(i + 3).is_some() =>
                    {
                        let digits = std::str::from_utf8(&line[i + 2..i + 4]).ok()?;
                        word.push(u8::from_str_radix(digits, 16).ok()?);
                        i += 3;
                    }
                    b'\\' if at(i + 1).is_some() => {
                        i += 1;
                        word.push(match line[i] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x08,
                            b'a' => 0x07,
                            other => other,
                        });
                    }
                    b'"' => {
                        if at(i + 1).is_some() && !is_space(at(i + 1)) {
                            return None;
                        }
                        i += 1;
                        break;
                    }
                    other => word.push(other),
                }
            } else if in_single {
                match c? {
                    b'\\' if at(i + 1) == Some(b'\'') => {
                        word.push(b'\'');
                        i += 1;
                    }
                    b'\'' => {
                        if at(i + 1).is_some() && !is_space(at(i + 1)) {
                            return None;
                        }
                        i += 1;
                        break;
                    }
                    other => word.push(other),
                }
            } else {
                match c {
                    None | Some(b' ' | b'\n' | b'\r' | b'\t') => break,
                    Some(b'"') => in_double = true,
                    Some(b'\'') => in_single = true,
                    Some(other) => word.push(other),
                }
            }
            i += 1;
        }
        args.push(Bytes::from(word));
    }
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

    /// A parsed value owns its bytes, so it does not keep the read buffer alive.
    #[test]
    fn an_argument_does_not_share_the_read_buffer() {
        let mut p = Parser::new(Limits::default());
        p.feed(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*1\r\n$4\r\nPI");
        let cmd = p.next().unwrap().unwrap();
        assert!(cmd.iter().all(Bytes::is_unique));
        assert!(p.next().unwrap().is_none());
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

    /// A large command leaves a large read buffer behind. It must not stay with the connection.
    #[test]
    fn a_large_command_does_not_keep_a_large_buffer() {
        let mut p = Parser::new(Limits::default());
        let value = vec![b'v'; 8 << 20];
        let mut wire = format!("*2\r\n$3\r\nSET\r\n${}\r\n", value.len()).into_bytes();
        wire.extend_from_slice(&value);
        wire.extend_from_slice(b"\r\n*1\r\n$4\r\nPI");
        p.buffer().extend_from_slice(&wire);
        assert_eq!(p.next().unwrap().unwrap()[1].len(), value.len());
        assert!(p.next().unwrap().is_none());
        // `capacity` counts only the room after the read position, and `reserve` takes back the
        // room before it, so the size of the allocation shows after `buffer`.
        let capacity = p.buffer().capacity();
        assert!(capacity <= MAX_IDLE_BUFFER, "{capacity}");
        p.feed(b"NG\r\n");
        assert_eq!(p.next().unwrap().unwrap(), vec!["PING"]);
    }

    fn all(p: &mut Parser) -> Result<Vec<Command>, ParseError> {
        let mut out = Vec::new();
        while let Some(cmd) = p.next()? {
            out.push(cmd);
        }
        Ok(out)
    }

    fn parsed(wire: &[u8]) -> Result<Vec<Command>, String> {
        let mut p = Parser::new(Limits::default());
        p.feed(wire);
        all(&mut p).map_err(|e| e.to_string())
    }

    /// redis-server 8.10.2 ignores an array header of zero or less.
    #[test]
    fn a_negative_array_length_is_ignored() {
        assert_eq!(
            parsed(b"*-1\r\n*0\r\n*1\r\n$4\r\nPING\r\n").unwrap(),
            vec![vec!["PING"]]
        );
    }

    /// redis-server 8.10.2 reads lengths with `string2ll`: no sign `+` and no leading zero.
    #[test]
    fn a_length_is_canonical() {
        for wire in [
            &b"*1\r\n$+4\r\nPING\r\n"[..],
            b"*1\r\n$04\r\nPING\r\n",
            b"*1\r\n$-0\r\n",
        ] {
            assert_eq!(
                parsed(wire).unwrap_err(),
                "ERR Protocol error: invalid bulk length"
            );
        }
        for wire in [&b"*+1\r\n$4\r\nPING\r\n"[..], b"*01\r\n$4\r\nPING\r\n"] {
            assert_eq!(
                parsed(wire).unwrap_err(),
                "ERR Protocol error: invalid multibulk length"
            );
        }
    }

    /// Inline commands split as `sdssplitargs` of redis-server 8.10.2 does.
    #[test]
    fn an_inline_command_splits_like_valkey() {
        let wire = b"PING\r\n\r\n  \r\nECHO hello\nSET \"a b\" 'c d'\r\n\
                     ECHO \"\\x41\\n\\\"\" 'it\\'s' a\"b c\"\r\n";
        assert_eq!(
            parsed(wire).unwrap(),
            vec![
                vec![&b"PING"[..]],
                vec![b"ECHO", b"hello"],
                vec![b"SET", b"a b", b"c d"],
                vec![b"ECHO", b"A\n\"", b"it's", b"ab c"],
            ]
        );
        assert_eq!(
            parsed(b"ECHO \"abc\r\n").unwrap_err(),
            "ERR Protocol error: unbalanced quotes in request"
        );
        assert_eq!(
            parsed(b"ECHO \"a\"b\r\n").unwrap_err(),
            "ERR Protocol error: unbalanced quotes in request"
        );
    }

    /// A partial inline command waits for its line, up to the inline limit.
    #[test]
    fn an_inline_command_waits_for_its_line_within_a_limit() {
        let mut p = Parser::new(Limits::default());
        p.feed(b"ECHO hel");
        assert!(p.next().unwrap().is_none());
        p.feed(b"lo\r\n");
        assert_eq!(p.next().unwrap().unwrap(), vec!["ECHO", "hello"]);
        p.feed(&vec![b'a'; MAX_INLINE + 1]);
        assert_eq!(
            p.next().unwrap_err().to_string(),
            "ERR Protocol error: too big inline request"
        );
    }

    /// The array limit holds for inline commands, so the limits before authentication hold.
    #[test]
    fn an_inline_command_keeps_the_limits() {
        let mut p = Parser::new(Limits {
            max_bulk_bytes: 8,
            max_array_len: 2,
        });
        p.feed(b"A B C\r\n");
        assert!(matches!(p.next(), Err(ParseError::ArrayTooLarge { .. })));
        let mut p = Parser::new(Limits {
            max_bulk_bytes: 8,
            max_array_len: 2,
        });
        p.feed(b"ECHO 123456789\r\n");
        assert!(matches!(p.next(), Err(ParseError::BulkTooLarge { .. })));
    }
}
