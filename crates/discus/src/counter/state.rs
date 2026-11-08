//! The counter state machine (spec section 10.4).
//!
//! Every replica of a group applies the same commands in the same order. A write carries the
//! stamp that the leader took, and it applies at `max(clock, stamp)`, where the clock is the
//! largest stamp applied so far. Expiry follows that time, never the local clock, so every
//! replica reaches the same state. A read is pure: it evaluates expiry at the time it is given and
//! never removes an entry, because a removal on the leader alone would let a later write apply
//! differently there.

use std::collections::BTreeMap;

use bytes::Bytes;
use discus_resp::{Command, Value};

use crate::command::float::{incr_by_float, parse_float_delta, FloatDelta};
use crate::command::string::{deadline, set_options, Expiry};
use crate::command::util::{parse_i64, upper};
use crate::command::CommandError;

/// Bytes that one entry costs beyond its key and value.
pub const ENTRY_OVERHEAD: u64 = 64;

/// The internal command that drops every key expired by its stamp.
pub const PURGE: &[u8] = b"PURGE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterEntry {
    pub value: Bytes,
    pub expires_at_ms: Option<u64>,
}

impl CounterEntry {
    fn alive(&self, now_ms: u64) -> bool {
        self.expires_at_ms.is_none_or(|t| t > now_ms)
    }
}

/// Limits that a write checks inside the state machine. Every node runs with the same flags.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_value_bytes: usize,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CounterState {
    map: BTreeMap<Bytes, CounterEntry>,
    clock_ms: u64,
    bytes: u64,
}

fn cost(key: &[u8], value: &[u8]) -> u64 {
    key.len() as u64 + value.len() as u64 + ENTRY_OVERHEAD
}

const COMMANDS: &[&str] = &[
    "GET",
    "SET",
    "GETDEL",
    "GETEX",
    "MGET",
    "MSET",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "INCRBYFLOAT",
    "APPEND",
    "STRLEN",
    "DEL",
    "EXISTS",
    "TYPE",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "TTL",
    "PTTL",
    "PERSIST",
];

/// True for the commands that counter keys accept (spec section 12).
pub fn accepts(name: &str) -> bool {
    COMMANDS.contains(&name)
}

/// True for the commands that only read. The leader serves them without a log entry.
pub fn is_read(cmd: &Command) -> bool {
    matches!(
        upper(&cmd[0]).as_str(),
        "GET" | "EXISTS" | "TTL" | "PTTL" | "TYPE" | "MGET" | "STRLEN"
    )
}

/// True for the writes that never grow the state. A full reserve still accepts them.
pub fn is_shrinking(cmd: &Command) -> bool {
    matches!(
        upper(&cmd[0]).as_str(),
        "DEL"
            | "GETDEL"
            | "PERSIST"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "FLUSHALL"
            | "FLUSHDB"
            | "PURGE"
    )
}

type Reply = Result<Value, CommandError>;

impl CounterState {
    pub fn new() -> CounterState {
        CounterState::default()
    }

    /// A state from a snapshot.
    pub fn from_parts(clock_ms: u64, entries: Vec<(Bytes, CounterEntry)>) -> CounterState {
        let mut state = CounterState {
            clock_ms,
            ..CounterState::default()
        };
        for (key, entry) in entries {
            state.bytes += cost(&key, &entry.value);
            state.map.insert(key, entry);
        }
        state
    }

    pub fn clock_ms(&self) -> u64 {
        self.clock_ms
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Bytes, &CounterEntry)> {
        self.map.iter()
    }

    pub fn get(&self, key: &[u8]) -> Option<&CounterEntry> {
        self.map.get(key)
    }

    /// True when some key expired by the clock of the state but is still held.
    pub fn expired_keys(&self, now_ms: u64) -> bool {
        let now = now_ms.max(self.clock_ms);
        self.map.values().any(|e| !e.alive(now))
    }

    fn put(&mut self, key: &Bytes, entry: CounterEntry) {
        self.bytes += cost(key, &entry.value);
        if let Some(old) = self.map.insert(key.clone(), entry) {
            self.bytes -= cost(key, &old.value);
        }
    }

    fn take(&mut self, key: &[u8]) -> Option<CounterEntry> {
        let old = self.map.remove(key)?;
        self.bytes -= cost(key, &old.value);
        Some(old)
    }

    /// The live entry of `key` at `now`. An expired entry is removed first.
    fn live(&mut self, key: &[u8], now: u64) -> Option<&mut CounterEntry> {
        if self.map.get(key).is_some_and(|e| !e.alive(now)) {
            self.take(key);
        }
        self.map.get_mut(key)
    }

    fn peek(&self, key: &[u8], now: u64) -> Option<&CounterEntry> {
        self.map.get(key).filter(|e| e.alive(now))
    }

    /// Apply one write at `max(clock, stamp_ms)` and return its reply.
    pub fn apply(&mut self, stamp_ms: u64, cmd: &Command, limits: &Limits) -> Value {
        self.clock_ms = self.clock_ms.max(stamp_ms);
        let now = self.clock_ms;
        if is_read(cmd) {
            return self.read(now, cmd);
        }
        self.write(now, cmd, limits)
            .unwrap_or_else(|e| e.to_value())
    }

    /// Answer a read at `max(clock, now_ms)` without changing the state.
    pub fn read(&self, now_ms: u64, cmd: &Command) -> Value {
        let now = now_ms.max(self.clock_ms);
        self.read_at(now, cmd).unwrap_or_else(|e| e.to_value())
    }

    fn read_at(&self, now: u64, cmd: &Command) -> Reply {
        let name = upper(&cmd[0]);
        let bulk = |key: &[u8]| {
            self.peek(key, now)
                .map_or(Value::Null, |e| Value::Bulk(e.value.clone()))
        };
        Ok(match name.as_str() {
            "GET" => bulk(&cmd[1]),
            "MGET" => Value::Array(cmd[1..].iter().map(|k| bulk(k)).collect()),
            "STRLEN" => Value::Integer(self.peek(&cmd[1], now).map_or(0, |e| e.value.len() as i64)),
            "EXISTS" => Value::Integer(
                cmd[1..]
                    .iter()
                    .filter(|k| self.peek(k, now).is_some())
                    .count() as i64,
            ),
            "TYPE" => Value::simple(if self.peek(&cmd[1], now).is_some() {
                "string"
            } else {
                "none"
            }),
            "TTL" => remaining(self.peek(&cmd[1], now), now, |ms| {
                ((ms + 500) / 1000) as i64
            }),
            "PTTL" => remaining(self.peek(&cmd[1], now), now, |ms| ms as i64),
            other => return Err(CommandError::UnknownCommand(other.to_lowercase())),
        })
    }

    fn write(&mut self, now: u64, cmd: &Command, limits: &Limits) -> Reply {
        let name = upper(&cmd[0]);
        match name.as_str() {
            "SET" => self.set(now, cmd),
            "GETDEL" => Ok(self.getdel(now, &cmd[1])),
            "GETEX" => self.getex(now, cmd),
            "MSET" => {
                if cmd.len().is_multiple_of(2) {
                    return Err(CommandError::WrongArity("MSET".into()));
                }
                for pair in cmd[1..].chunks(2) {
                    self.put(
                        &pair[0],
                        CounterEntry {
                            value: pair[1].clone(),
                            expires_at_ms: None,
                        },
                    );
                }
                Ok(Value::ok())
            }
            "INCR" | "DECR" => {
                let delta = if name == "INCR" { 1 } else { -1 };
                let expiry = increment_expiry(&cmd[2..], now, &name)?;
                self.add(now, &cmd[1], delta, expiry)
            }
            "INCRBY" | "DECRBY" => {
                let n = parse_i64(cmd.get(2).ok_or(CommandError::Syntax)?)?;
                let delta = if name == "INCRBY" {
                    n
                } else {
                    n.checked_neg().ok_or(CommandError::Overflow)?
                };
                let expiry = increment_expiry(&cmd[3..], now, &name)?;
                self.add(now, &cmd[1], delta, expiry)
            }
            "INCRBYFLOAT" => self.add_float(now, &cmd[1], parse_float_delta(&cmd[2])?),
            "APPEND" => self.append(now, &cmd[1], &cmd[2], limits),
            "DEL" => {
                let removed = cmd[1..]
                    .iter()
                    .filter(|k| {
                        let found = self.live(k, now).is_some();
                        self.take(k);
                        found
                    })
                    .count();
                Ok(Value::Integer(removed as i64))
            }
            "EXPIRE" => {
                let ms = span(&cmd[2], 1000, "expire")?;
                Ok(self.set_deadline(now, &cmd[1], now as i128 + ms))
            }
            "PEXPIRE" => {
                let ms = span(&cmd[2], 1, "pexpire")?;
                Ok(self.set_deadline(now, &cmd[1], now as i128 + ms))
            }
            "EXPIREAT" => Ok(self.set_deadline(now, &cmd[1], span(&cmd[2], 1000, "expireat")?)),
            "PEXPIREAT" => Ok(self.set_deadline(now, &cmd[1], span(&cmd[2], 1, "pexpireat")?)),
            "PERSIST" => Ok(Value::Integer(match self.live(&cmd[1], now) {
                Some(e) if e.expires_at_ms.is_some() => {
                    e.expires_at_ms = None;
                    1
                }
                _ => 0,
            })),
            "FLUSHALL" | "FLUSHDB" => {
                self.map.clear();
                self.bytes = 0;
                Ok(Value::ok())
            }
            "PURGE" => {
                let expired: Vec<Bytes> = self
                    .map
                    .iter()
                    .filter(|(_, e)| !e.alive(now))
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in &expired {
                    self.take(k);
                }
                Ok(Value::Integer(expired.len() as i64))
            }
            other => Err(CommandError::UnknownCommand(other.to_lowercase())),
        }
    }

    fn set(&mut self, now: u64, cmd: &Command) -> Reply {
        let o = set_options(cmd, now)?;
        let key = &cmd[1];
        let old = self.live(key, now).cloned();
        let reply = |old: Option<&CounterEntry>| {
            if o.get {
                old.map_or(Value::Null, |e| Value::Bulk(e.value.clone()))
            } else {
                Value::ok()
            }
        };
        if (o.nx && old.is_some()) || (o.xx && old.is_none()) {
            return Ok(if o.get {
                reply(old.as_ref())
            } else {
                Value::Null
            });
        }
        let expires_at_ms = match o.expiry {
            Some(Expiry::At(t)) => Some(t),
            Some(Expiry::Keep) => old.as_ref().and_then(|e| e.expires_at_ms),
            _ => None,
        };
        self.put(
            key,
            CounterEntry {
                value: cmd[2].clone(),
                expires_at_ms,
            },
        );
        Ok(reply(old.as_ref()))
    }

    fn getdel(&mut self, now: u64, key: &[u8]) -> Value {
        if self.live(key, now).is_none() {
            return Value::Null;
        }
        self.take(key).map_or(Value::Null, |e| Value::Bulk(e.value))
    }

    fn getex(&mut self, now: u64, cmd: &Command) -> Reply {
        enum Change {
            None,
            At(u64),
            Persist,
        }
        let change = match cmd.len() {
            2 => Change::None,
            3 if upper(&cmd[2]) == "PERSIST" => Change::Persist,
            4 => {
                let opt = upper(&cmd[2]);
                if !matches!(opt.as_str(), "EX" | "PX" | "EXAT" | "PXAT") {
                    return Err(CommandError::Syntax);
                }
                Change::At(deadline(&opt, &cmd[3], now, "getex")?)
            }
            _ => return Err(CommandError::Syntax),
        };
        Ok(match self.live(&cmd[1], now) {
            None => Value::Null,
            Some(e) => {
                match change {
                    Change::At(t) => e.expires_at_ms = Some(t),
                    Change::Persist => e.expires_at_ms = None,
                    Change::None => {}
                }
                Value::Bulk(e.value.clone())
            }
        })
    }

    fn add(&mut self, now: u64, key: &Bytes, delta: i64, expiry: Option<u64>) -> Reply {
        let old = self.live(key, now).cloned();
        let current = match &old {
            None => 0,
            Some(e) => parse_i64(&e.value)?,
        };
        let next = current.checked_add(delta).ok_or(CommandError::Overflow)?;
        // `INCR ... EX` sets a deadline only on a key without one, so a window never slides.
        let expires_at_ms = old.and_then(|e| e.expires_at_ms).or(expiry);
        self.put(
            key,
            CounterEntry {
                value: Bytes::from(next.to_string()),
                expires_at_ms,
            },
        );
        Ok(Value::Integer(next))
    }

    fn add_float(&mut self, now: u64, key: &Bytes, delta: FloatDelta) -> Reply {
        let old = self.live(key, now).cloned();
        let text = incr_by_float(old.as_ref().map(|e| &e.value[..]), delta)?;
        self.put(
            key,
            CounterEntry {
                value: text.clone(),
                expires_at_ms: old.and_then(|e| e.expires_at_ms),
            },
        );
        Ok(Value::Bulk(text))
    }

    fn append(&mut self, now: u64, key: &Bytes, tail: &[u8], limits: &Limits) -> Reply {
        let old = self.live(key, now).cloned();
        let mut bytes = old.as_ref().map(|e| e.value.to_vec()).unwrap_or_default();
        bytes.extend_from_slice(tail);
        if bytes.len() > limits.max_value_bytes {
            return Err(CommandError::ValueTooLarge);
        }
        let len = bytes.len() as i64;
        self.put(
            key,
            CounterEntry {
                value: Bytes::from(bytes),
                expires_at_ms: old.and_then(|e| e.expires_at_ms),
            },
        );
        Ok(Value::Integer(len))
    }

    fn set_deadline(&mut self, now: u64, key: &[u8], deadline_ms: i128) -> Value {
        if self.live(key, now).is_none() {
            return Value::Integer(0);
        }
        if deadline_ms <= now as i128 {
            self.take(key);
        } else if let Some(e) = self.map.get_mut(key) {
            e.expires_at_ms = Some(deadline_ms as u64);
        }
        Value::Integer(1)
    }
}

fn remaining(entry: Option<&CounterEntry>, now: u64, to_unit: impl Fn(u64) -> i64) -> Value {
    Value::Integer(match entry {
        None => -2,
        Some(e) => match e.expires_at_ms {
            None => -1,
            Some(t) => to_unit(t.saturating_sub(now)),
        },
    })
}

fn span(arg: &[u8], unit_ms: i128, who: &str) -> Result<i128, CommandError> {
    let ms = parse_i64(arg)? as i128 * unit_ms;
    if ms.abs() > (i64::MAX / 2) as i128 {
        return Err(CommandError::Custom(format!(
            "ERR invalid expire time in '{who}' command"
        )));
    }
    Ok(ms)
}

/// The deadline of `INCR key [EX s | PX ms | EXAT s | PXAT ms]`, from the arguments after the
/// key (or after the increment).
fn increment_expiry(rest: &[Bytes], now: u64, name: &str) -> Result<Option<u64>, CommandError> {
    match rest {
        [] => Ok(None),
        [opt, arg] => {
            let opt = upper(opt);
            if !matches!(opt.as_str(), "EX" | "PX" | "EXAT" | "PXAT") {
                return Err(CommandError::Syntax);
            }
            deadline(&opt, arg, now, &name.to_lowercase()).map(Some)
        }
        _ => Err(CommandError::Syntax),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: Limits = Limits {
        max_value_bytes: 1 << 20,
    };

    fn cmd(parts: &[&str]) -> Command {
        parts
            .iter()
            .map(|p| Bytes::copy_from_slice(p.as_bytes()))
            .collect()
    }

    fn run(s: &mut CounterState, at: u64, parts: &[&str]) -> Value {
        s.apply(at, &cmd(parts), &LIMITS)
    }

    fn int(v: i64) -> Value {
        Value::Integer(v)
    }

    fn bulk(s: &str) -> Value {
        Value::Bulk(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn incr_counts_and_keeps_the_ttl() {
        let mut s = CounterState::new();
        assert_eq!(
            run(&mut s, 1, &["SET", "k", "1", "PX", "1000"]),
            Value::ok()
        );
        assert_eq!(run(&mut s, 2, &["INCR", "k"]), int(2));
        assert_eq!(run(&mut s, 3, &["INCRBY", "k", "5"]), int(7));
        assert_eq!(run(&mut s, 4, &["DECRBY", "k", "2"]), int(5));
        assert_eq!(run(&mut s, 5, &["DECR", "k"]), int(4));
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(1001));
        assert_eq!(run(&mut s, 6, &["SET", "s", "abc"]), Value::ok());
        assert!(matches!(run(&mut s, 7, &["INCR", "s"]), Value::Error(_)));
        assert_eq!(
            run(&mut s, 8, &["SET", "m", &i64::MAX.to_string()]),
            Value::ok()
        );
        assert!(matches!(run(&mut s, 9, &["INCR", "m"]), Value::Error(_)));
    }

    #[test]
    fn set_honours_every_option() {
        let mut s = CounterState::new();
        assert_eq!(run(&mut s, 10, &["SET", "k", "a", "NX"]), Value::ok());
        assert_eq!(run(&mut s, 10, &["SET", "k", "b", "NX"]), Value::Null);
        assert_eq!(run(&mut s, 10, &["SET", "x", "b", "XX"]), Value::Null);
        assert_eq!(run(&mut s, 10, &["SET", "k", "c", "XX", "GET"]), bulk("a"));
        assert_eq!(run(&mut s, 10, &["SET", "k", "d", "EX", "5"]), Value::ok());
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(5010));
        assert_eq!(run(&mut s, 10, &["SET", "k", "e", "KEEPTTL"]), Value::ok());
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(5010));
        assert_eq!(run(&mut s, 10, &["SET", "k", "f", "PX", "7"]), Value::ok());
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(17));
        assert_eq!(
            run(&mut s, 10, &["SET", "k", "g", "EXAT", "100"]),
            Value::ok()
        );
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(100_000));
        assert_eq!(
            run(&mut s, 10, &["SET", "k", "h", "PXAT", "900"]),
            Value::ok()
        );
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(900));
        assert_eq!(run(&mut s, 10, &["SET", "k", "i"]), Value::ok());
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), None);
        assert!(matches!(
            run(&mut s, 10, &["SET", "k", "v", "NX", "XX"]),
            Value::Error(_)
        ));
        assert!(matches!(
            run(&mut s, 10, &["SET", "k", "v", "EX", "0"]),
            Value::Error(_)
        ));
        assert!(matches!(
            run(&mut s, 10, &["SET", "k", "v", "BOGUS"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn getex_getdel_append_strlen_incrbyfloat() {
        let mut s = CounterState::new();
        assert_eq!(run(&mut s, 1, &["APPEND", "k", "ab"]), int(2));
        assert_eq!(run(&mut s, 1, &["APPEND", "k", "cd"]), int(4));
        assert_eq!(run(&mut s, 1, &["STRLEN", "k"]), int(4));
        assert_eq!(run(&mut s, 1, &["GETEX", "k", "PX", "50"]), bulk("abcd"));
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(51));
        assert_eq!(run(&mut s, 1, &["GETEX", "k", "PERSIST"]), bulk("abcd"));
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), None);
        assert_eq!(run(&mut s, 1, &["GETDEL", "k"]), bulk("abcd"));
        assert_eq!(run(&mut s, 1, &["GETDEL", "k"]), Value::Null);
        assert_eq!(run(&mut s, 1, &["INCRBYFLOAT", "f", "1.5"]), bulk("1.5"));
        assert_eq!(run(&mut s, 1, &["INCRBYFLOAT", "f", "1"]), bulk("2.5"));
        run(&mut s, 1, &["SET", "g", "0.1"]);
        assert_eq!(run(&mut s, 1, &["INCRBYFLOAT", "g", "0.2"]), bulk("0.3"));
        let small = Limits { max_value_bytes: 3 };
        assert!(matches!(
            s.apply(1, &cmd(&["APPEND", "z", "abcd"]), &small),
            Value::Error(_)
        ));
    }

    #[test]
    fn expiry_commands_match_redis() {
        let mut s = CounterState::new();
        run(&mut s, 1000, &["SET", "k", "v"]);
        assert_eq!(run(&mut s, 1000, &["TTL", "k"]), int(-1));
        assert_eq!(run(&mut s, 1000, &["TTL", "missing"]), int(-2));
        assert_eq!(run(&mut s, 1000, &["EXPIRE", "k", "10"]), int(1));
        assert_eq!(run(&mut s, 1000, &["TTL", "k"]), int(10));
        assert_eq!(run(&mut s, 1000, &["PTTL", "k"]), int(10_000));
        assert_eq!(run(&mut s, 1000, &["PEXPIRE", "k", "500"]), int(1));
        assert_eq!(run(&mut s, 1000, &["PTTL", "k"]), int(500));
        assert_eq!(run(&mut s, 1000, &["PEXPIREAT", "k", "4000"]), int(1));
        assert_eq!(run(&mut s, 1000, &["EXPIREAT", "k", "5"]), int(1));
        assert_eq!(run(&mut s, 1000, &["PERSIST", "k"]), int(1));
        assert_eq!(run(&mut s, 1000, &["PERSIST", "k"]), int(0));
        assert_eq!(run(&mut s, 1000, &["EXPIRE", "k", "-1"]), int(1));
        assert_eq!(run(&mut s, 1000, &["EXISTS", "k"]), int(0));
        assert_eq!(run(&mut s, 1000, &["EXPIRE", "k", "10"]), int(0));
        assert_eq!(run(&mut s, 1000, &["TYPE", "k"]), Value::simple("none"));
        run(&mut s, 1000, &["MSET", "a", "1", "b", "2"]);
        assert_eq!(run(&mut s, 1000, &["TYPE", "a"]), Value::simple("string"));
        assert_eq!(
            run(&mut s, 1000, &["MGET", "a", "x", "b"]),
            Value::Array(vec![bulk("1"), Value::Null, bulk("2")])
        );
        assert_eq!(run(&mut s, 1000, &["EXISTS", "a", "a", "x"]), int(2));
        assert_eq!(run(&mut s, 1000, &["DEL", "a", "b", "x"]), int(2));
    }

    #[test]
    fn incr_ex_sets_the_deadline_only_once() {
        let mut s = CounterState::new();
        assert_eq!(run(&mut s, 1000, &["INCR", "k", "EX", "10"]), int(1));
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(11_000));
        assert_eq!(run(&mut s, 5000, &["INCR", "k", "EX", "10"]), int(2));
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(11_000));
        assert_eq!(
            run(&mut s, 5000, &["INCRBY", "j", "5", "PX", "100"]),
            int(5)
        );
        assert_eq!(s.get(b"j").and_then(|e| e.expires_at_ms), Some(5100));
        assert_eq!(run(&mut s, 5000, &["DECR", "d", "PXAT", "9000"]), int(-1));
        assert_eq!(s.get(b"d").and_then(|e| e.expires_at_ms), Some(9000));
        assert_eq!(
            run(&mut s, 5000, &["DECRBY", "e", "2", "EXAT", "9"]),
            int(-2)
        );
        assert_eq!(s.get(b"e").and_then(|e| e.expires_at_ms), Some(9000));
        assert!(matches!(
            run(&mut s, 5000, &["INCR", "k", "EX"]),
            Value::Error(_)
        ));
        assert!(matches!(
            run(&mut s, 5000, &["INCR", "k", "KEEP", "1"]),
            Value::Error(_)
        ));
        // The window ends, and the next increment opens a new one.
        assert_eq!(run(&mut s, 11_000, &["INCR", "k", "EX", "10"]), int(1));
        assert_eq!(s.get(b"k").and_then(|e| e.expires_at_ms), Some(21_000));
    }

    #[test]
    fn expiry_uses_the_stamp() {
        let mut s = CounterState::new();
        run(&mut s, 0, &["SET", "k", "5", "PXAT", "2000"]);
        assert_eq!(run(&mut s, 1999, &["INCR", "k"]), int(6));
        assert_eq!(run(&mut s, 2000, &["INCR", "k"]), int(1));
    }

    #[test]
    fn the_clock_never_runs_backwards() {
        let mut s = CounterState::new();
        run(&mut s, 900, &["SET", "k", "1", "PXAT", "950"]);
        run(&mut s, 960, &["SET", "x", "1"]);
        assert_eq!(s.clock_ms(), 960);
        assert_eq!(run(&mut s, 500, &["INCR", "k"]), int(1));
        assert_eq!(s.clock_ms(), 960);
    }

    #[test]
    fn a_read_never_changes_the_state() {
        let mut s = CounterState::new();
        run(&mut s, 0, &["SET", "k", "5", "PXAT", "2000"]);
        let (len, bytes) = (s.len(), s.bytes());
        assert_eq!(s.read(5000, &cmd(&["GET", "k"])), Value::Null);
        assert_eq!(s.read(5000, &cmd(&["TTL", "k"])), int(-2));
        assert_eq!((s.len(), s.bytes()), (len, bytes));
        assert_eq!(run(&mut s, 1500, &["INCR", "k"]), int(6));
    }

    #[test]
    fn two_replicas_reach_the_same_state() {
        let names = [
            "INCR", "SET", "DEL", "PEXPIRE", "APPEND", "GETDEL", "INCRBY",
        ];
        let mut rng = fastrand::Rng::with_seed(7);
        let log: Vec<(u64, Command)> = (0..200)
            .map(|i| {
                let key = format!("k{}", rng.u8(0..8));
                let name = names[rng.usize(..names.len())];
                let parts: Vec<String> = match name {
                    "SET" => vec![name.into(), key, rng.u8(..).to_string()],
                    "PEXPIRE" => vec![name.into(), key, rng.u16(1..300).to_string()],
                    "APPEND" => vec![name.into(), key, "x".into()],
                    "INCRBY" => vec![name.into(), key, "3".into()],
                    _ => vec![name.into(), key],
                };
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
                (i * 10 + rng.u64(0..30), cmd(&refs))
            })
            .collect();
        let (mut a, mut b) = (CounterState::new(), CounterState::new());
        for (stamp, c) in &log {
            a.apply(*stamp, c, &LIMITS);
        }
        for (stamp, c) in &log {
            b.apply(*stamp, c, &LIMITS);
        }
        assert_eq!(a, b);
    }

    #[test]
    fn bytes_follow_the_entries() {
        let mut s = CounterState::new();
        run(&mut s, 1, &["SET", "ab", "xyz"]);
        assert_eq!(s.bytes(), 2 + 3 + ENTRY_OVERHEAD);
        run(&mut s, 1, &["APPEND", "ab", "12"]);
        assert_eq!(s.bytes(), 2 + 5 + ENTRY_OVERHEAD);
        run(&mut s, 1, &["SET", "c", "1"]);
        assert_eq!(s.bytes(), 2 + 5 + 1 + 1 + 2 * ENTRY_OVERHEAD);
        run(&mut s, 1, &["DEL", "ab"]);
        assert_eq!(s.bytes(), 2 + ENTRY_OVERHEAD);
        run(&mut s, 1, &["FLUSHALL"]);
        assert_eq!(s.bytes(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn purge_removes_keys_expired_by_its_stamp() {
        let mut s = CounterState::new();
        run(&mut s, 0, &["SET", "a", "1", "PXAT", "100"]);
        run(&mut s, 0, &["SET", "b", "1", "PXAT", "300"]);
        run(&mut s, 0, &["SET", "c", "1"]);
        assert!(s.expired_keys(200));
        assert_eq!(run(&mut s, 200, &["PURGE"]), int(1));
        assert_eq!(s.len(), 2);
        assert!(!s.expired_keys(200));
    }

    #[test]
    fn the_command_lists_are_consistent() {
        assert!(accepts("INCR") && accepts("PERSIST") && !accepts("HSET"));
        assert!(is_read(&cmd(&["get", "k"])) && !is_read(&cmd(&["INCR", "k"])));
        assert!(is_shrinking(&cmd(&["DEL", "k"])) && !is_shrinking(&cmd(&["SET", "k", "v"])));
    }
}
