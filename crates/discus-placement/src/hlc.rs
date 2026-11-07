//! The hybrid logical clock, spec §8.1.
//!
//! A [`Timestamp`] packs 48 bits of wall milliseconds and a 16-bit logical counter into one
//! `u64`, so the packed value orders like the pair. A [`Version`] adds the origin node, which
//! breaks ties: conflicts resolve by HLC, then by origin `NodeId`.
//!
//! The clock never issues a timestamp whose wall part is [`MAX_DRIFT_MS`] or more ahead of the
//! local wall clock, and it drops incoming timestamps more than `MAX_DRIFT_MS` ahead. A clock that
//! cannot issue within the bound is pinned: writes get `-TRYAGAIN` until the wall clock catches
//! up, and [`Hlc::refusals`] counts the refused ticks for an alarm.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{Clock, NodeId};

/// How far ahead of the wall clock an HLC timestamp may get, in milliseconds.
pub const MAX_DRIFT_MS: u64 = 1_000;

const LOGICAL_BITS: u32 = 16;
const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1;
const MAX_WALL_MS: u64 = (1 << (64 - LOGICAL_BITS)) - 1;

/// An HLC timestamp: wall milliseconds, then a logical counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const ZERO: Timestamp = Timestamp(0);

    /// A wall part beyond 48 bits saturates. That is the year 10889.
    pub fn new(wall_ms: u64, logical: u16) -> Timestamp {
        Timestamp((wall_ms.min(MAX_WALL_MS) << LOGICAL_BITS) | u64::from(logical))
    }

    pub fn wall_ms(self) -> u64 {
        self.0 >> LOGICAL_BITS
    }

    pub fn logical(self) -> u16 {
        (self.0 & LOGICAL_MASK) as u16
    }

    pub fn raw(self) -> u64 {
        self.0
    }

    pub fn from_raw(raw: u64) -> Timestamp {
        Timestamp(raw)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.wall_ms(), self.logical())
    }
}

/// The version of an entry: its HLC timestamp and the node that issued it. The derived order
/// compares the timestamp first and the origin second.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Version {
    pub ts: Timestamp,
    pub origin: NodeId,
}

impl Version {
    pub const ZERO: Version = Version {
        ts: Timestamp::ZERO,
        origin: NodeId::NIL,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlcError {
    /// The clock is at its drift bound and cannot issue a timestamp now.
    Pinned,
    /// An incoming timestamp is too far ahead of the local wall clock. It was dropped.
    TooFarAhead { ahead_ms: u64 },
}

impl fmt::Display for HlcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HlcError::Pinned => f.write_str("the clock is at its drift bound"),
            HlcError::TooFarAhead { ahead_ms } => {
                write!(f, "timestamp is {ahead_ms} ms ahead of the wall clock")
            }
        }
    }
}

impl std::error::Error for HlcError {}

/// A hybrid logical clock for one node. Lock-free, shared by every connection.
pub struct Hlc {
    origin: NodeId,
    clock: Arc<dyn Clock>,
    last: AtomicU64,
    refusals: AtomicU64,
}

/// The timestamp after `last` at wall time `wall`, or `None` when it would reach the bound.
fn successor(last: Timestamp, wall: u64) -> Option<Timestamp> {
    let next = if wall > last.wall_ms() {
        Timestamp::new(wall, 0)
    } else if last.logical() < u16::MAX {
        Timestamp(last.0 + 1)
    } else {
        Timestamp::new(last.wall_ms() + 1, 0)
    };
    (next.wall_ms() < wall.saturating_add(MAX_DRIFT_MS)).then_some(next)
}

impl Hlc {
    /// A clock that starts from the wall clock, as the bootstrap node does. A joining node calls
    /// [`Hlc::set_floor`] with the cluster maximum before it serves writes.
    pub fn new(origin: NodeId, clock: Arc<dyn Clock>) -> Hlc {
        Hlc {
            origin,
            clock,
            last: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
        }
    }

    pub fn origin(&self) -> NodeId {
        self.origin
    }

    /// The highest timestamp issued or observed.
    pub fn last(&self) -> Timestamp {
        Timestamp(self.last.load(Ordering::Acquire))
    }

    /// Issue a new version for a local write. It is greater than every timestamp issued or
    /// observed before.
    pub fn tick(&self) -> Result<Version, HlcError> {
        let wall = self.clock.now_ms();
        let mut current = self.last.load(Ordering::Acquire);
        loop {
            let Some(next) = successor(Timestamp(current), wall) else {
                self.refusals.fetch_add(1, Ordering::Relaxed);
                return Err(HlcError::Pinned);
            };
            match self.last.compare_exchange_weak(
                current,
                next.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Version {
                        ts: next,
                        origin: self.origin,
                    })
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Merge a timestamp from another node, so later ticks are greater. A timestamp more than
    /// `MAX_DRIFT_MS` ahead of the wall clock is rejected and changes nothing.
    pub fn observe(&self, remote: Timestamp) -> Result<(), HlcError> {
        let wall = self.clock.now_ms();
        if remote.wall_ms() > wall.saturating_add(MAX_DRIFT_MS) {
            return Err(HlcError::TooFarAhead {
                ahead_ms: remote.wall_ms() - wall,
            });
        }
        self.last.fetch_max(remote.0, Ordering::AcqRel);
        Ok(())
    }

    /// Raise the clock to at least `floor`, the cluster maximum that a joining node receives. The
    /// bound is the same as for [`Hlc::observe`].
    pub fn set_floor(&self, floor: Timestamp) -> Result<(), HlcError> {
        self.observe(floor)
    }

    /// True when the next tick refuses, because the clock is at its drift bound.
    pub fn is_pinned(&self) -> bool {
        successor(self.last(), self.clock.now_ms()).is_none()
    }

    /// Ticks refused at the drift bound since start.
    pub fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hlc")
            .field("origin", &self.origin)
            .field("last", &self.last())
            .finish()
    }
}
