//! The hybrid logical clock, spec §8.1, built on the `uhlc` crate.
//!
//! A [`Version`] is a `uhlc::Timestamp`: a 64-bit NTP time (32 bits of seconds since the Unix
//! epoch and 32 bits of fraction, whose low 4 bits are the logical counter) plus a 16-byte ID. The
//! ID is the origin `NodeId`, so conflicts resolve by time and then by origin through the
//! timestamp order of `uhlc`. That order compares the ID as its 16 little-endian bytes. This
//! format is part of the wire contract of later plans.
//!
//! `uhlc` does not bound what it issues. [`Hlc`] adds that: it never hands out a timestamp
//! [`MAX_DRIFT`] or more ahead of the wall clock, and it drops incoming timestamps more than
//! `MAX_DRIFT` ahead. A clock that cannot issue within the bound is pinned: writes get
//! `-TRYAGAIN` until the wall clock catches up, and [`Hlc::refusals`] counts the refused ticks for
//! an alarm.

use std::fmt;
use std::num::NonZeroU128;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use uhlc::{HLCBuilder, Timestamp, CSIZE, HLC, ID, NTP64};

use crate::NodeId;

/// How far ahead of the wall clock an HLC timestamp may get: 1 s.
pub const MAX_DRIFT: NTP64 = NTP64(1 << 32);

/// The bits of an NTP64 time that `uhlc` keeps for the physical part.
const PHYSICAL_MASK: u64 = !((1u64 << CSIZE) - 1);

/// The wall clock as NTP64, for production. `uhlc` needs a plain function.
pub fn system_clock() -> NTP64 {
    NTP64::from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO),
    )
}

fn uhlc_id(node: NodeId) -> ID {
    // A random NodeId is never zero. NIL maps to 1, because uhlc IDs cannot be zero.
    ID::from(NonZeroU128::new(node.as_u128()).unwrap_or(NonZeroU128::MIN))
}

/// The version of an entry: the HLC time of the last write and the node that issued it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(Timestamp);

impl Version {
    pub fn new(time: NTP64, origin: NodeId) -> Version {
        Version(Timestamp::new(time, uhlc_id(origin)))
    }

    /// The lowest version, for an entry that no write stamped yet.
    pub fn zero() -> Version {
        Version::new(NTP64(0), NodeId::NIL)
    }

    pub fn time(&self) -> NTP64 {
        *self.0.get_time()
    }

    pub fn origin(&self) -> NodeId {
        NodeId::from_u128(u128::from_le_bytes(self.0.get_id().to_le_bytes()))
    }

    pub fn as_uhlc(&self) -> &Timestamp {
        &self.0
    }
}

impl From<Timestamp> for Version {
    fn from(ts: Timestamp) -> Version {
        Version(ts)
    }
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

/// Milliseconds from `now` to `later`, rounded to the nearest millisecond.
fn ahead_ms(later: NTP64, now: NTP64) -> u64 {
    let nanos = later
        .to_duration()
        .saturating_sub(now.to_duration())
        .as_nanos();
    ((nanos + 500_000) / 1_000_000) as u64
}

/// A hybrid logical clock for one node, shared by every connection.
pub struct Hlc {
    origin: NodeId,
    clock: fn() -> NTP64,
    inner: HLC,
    /// The highest time issued or observed. `uhlc` does not expose it, so the wrapper keeps it
    /// to answer [`Hlc::is_pinned`].
    last: AtomicU64,
    refusals: AtomicU64,
}

impl Hlc {
    /// A clock on `clock` that starts from the wall clock, as the bootstrap node does. A joining
    /// node calls [`Hlc::set_floor`] with the cluster maximum before it serves writes.
    pub fn new(origin: NodeId, clock: fn() -> NTP64) -> Hlc {
        Hlc {
            origin,
            clock,
            inner: HLCBuilder::new()
                .with_id(uhlc_id(origin))
                .with_clock(clock)
                .with_max_delta(MAX_DRIFT.to_duration())
                .build(),
            last: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
        }
    }

    pub fn origin(&self) -> NodeId {
        self.origin
    }

    /// The highest time issued or observed.
    pub fn last(&self) -> NTP64 {
        NTP64(self.last.load(Ordering::Acquire))
    }

    /// Issue a new version for a local write. It is greater than every version issued or
    /// observed before. A clock at its drift bound refuses.
    pub fn tick(&self) -> Result<Version, HlcError> {
        let ts = self.inner.new_timestamp();
        self.last
            .fetch_max(ts.get_time().as_u64(), Ordering::AcqRel);
        if *ts.get_time() >= (self.clock)() + MAX_DRIFT {
            self.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(HlcError::Pinned);
        }
        Ok(Version(ts))
    }

    /// Merge a version from another node, so later ticks are greater. A version more than
    /// `MAX_DRIFT` ahead of the wall clock is rejected and changes nothing.
    pub fn observe(&self, remote: Version) -> Result<(), HlcError> {
        self.inner
            .update_with_timestamp(&remote.0)
            .map_err(|_| HlcError::TooFarAhead {
                ahead_ms: ahead_ms(remote.time(), (self.clock)()),
            })?;
        self.last
            .fetch_max(remote.time().as_u64(), Ordering::AcqRel);
        Ok(())
    }

    /// Raise the clock to at least `floor`, the cluster maximum that a joining node receives. The
    /// bound is the same as for [`Hlc::observe`].
    pub fn set_floor(&self, floor: NTP64) -> Result<(), HlcError> {
        self.observe(Version::new(floor, self.origin))
    }

    /// True when the next tick refuses, because the clock is at its drift bound.
    pub fn is_pinned(&self) -> bool {
        let now = (self.clock)();
        let last = self.last.load(Ordering::Acquire);
        let physical = now.as_u64() & PHYSICAL_MASK;
        let next = if physical > last & PHYSICAL_MASK {
            physical
        } else {
            last + 1
        };
        NTP64(next) >= now + MAX_DRIFT
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
