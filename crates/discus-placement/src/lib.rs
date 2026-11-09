//! Identity, placement, epochs, and time for Discus nodes.
//!
//! This crate depends on no other Discus crate. The store and the node build on it.

pub mod clock;
pub mod epoch;
pub mod history;
pub mod hlc;
pub mod hrw;
pub mod id;
pub mod member;
#[cfg(feature = "mock-clock")]
pub mod mock;

pub use clock::{Clock, SystemClock};
pub use epoch::{Epoch, EpochError, EpochId, FIRST_GENERATION};
pub use history::{Adopt, EpochHistory, MIN_HISTORY};
pub use hlc::{Hlc, HlcError, Version, MAX_DRIFT};
pub use hrw::{owners, DEFAULT_RF, PLACEMENT_SEED};
pub use id::{ClusterId, NodeId, ParseIdError};
pub use member::{Member, MemberState, Weight};
#[cfg(feature = "mock-clock")]
pub use mock::ManualClock;
pub use uhlc::NTP64;
