//! The sharded in-memory data plane.
//!
//! The key hash is xxh3 with [`HASH_SEED`]. It is part of the placement contract: a change to the
//! hash or the seed is a breaking change.

pub mod clock;
pub mod entry;
pub mod store;
pub mod ttl;
pub mod value;

pub use clock::{Clock, ManualClock, SystemClock};
pub use entry::{estimate_size, Entry};
pub use store::{Store, StoreConfig, HASH_SEED};
pub use ttl::spawn_sweeper;
pub use value::Value;
