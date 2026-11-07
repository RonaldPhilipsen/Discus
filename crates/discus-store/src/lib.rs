//! The sharded in-memory data plane.
//!
//! The key hash is xxh3 with [`HASH_SEED`]. It only picks the local shard. Placement uses its own
//! hash and seed in `discus-placement`.

pub mod entry;
pub mod evict;
pub mod glob;
pub mod scan;
pub mod store;
pub mod ttl;
pub mod value;
pub mod zset;

pub use discus_placement::clock::{self, Clock, ManualClock, SystemClock};
pub use discus_placement::{NodeId, Version, NTP64};
pub use entry::{estimate_size, Entry};
pub use evict::Admit;
pub use glob::glob_match;
pub use store::{Store, StoreConfig, HASH_SEED};
pub use ttl::spawn_sweeper;
pub use value::Value;
pub use zset::ZSet;
