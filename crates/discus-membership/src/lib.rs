//! SWIM membership for Discus (spec section 6).
//!
//! The core is sans-IO: [`Swim`] takes the time and the messages as input and returns the
//! messages to send. It owns the member view, the failure detector, and the membership decisions.
//! It reads and adopts epochs through the [`Host`] trait. The `discus` crate drives it over QUIC.
//! The `testing` feature adds a deterministic network simulator in [`sim`].

mod config;
mod detector;
mod health;
mod host;
pub mod lease;
pub mod merge;
mod swim;
mod view;

#[cfg(feature = "testing")]
pub mod sim;

pub use config::{Millis, SwimConfig};
pub use detector::Detector;
pub use host::{Event, Host};
#[cfg(feature = "testing")]
pub use lease::MemoryLease;
pub use lease::{LeaseError, LeaseHolder, LeaseProvider, LeaseState, NoneLease};
pub use swim::{
    gained_owners, normal_count, provisional_epoch, suspicion_timeout, Action, Handled, Outcome,
    Phase, Swim,
};
