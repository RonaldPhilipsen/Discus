//! Identity, placement, epochs, and time for Discus nodes.
//!
//! This crate depends on no other Discus crate. The store and the node build on it.

pub mod id;
pub mod member;

pub use id::{ClusterId, NodeId, ParseIdError};
pub use member::{Member, MemberState, Weight};
