//! Command parsing, dispatch, and handlers.

pub mod coll;
pub mod connection;
pub mod error;
pub mod hash;
pub mod keys;
pub mod set;
pub mod string;
pub mod table;
pub mod util;
pub mod zset;

pub use error::CommandError;
pub use table::{dispatch, lookup, names, CommandSpec, Handler};
