//! Command parsing, dispatch, and handlers.

pub mod connection;
pub mod error;
pub mod table;

pub use error::CommandError;
pub use table::{dispatch, lookup, CommandSpec, Handler};
