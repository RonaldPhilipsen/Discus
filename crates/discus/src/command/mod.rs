//! Command parsing, dispatch, and handlers.

pub mod connection;
pub mod error;
pub mod keys;
pub mod string;
pub mod table;
pub mod util;

pub use error::CommandError;
pub use table::{dispatch, lookup, CommandSpec, Handler};
