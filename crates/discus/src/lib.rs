//! The Discus node: a RESP server over a sharded in-memory store.

pub mod config;
pub mod server;

pub use config::Config;
pub use server::Server;
