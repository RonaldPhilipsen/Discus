//! The Discus node: a RESP server over a sharded in-memory store.

pub mod admin;
pub mod build;
pub mod command;
pub mod config;
pub mod node;
pub mod security;
pub mod server;
pub mod telemetry;

pub use config::Config;
pub use node::{ConnState, Node, Phase};
pub use server::Server;
