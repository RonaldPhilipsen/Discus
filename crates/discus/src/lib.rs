//! The Discus node: a RESP server over a sharded in-memory store.

pub mod admin;
pub mod build;
pub mod cluster;
pub mod command;
pub mod config;
pub mod node;
pub mod server;

pub use config::{Config, Peer};
pub use node::{ConnState, Node, Phase};
pub use server::Server;
