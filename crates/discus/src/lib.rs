//! The Discus node: a RESP server over a sharded in-memory store.

pub mod admin;
pub mod build;
pub mod cluster;
pub mod command;
pub mod config;
pub mod control;
pub mod counter;
pub mod lag;
pub mod membership;
pub mod node;
pub mod replication;
pub mod security;
pub mod server;
pub mod telemetry;

pub use config::{Config, LeaseKind, MembershipMode, Peer};
pub use node::{ConnState, EpochPin, Node, Phase};
pub use server::Server;
