//! The Discus node-to-node wire contract (spec section 13.2 and 13.3).
//!
//! The `.fbs` files in `schema/` are the contract. `flatc` generates `generated/`, which is
//! checked in, and `scripts/gen-wire.sh` regenerates it. This crate wraps the generated code in
//! owned message types and a decoder that runs the FlatBuffers verifier on every frame and on the
//! nested payload before it reads them.

#[allow(
    clippy::all,
    clippy::unwrap_used,
    dead_code,
    deprecated,
    non_camel_case_types,
    unused_imports,
    mismatched_lifetime_syntaxes,
    unsafe_op_in_unsafe_fn
)]
#[rustfmt::skip]
#[path = "generated/discus_generated.rs"]
mod generated;

/// The generated FlatBuffers types, for tests and for code that builds frames by hand.
pub mod fb {
    pub use crate::generated::discus::wire::*;
}

mod codec;
mod counter;
mod message;
mod resp;

pub use codec::{decode_frame, encode_envelope, encode_frame, DecodeError};
pub use counter::{
    decode_counter_snapshot, decode_hints, encode_counter_snapshot, encode_hints, SnapshotEntries,
};
pub use message::{
    Batch, BatchAck, Capacity, CounterAck, CounterMode, CounterSync, Forward, ForwardMode,
    ForwardReply, Frame, Gossip, GossipKind, GroupHint, HandshakeCode, HandshakeFailed, Hello,
    Liveness, MemberInfo, Message, RaftAppend, RaftConfig, RaftEntry, RaftHeader, RaftLogId,
    RaftMembership, RaftOutcome, RaftPayload, RaftReply, RaftSnapshot, RaftVote, RaftVoteReq,
    Regime, Rumor, TraceContext,
};
pub use resp::encode_command;

/// Capability bits. The agreed set of two peers is the bitwise AND of their offers. New behavior
/// is gated by a bit that both peers advertise, never by the presence of a field.
pub mod caps {
    /// A forward carries the full sender epoch when the peer last reported an older one.
    pub const EPOCH_PUSH: u64 = 1;
    /// The node serves SWIM gossip on the `discus-gossip` ALPN.
    pub const GOSSIP: u64 = 2;
    /// The node takes replication, hints, handoff, repair, and digests on the bulk lane, and
    /// answers tracked reads.
    pub const REPLICATION: u64 = 4;
    /// The node runs the counter tier: leader hints, leader-follower sync, and Raft (spec
    /// section 10).
    pub const COUNTERS: u64 = 8;
    /// The node takes many requests on one command-lane stream, opened with an empty frame, and
    /// answers each with its request ID in any order.
    pub const SHARED_STREAM: u64 = 16;
    /// The dialer takes reads on the shared stream too. The acceptor sends a reply larger than
    /// the shared limit on a new unidirectional stream, and the dialer accepts those streams.
    pub const SHARED_READS: u64 = 32;
    /// Every bit this build knows.
    pub const KNOWN: u64 =
        EPOCH_PUSH | GOSSIP | REPLICATION | COUNTERS | SHARED_STREAM | SHARED_READS;
}

/// The frame limit before `HELLO` completes, and the smallest offer that `HELLO` accepts.
pub const HELLO_FRAME_LIMIT: usize = 65_536;

/// The default maximum frame size: 128 MiB plus 1 MiB, so a forwarded command up to the default
/// request size fits.
pub const DEFAULT_MAX_FRAME: usize = 135_266_304;

/// Verifier depth limit for every frame and nested payload.
pub const MAX_DEPTH: usize = 32;

/// Verifier table limit: two tables per member of the largest collection reply.
pub const MAX_TABLES: usize = 4_194_304;

/// The limits that the decoder enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_frame: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_frame: DEFAULT_MAX_FRAME,
        }
    }
}

impl Limits {
    pub fn verifier(&self) -> flatbuffers::VerifierOptions {
        flatbuffers::VerifierOptions {
            max_depth: MAX_DEPTH,
            max_tables: MAX_TABLES,
            max_apparent_size: self.max_frame,
            ignore_missing_null_terminator: false,
        }
    }
}
