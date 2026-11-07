//! Owned message types. The codec converts them to and from the generated FlatBuffers tables.

use std::net::SocketAddr;

use bytes::Bytes;
use discus_placement::{ClusterId, Epoch, EpochId, MemberState, NodeId, Version, Weight};
use discus_resp::Value;

/// W3C Trace Context, copied unchanged across every hop.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

/// Why `HELLO` failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeCode {
    Other,
    ClusterMismatch,
    FrameTooSmall,
    NotHello,
    UnexpectedNode,
}

/// How the receiver of a forward acts on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardMode {
    /// Route again if the receiver does not own the keys.
    Route,
    /// Execute on the receiver with no routing.
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub node_id: NodeId,
    pub cluster_id: ClusterId,
    pub capabilities: u64,
    pub epoch: EpochId,
    pub max_frame: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeFailed {
    pub code: HandshakeCode,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Forward {
    /// The raw command as a RESP multibulk.
    pub command: Bytes,
    pub epoch: EpochId,
    /// The full sender epoch, sent only with the `EPOCH_PUSH` capability.
    pub epoch_body: Option<Epoch>,
    /// Forwards so far, including this one.
    pub hops: u8,
    /// The nodes that forwarded this command, in order.
    pub path: Vec<NodeId>,
    pub mode: ForwardMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForwardReply {
    pub value: Value,
    /// The HLC stamp that the command took, if it wrote.
    pub applied: Option<Version>,
    /// The epoch of the receiver after it acted.
    pub epoch: EpochId,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Hello(Hello),
    HandshakeFailed(HandshakeFailed),
    Unsupported { message_type: u16 },
    Forward(Forward),
    ForwardReply(ForwardReply),
    Ping { nonce: u64 },
    Pong { nonce: u64 },
    Gossip(Gossip),
}

impl Message {
    /// The `MessageType` value on the wire.
    pub fn type_code(&self) -> u16 {
        use crate::fb::MessageType as T;
        match self {
            Message::Hello(_) => T::HELLO.0,
            Message::HandshakeFailed(_) => T::HANDSHAKE_FAILED.0,
            Message::Unsupported { .. } => T::UNSUPPORTED.0,
            Message::Forward(_) => T::FORWARD.0,
            Message::ForwardReply(_) => T::FORWARD_REPLY.0,
            Message::Ping { .. } => T::PING.0,
            Message::Pong { .. } => T::PONG.0,
            Message::Gossip(_) => T::GOSSIP.0,
        }
    }
}

/// One frame: the envelope fields and the decoded body.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub request_id: u64,
    pub trace: TraceContext,
    pub message: Message,
}

/// Capacity hints of a member, in bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capacity {
    pub used: u64,
    pub max: u64,
}

/// The gossip payload of one member (spec section 6.1).
#[derive(Debug, Clone, PartialEq)]
pub struct MemberInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub state: MemberState,
    /// Only the member itself raises its incarnation.
    pub incarnation: u64,
    pub weight: Weight,
    /// The epoch that the member holds.
    pub epoch: EpochId,
    /// The same capability bits as `HELLO`.
    pub capabilities: u64,
    pub capacity: Capacity,
    /// Counter leader hints. Opaque to membership.
    pub leader_hints: Bytes,
}

/// What the sender believes about a member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Liveness {
    Alive,
    Suspect,
    Dead,
}

/// One member update. A suspect rumor names its accuser.
#[derive(Debug, Clone, PartialEq)]
pub struct Rumor {
    pub info: MemberInfo,
    pub liveness: Liveness,
    pub accuser: Option<NodeId>,
}

/// The kind of a gossip body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GossipKind {
    Ping,
    Ack,
    Nack,
    PingReq,
    Sync,
    SyncReply,
    Join,
    JoinReply,
    Leave,
}

/// One gossip message. Every message carries the sender's own payload.
#[derive(Debug, Clone, PartialEq)]
pub struct Gossip {
    pub kind: GossipKind,
    pub from: MemberInfo,
    /// `ClusterId::NIL` when the sender is not in a cluster yet.
    pub cluster_id: ClusterId,
    /// The `NORMAL` members in the sender's current epoch.
    pub cluster_size: u32,
    /// The full sender epoch, when the receiver has an older one.
    pub epoch_body: Option<Epoch>,
    /// The sender's HLC as a raw `NTP64`, or the cluster maximum in a join reply.
    pub hlc: u64,
    /// The probe target of a `PING_REQ`.
    pub target: Option<(NodeId, SocketAddr)>,
    pub rumors: Vec<Rumor>,
    /// A refused join waits this long before it tries again.
    pub retry_after_ms: u32,
}

impl Gossip {
    /// A message of `kind` from `from` with every other field empty.
    pub fn new(kind: GossipKind, from: MemberInfo) -> Gossip {
        Gossip {
            kind,
            from,
            cluster_id: ClusterId::NIL,
            cluster_size: 0,
            epoch_body: None,
            hlc: 0,
            target: None,
            rumors: Vec::new(),
            retry_after_ms: 0,
        }
    }
}
