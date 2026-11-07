//! Owned message types. The codec converts them to and from the generated FlatBuffers tables.

use bytes::Bytes;
use discus_placement::{ClusterId, Epoch, EpochId, NodeId, Version};
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
