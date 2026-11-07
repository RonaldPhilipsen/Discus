//! Frame encoding and the verifying decoder.

use bytes::Bytes;
use discus_placement::{
    ClusterId, Epoch, EpochId, Member, MemberState, NodeId, Version, Weight, NTP64,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::fb;
use crate::message::*;
use crate::resp::{from_wire, to_wire};
use crate::Limits;

/// Why a frame did not decode.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DecodeError {
    /// The frame or its nested payload failed the verifier, or a required field is missing. The
    /// receiver drops the frame, logs it, and counts it.
    #[error("invalid frame: {0}")]
    Invalid(String),
    /// The envelope is valid but names a message type this build does not know. The receiver
    /// answers `UNSUPPORTED` and keeps the connection.
    #[error("unknown message type {message_type}")]
    UnknownType {
        message_type: u16,
        request_id: u64,
        trace: TraceContext,
    },
}

fn invalid(e: impl std::fmt::Display) -> DecodeError {
    DecodeError::Invalid(e.to_string())
}

fn id128(bytes: [u8; 16]) -> fb::Id128 {
    fb::Id128::new(&bytes)
}

fn node_of(id: &fb::Id128) -> NodeId {
    NodeId::from_u128(u128::from_be_bytes(id.bytes().into()))
}

fn epoch_id_to(e: EpochId) -> fb::EpochId {
    fb::EpochId::new(e.generation, &id128(e.origin.to_be_bytes()))
}

fn epoch_id_of(e: &fb::EpochId) -> EpochId {
    EpochId {
        generation: e.generation(),
        origin: node_of(e.origin()),
    }
}

fn state_to(s: MemberState) -> fb::MemberState {
    match s {
        MemberState::Joining => fb::MemberState::JOINING,
        MemberState::Normal => fb::MemberState::NORMAL,
        MemberState::Leaving => fb::MemberState::LEAVING,
        MemberState::Dead => fb::MemberState::DEAD,
    }
}

fn state_of(s: fb::MemberState) -> Result<MemberState, DecodeError> {
    Ok(match s {
        fb::MemberState::JOINING => MemberState::Joining,
        fb::MemberState::NORMAL => MemberState::Normal,
        fb::MemberState::LEAVING => MemberState::Leaving,
        fb::MemberState::DEAD => MemberState::Dead,
        other => return Err(invalid(format!("unknown member state {}", other.0))),
    })
}

fn code_to(c: HandshakeCode) -> fb::HandshakeCode {
    match c {
        HandshakeCode::Other => fb::HandshakeCode::OTHER,
        HandshakeCode::ClusterMismatch => fb::HandshakeCode::CLUSTER_MISMATCH,
        HandshakeCode::FrameTooSmall => fb::HandshakeCode::FRAME_TOO_SMALL,
        HandshakeCode::NotHello => fb::HandshakeCode::NOT_HELLO,
        HandshakeCode::UnexpectedNode => fb::HandshakeCode::UNEXPECTED_NODE,
    }
}

fn code_of(c: fb::HandshakeCode) -> HandshakeCode {
    match c {
        fb::HandshakeCode::CLUSTER_MISMATCH => HandshakeCode::ClusterMismatch,
        fb::HandshakeCode::FRAME_TOO_SMALL => HandshakeCode::FrameTooSmall,
        fb::HandshakeCode::NOT_HELLO => HandshakeCode::NotHello,
        fb::HandshakeCode::UNEXPECTED_NODE => HandshakeCode::UnexpectedNode,
        // An unknown code from a newer build is still a failed handshake.
        _ => HandshakeCode::Other,
    }
}

fn version_to(v: Version) -> fb::Version {
    fb::Version::new(v.time().0, &id128(v.as_uhlc().get_id().to_le_bytes()))
}

fn version_of(v: &fb::Version) -> Version {
    let origin = NodeId::from_u128(u128::from_le_bytes(v.id().bytes().into()));
    Version::new(NTP64(v.time()), origin)
}

fn epoch_to<'a>(fbb: &mut FlatBufferBuilder<'a>, epoch: &Epoch) -> WIPOffset<fb::Epoch<'a>> {
    let members: Vec<_> = epoch
        .members()
        .iter()
        .map(|m| {
            let addr = fbb.create_string(&m.addr.to_string());
            fb::Member::create(
                fbb,
                &fb::MemberArgs {
                    id: Some(&id128(m.id.to_be_bytes())),
                    addr: Some(addr),
                    state: state_to(m.state),
                    weight: m.weight.get(),
                },
            )
        })
        .collect();
    let members = fbb.create_vector(&members);
    fb::Epoch::create(
        fbb,
        &fb::EpochArgs {
            id: Some(&epoch_id_to(epoch.id())),
            cluster_id: Some(&id128(epoch.cluster_id().to_be_bytes())),
            members: Some(members),
        },
    )
}

fn epoch_of(e: fb::Epoch<'_>) -> Result<Epoch, DecodeError> {
    let id = e.id().ok_or_else(|| invalid("epoch without an id"))?;
    let cluster = e
        .cluster_id()
        .ok_or_else(|| invalid("epoch without a cluster id"))?;
    let mut members = Vec::new();
    for m in e.members().into_iter().flatten() {
        let mid = m.id().ok_or_else(|| invalid("member without an id"))?;
        let addr = m
            .addr()
            .and_then(|a| a.parse().ok())
            .ok_or_else(|| invalid("member without a valid address"))?;
        members.push(Member {
            id: node_of(mid),
            addr,
            state: state_of(m.state())?,
            weight: Weight::new(m.weight()).ok_or_else(|| invalid("member with a bad weight"))?,
        });
    }
    Epoch::new(
        epoch_id_of(id),
        ClusterId::from_u128(u128::from_be_bytes(cluster.bytes().into())),
        members,
    )
    .map_err(invalid)
}

/// Encode the body of a message as its own FlatBuffer.
fn encode_body(message: &Message) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    match message {
        Message::Hello(h) => {
            let t = fb::Hello::create(
                &mut fbb,
                &fb::HelloArgs {
                    node_id: Some(&id128(h.node_id.to_be_bytes())),
                    cluster_id: Some(&id128(h.cluster_id.to_be_bytes())),
                    capabilities: h.capabilities,
                    epoch: Some(&epoch_id_to(h.epoch)),
                    max_frame: h.max_frame,
                },
            );
            fbb.finish_minimal(t);
        }
        Message::HandshakeFailed(f) => {
            let reason = fbb.create_string(&f.reason);
            let t = fb::HandshakeFailed::create(
                &mut fbb,
                &fb::HandshakeFailedArgs {
                    code: code_to(f.code),
                    reason: Some(reason),
                },
            );
            fbb.finish_minimal(t);
        }
        Message::Unsupported { message_type } => {
            let t = fb::Unsupported::create(
                &mut fbb,
                &fb::UnsupportedArgs {
                    message_type: *message_type,
                },
            );
            fbb.finish_minimal(t);
        }
        Message::Forward(f) => {
            let command = fbb.create_vector(&f.command);
            let body = f.epoch_body.as_ref().map(|e| epoch_to(&mut fbb, e));
            let path: Vec<fb::Id128> = f.path.iter().map(|n| id128(n.to_be_bytes())).collect();
            let path = fbb.create_vector(&path);
            let t = fb::Forward::create(
                &mut fbb,
                &fb::ForwardArgs {
                    command: Some(command),
                    epoch: Some(&epoch_id_to(f.epoch)),
                    epoch_body: body,
                    hops: f.hops,
                    path: Some(path),
                    mode: match f.mode {
                        ForwardMode::Route => fb::ForwardMode::ROUTE,
                        ForwardMode::Local => fb::ForwardMode::LOCAL,
                    },
                },
            );
            fbb.finish_minimal(t);
        }
        Message::ForwardReply(r) => {
            let value = to_wire(&mut fbb, &r.value);
            let applied = r.applied.map(version_to);
            let t = fb::ForwardReply::create(
                &mut fbb,
                &fb::ForwardReplyArgs {
                    value: Some(value),
                    applied: applied.as_ref(),
                    epoch: Some(&epoch_id_to(r.epoch)),
                },
            );
            fbb.finish_minimal(t);
        }
        Message::Ping { nonce } | Message::Pong { nonce } => {
            let t = fb::Ping::create(&mut fbb, &fb::PingArgs { nonce: *nonce });
            fbb.finish_minimal(t);
        }
    }
    fbb.finished_data().to_vec()
}

/// Encode an `Envelope` around an already encoded payload. Tests use it to build frames with
/// unknown types or broken payloads.
pub fn encode_envelope(
    message_type: u16,
    request_id: u64,
    trace: &TraceContext,
    payload: &[u8],
) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(payload.len() + 128);
    let payload = fbb.create_vector(payload);
    let traceparent = trace.traceparent.as_deref().map(|s| fbb.create_string(s));
    let tracestate = trace.tracestate.as_deref().map(|s| fbb.create_string(s));
    let env = fb::Envelope::create(
        &mut fbb,
        &fb::EnvelopeArgs {
            type_: fb::MessageType(message_type),
            request_id,
            traceparent,
            tracestate,
            payload: Some(payload),
        },
    );
    fbb.finish_minimal(env);
    fbb.finished_data().to_vec()
}

/// Encode a frame as `Envelope` bytes, without the length prefix.
pub fn encode_frame(frame: &Frame) -> Vec<u8> {
    let body = encode_body(&frame.message);
    encode_envelope(
        frame.message.type_code(),
        frame.request_id,
        &frame.trace,
        &body,
    )
}

/// Verify and decode a frame. The verifier runs on the envelope and then on the nested payload
/// with the depth, table, and size limits of `limits`.
pub fn decode_frame(bytes: &[u8], limits: &Limits) -> Result<Frame, DecodeError> {
    if bytes.len() > limits.max_frame {
        return Err(invalid(format!(
            "frame of {} bytes exceeds the limit of {}",
            bytes.len(),
            limits.max_frame
        )));
    }
    let opts = limits.verifier();
    let env = flatbuffers::root_with_opts::<fb::Envelope>(&opts, bytes).map_err(invalid)?;
    let trace = TraceContext {
        traceparent: env.traceparent().map(str::to_owned),
        tracestate: env.tracestate().map(str::to_owned),
    };
    let request_id = env.request_id();
    let payload = env.payload().map(|p| p.bytes()).unwrap_or_default();
    use fb::MessageType as T;
    let message = match env.type_() {
        T::HELLO => {
            let h = flatbuffers::root_with_opts::<fb::Hello>(&opts, payload).map_err(invalid)?;
            Message::Hello(Hello {
                node_id: node_of(
                    h.node_id()
                        .ok_or_else(|| invalid("HELLO without a node id"))?,
                ),
                cluster_id: ClusterId::from_u128(u128::from_be_bytes(
                    h.cluster_id()
                        .ok_or_else(|| invalid("HELLO without a cluster id"))?
                        .bytes()
                        .into(),
                )),
                capabilities: h.capabilities(),
                epoch: epoch_id_of(h.epoch().ok_or_else(|| invalid("HELLO without an epoch"))?),
                max_frame: h.max_frame(),
            })
        }
        T::HANDSHAKE_FAILED => {
            let f = flatbuffers::root_with_opts::<fb::HandshakeFailed>(&opts, payload)
                .map_err(invalid)?;
            Message::HandshakeFailed(HandshakeFailed {
                code: code_of(f.code()),
                reason: f.reason().unwrap_or_default().to_owned(),
            })
        }
        T::UNSUPPORTED => {
            let u =
                flatbuffers::root_with_opts::<fb::Unsupported>(&opts, payload).map_err(invalid)?;
            Message::Unsupported {
                message_type: u.message_type(),
            }
        }
        T::FORWARD => {
            let f = flatbuffers::root_with_opts::<fb::Forward>(&opts, payload).map_err(invalid)?;
            Message::Forward(Forward {
                command: f
                    .command()
                    .map(|c| Bytes::copy_from_slice(c.bytes()))
                    .unwrap_or_default(),
                epoch: epoch_id_of(
                    f.epoch()
                        .ok_or_else(|| invalid("FORWARD without an epoch"))?,
                ),
                epoch_body: f.epoch_body().map(epoch_of).transpose()?,
                hops: f.hops(),
                path: f.path().into_iter().flatten().map(node_of).collect(),
                mode: match f.mode() {
                    fb::ForwardMode::ROUTE => ForwardMode::Route,
                    fb::ForwardMode::LOCAL => ForwardMode::Local,
                    other => return Err(invalid(format!("unknown forward mode {}", other.0))),
                },
            })
        }
        T::FORWARD_REPLY => {
            let r =
                flatbuffers::root_with_opts::<fb::ForwardReply>(&opts, payload).map_err(invalid)?;
            Message::ForwardReply(ForwardReply {
                value: from_wire(r.value().ok_or_else(|| invalid("reply without a value"))?)?,
                applied: r.applied().map(version_of),
                epoch: epoch_id_of(r.epoch().ok_or_else(|| invalid("reply without an epoch"))?),
            })
        }
        T::PING | T::PONG => {
            let p = flatbuffers::root_with_opts::<fb::Ping>(&opts, payload).map_err(invalid)?;
            if env.type_() == T::PING {
                Message::Ping { nonce: p.nonce() }
            } else {
                Message::Pong { nonce: p.nonce() }
            }
        }
        other => {
            return Err(DecodeError::UnknownType {
                message_type: other.0,
                request_id,
                trace,
            })
        }
    };
    Ok(Frame {
        request_id,
        trace,
        message,
    })
}
