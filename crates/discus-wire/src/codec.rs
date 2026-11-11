//! Frame encoding and the verifying decoder.

use std::net::SocketAddr;

use bytes::Bytes;
use discus_placement::{
    ClusterId, Epoch, EpochId, Member, MemberState, NodeId, Version, Weight, NTP64,
};
use discus_store::{Entry, Value};
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

pub(crate) fn invalid(e: impl std::fmt::Display) -> DecodeError {
    DecodeError::Invalid(e.to_string())
}

pub(crate) fn id128(bytes: [u8; 16]) -> fb::Id128 {
    fb::Id128::new(&bytes)
}

pub(crate) fn node_of(id: &fb::Id128) -> NodeId {
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

/// An enum and its FlatBuffers twin, mapped in both directions. An unknown wire value is an
/// error named by `$what`.
macro_rules! wire_enum {
    ($to:ident, $of:ident, $ty:ident, $what:literal, { $($v:ident => $w:ident),+ $(,)? }) => {
        fn $to(x: $ty) -> fb::$ty {
            match x {
                $($ty::$v => fb::$ty::$w,)+
            }
        }

        fn $of(x: fb::$ty) -> Result<$ty, DecodeError> {
            Ok(match x {
                $(fb::$ty::$w => $ty::$v,)+
                other => return Err(invalid(format!(concat!("unknown ", $what, " {}"), other.0))),
            })
        }
    };
}

wire_enum!(state_to, state_of, MemberState, "member state", {
    Joining => JOINING,
    Normal => NORMAL,
    Leaving => LEAVING,
    Dead => DEAD,
});

wire_enum!(liveness_to, liveness_of, Liveness, "liveness", {
    Alive => ALIVE,
    Suspect => SUSPECT,
    Dead => DEAD,
});

wire_enum!(kind_to, kind_of, GossipKind, "gossip kind", {
    Ping => PING,
    Ack => ACK,
    Nack => NACK,
    PingReq => PING_REQ,
    Sync => SYNC,
    SyncReply => SYNC_REPLY,
    Join => JOIN,
    JoinReply => JOIN_REPLY,
    Leave => LEAVE,
});

fn code_to(c: HandshakeCode) -> fb::HandshakeCode {
    match c {
        HandshakeCode::Other => fb::HandshakeCode::OTHER,
        HandshakeCode::ClusterMismatch => fb::HandshakeCode::CLUSTER_MISMATCH,
        HandshakeCode::FrameTooSmall => fb::HandshakeCode::FRAME_TOO_SMALL,
        HandshakeCode::NotHello => fb::HandshakeCode::NOT_HELLO,
        HandshakeCode::UnexpectedNode => fb::HandshakeCode::UNEXPECTED_NODE,
        HandshakeCode::PlacementMismatch => fb::HandshakeCode::PLACEMENT_MISMATCH,
    }
}

fn code_of(c: fb::HandshakeCode) -> HandshakeCode {
    match c {
        fb::HandshakeCode::CLUSTER_MISMATCH => HandshakeCode::ClusterMismatch,
        fb::HandshakeCode::FRAME_TOO_SMALL => HandshakeCode::FrameTooSmall,
        fb::HandshakeCode::NOT_HELLO => HandshakeCode::NotHello,
        fb::HandshakeCode::UNEXPECTED_NODE => HandshakeCode::UnexpectedNode,
        fb::HandshakeCode::PLACEMENT_MISMATCH => HandshakeCode::PlacementMismatch,
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

fn client_addr_of(s: Option<&str>) -> Result<Option<SocketAddr>, DecodeError> {
    s.map(|a| a.parse().map_err(|_| invalid("a bad client address")))
        .transpose()
}

fn epoch_to<'a>(fbb: &mut FlatBufferBuilder<'a>, epoch: &Epoch) -> WIPOffset<fb::Epoch<'a>> {
    let members: Vec<_> = epoch
        .members()
        .iter()
        .map(|m| {
            let addr = fbb.create_string(&m.addr.to_string());
            let rpc_addr = fbb.create_string(&m.rpc_addr.to_string());
            let client_addr = m.client_addr.map(|a| fbb.create_string(&a.to_string()));
            let client_host = m.client_host.as_deref().map(|h| fbb.create_string(h));
            fb::Member::create(
                fbb,
                &fb::MemberArgs {
                    id: Some(&id128(m.id.to_be_bytes())),
                    addr: Some(addr),
                    rpc_addr: Some(rpc_addr),
                    client_addr,
                    client_host,
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
        let rpc_addr = m
            .rpc_addr()
            .and_then(|a| a.parse().ok())
            .ok_or_else(|| invalid("member without a valid RPC address"))?;
        members.push(Member {
            id: node_of(mid),
            addr,
            rpc_addr,
            client_addr: client_addr_of(m.client_addr())?,
            client_host: m.client_host().map(str::to_owned),
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

fn cluster_of(id: &fb::Id128) -> ClusterId {
    ClusterId::from_u128(u128::from_be_bytes(id.bytes().into()))
}

fn info_to<'a>(fbb: &mut FlatBufferBuilder<'a>, m: &MemberInfo) -> WIPOffset<fb::MemberInfo<'a>> {
    let addr = fbb.create_string(&m.addr.to_string());
    let rpc_addr = fbb.create_string(&m.rpc_addr.to_string());
    let client_addr = m.client_addr.map(|a| fbb.create_string(&a.to_string()));
    let client_host = m.client_host.as_deref().map(|h| fbb.create_string(h));
    let hints = (!m.leader_hints.is_empty()).then(|| fbb.create_vector(&m.leader_hints));
    fb::MemberInfo::create(
        fbb,
        &fb::MemberInfoArgs {
            id: Some(&id128(m.id.to_be_bytes())),
            addr: Some(addr),
            rpc_addr: Some(rpc_addr),
            client_addr,
            client_host,
            state: state_to(m.state),
            incarnation: m.incarnation,
            weight: m.weight.get(),
            epoch: Some(&epoch_id_to(m.epoch)),
            capabilities: m.capabilities,
            capacity_used: m.capacity.used,
            capacity_max: m.capacity.max,
            leader_hints: hints,
        },
    )
}

fn info_of(m: fb::MemberInfo<'_>) -> Result<MemberInfo, DecodeError> {
    Ok(MemberInfo {
        id: node_of(m.id().ok_or_else(|| invalid("member info without an id"))?),
        addr: m
            .addr()
            .and_then(|a| a.parse().ok())
            .ok_or_else(|| invalid("member info without a valid address"))?,
        rpc_addr: m
            .rpc_addr()
            .and_then(|a| a.parse().ok())
            .ok_or_else(|| invalid("member info without a valid RPC address"))?,
        client_addr: client_addr_of(m.client_addr())?,
        client_host: m.client_host().map(str::to_owned),
        state: state_of(m.state())?,
        incarnation: m.incarnation(),
        weight: Weight::new(m.weight()).ok_or_else(|| invalid("member info with a bad weight"))?,
        epoch: epoch_id_of(
            m.epoch()
                .ok_or_else(|| invalid("member info without an epoch"))?,
        ),
        capabilities: m.capabilities(),
        capacity: Capacity {
            used: m.capacity_used(),
            max: m.capacity_max(),
        },
        leader_hints: m
            .leader_hints()
            .map(|h| Bytes::copy_from_slice(h.bytes()))
            .unwrap_or_default(),
    })
}

fn gossip_to(fbb: &mut FlatBufferBuilder<'_>, g: &Gossip) {
    let from = info_to(fbb, &g.from);
    let body = g.epoch_body.as_ref().map(|e| epoch_to(fbb, e));
    let rumors: Vec<_> = g
        .rumors
        .iter()
        .map(|r| {
            let info = info_to(fbb, &r.info);
            let accuser = r.accuser.map(|a| id128(a.to_be_bytes()));
            fb::Rumor::create(
                fbb,
                &fb::RumorArgs {
                    info: Some(info),
                    liveness: liveness_to(r.liveness),
                    accuser: accuser.as_ref(),
                },
            )
        })
        .collect();
    let rumors = fbb.create_vector(&rumors);
    let target_id = g.target.map(|(id, _)| id128(id.to_be_bytes()));
    let target_addr = g.target.map(|(_, a)| fbb.create_string(&a.to_string()));
    let t = fb::Gossip::create(
        fbb,
        &fb::GossipArgs {
            kind: kind_to(g.kind),
            from: Some(from),
            cluster_id: Some(&id128(g.cluster_id.to_be_bytes())),
            cluster_size: g.cluster_size,
            epoch_body: body,
            hlc: g.hlc,
            target_id: target_id.as_ref(),
            target_addr,
            rumors: Some(rumors),
            retry_after_ms: g.retry_after_ms,
        },
    );
    fbb.finish_minimal(t);
}

fn gossip_of(g: fb::Gossip<'_>) -> Result<Gossip, DecodeError> {
    let target = match (g.target_id(), g.target_addr()) {
        (Some(id), Some(addr)) => Some((
            node_of(id),
            addr.parse()
                .map_err(|_| invalid("gossip with a bad target address"))?,
        )),
        _ => None,
    };
    let mut rumors = Vec::new();
    for r in g.rumors().into_iter().flatten() {
        rumors.push(Rumor {
            info: info_of(r.info().ok_or_else(|| invalid("rumor without a member"))?)?,
            liveness: liveness_of(r.liveness())?,
            accuser: r.accuser().map(node_of),
        });
    }
    Ok(Gossip {
        kind: kind_of(g.kind())?,
        from: info_of(g.from().ok_or_else(|| invalid("gossip without a sender"))?)?,
        cluster_id: g.cluster_id().map(cluster_of).unwrap_or(ClusterId::NIL),
        cluster_size: g.cluster_size(),
        epoch_body: g.epoch_body().map(epoch_of).transpose()?,
        hlc: g.hlc(),
        target,
        rumors,
        retry_after_ms: g.retry_after_ms(),
    })
}

fn blobs<'a, 'b>(
    fbb: &mut FlatBufferBuilder<'a>,
    items: impl Iterator<Item = &'b [u8]>,
) -> WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<fb::Blob<'a>>>> {
    let items: Vec<_> = items
        .map(|b| {
            let data = fbb.create_vector(b);
            fb::Blob::create(fbb, &fb::BlobArgs { data: Some(data) })
        })
        .collect();
    fbb.create_vector(&items)
}

fn blobs_of(
    v: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<fb::Blob<'_>>>>,
) -> Vec<Bytes> {
    v.into_iter()
        .flatten()
        .map(|b| {
            b.data()
                .map(|d| Bytes::copy_from_slice(d.bytes()))
                .unwrap_or_default()
        })
        .collect()
}

fn stored_to<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    key: &[u8],
    e: &Entry,
) -> WIPOffset<fb::StoredEntry<'a>> {
    let key = fbb.create_vector(key);
    let (kind, bytes, items, scores) = match &e.value {
        Value::Str(b) => (
            fb::ValueKind::STRING,
            Some(fbb.create_vector(b)),
            None,
            None,
        ),
        Value::Hash(h) => {
            let flat = h.iter().flat_map(|(k, v)| [&k[..], &v[..]]);
            (fb::ValueKind::HASH, None, Some(blobs(fbb, flat)), None)
        }
        Value::Set(s) => {
            let items = blobs(fbb, s.iter().map(|m| &m[..]));
            (fb::ValueKind::SET, None, Some(items), None)
        }
        Value::ZSet(z) => {
            let all = z.range(0, -1, false);
            let scores: Vec<f64> = all.iter().map(|(_, s)| *s).collect();
            let items = blobs(fbb, all.into_iter().map(|(m, _)| &m[..]));
            let scores = fbb.create_vector(&scores);
            (fb::ValueKind::ZSET, None, Some(items), Some(scores))
        }
    };
    let version = version_to(e.version);
    fb::StoredEntry::create(
        fbb,
        &fb::StoredEntryArgs {
            key: Some(key),
            kind,
            bytes,
            items,
            scores,
            expires_at_ms: e.expires_at_ms.unwrap_or(0),
            version: Some(&version),
            tombstone: e.tombstone,
        },
    )
}

fn stored_of(s: fb::StoredEntry<'_>) -> Result<(Bytes, Entry), DecodeError> {
    let key = s
        .key()
        .map(|k| Bytes::copy_from_slice(k.bytes()))
        .ok_or_else(|| invalid("entry without a key"))?;
    let items = blobs_of(s.items());
    let value = match s.kind() {
        fb::ValueKind::STRING => Value::Str(
            s.bytes()
                .map(|b| Bytes::copy_from_slice(b.bytes()))
                .unwrap_or_default(),
        ),
        fb::ValueKind::HASH => {
            if !items.len().is_multiple_of(2) {
                return Err(invalid("a hash entry with an odd item count"));
            }
            let mut h = std::collections::HashMap::with_capacity(items.len() / 2);
            let mut it = items.into_iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                h.insert(k, v);
            }
            Value::Hash(h)
        }
        fb::ValueKind::SET => Value::Set(items.into_iter().collect()),
        fb::ValueKind::ZSET => {
            let scores: Vec<f64> = s.scores().map(|v| v.iter().collect()).unwrap_or_default();
            if scores.len() != items.len() {
                return Err(invalid("a sorted set entry with mismatched scores"));
            }
            let mut z = discus_store::ZSet::new();
            for (m, score) in items.into_iter().zip(scores) {
                z.insert(m, score);
            }
            Value::ZSet(z)
        }
        other => return Err(invalid(format!("unknown value kind {}", other.0))),
    };
    let version = version_of(
        s.version()
            .ok_or_else(|| invalid("entry without a version"))?,
    );
    let expires = s.expires_at_ms();
    Ok((
        key,
        Entry {
            value,
            expires_at_ms: (expires != 0).then_some(expires),
            last_access_ms: 0,
            version,
            tombstone: s.tombstone(),
        },
    ))
}

fn batch_to(fbb: &mut FlatBufferBuilder<'_>, b: &Batch) {
    let entries: Vec<_> = b
        .entries
        .iter()
        .map(|(k, e)| stored_to(fbb, k, e))
        .collect();
    let entries = fbb.create_vector(&entries);
    let offers: Vec<_> = b
        .offers
        .iter()
        .map(|(k, v)| {
            let key = fbb.create_vector(k);
            let version = version_to(*v);
            fb::KeyVersion::create(
                fbb,
                &fb::KeyVersionArgs {
                    key: Some(key),
                    version: Some(&version),
                },
            )
        })
        .collect();
    let offers = fbb.create_vector(&offers);
    let discard = fbb.create_vector(&b.discard);
    let done_from = b.done.map(|(f, _)| epoch_id_to(f));
    let done_to = b.done.map(|(_, t)| epoch_id_to(t));
    let t = fb::EntryBatch::create(
        fbb,
        &fb::EntryBatchArgs {
            epoch: Some(&epoch_id_to(b.epoch)),
            entries: Some(entries),
            offers: Some(offers),
            discard_buckets: Some(discard),
            done_from: done_from.as_ref(),
            done_to: done_to.as_ref(),
        },
    );
    fbb.finish_minimal(t);
}

fn batch_of(b: fb::EntryBatch<'_>) -> Result<Batch, DecodeError> {
    let mut entries = Vec::new();
    for e in b.entries().into_iter().flatten() {
        entries.push(stored_of(e)?);
    }
    let mut offers = Vec::new();
    for o in b.offers().into_iter().flatten() {
        let key = o
            .key()
            .map(|k| Bytes::copy_from_slice(k.bytes()))
            .ok_or_else(|| invalid("offer without a key"))?;
        let version = version_of(
            o.version()
                .ok_or_else(|| invalid("offer without a version"))?,
        );
        offers.push((key, version));
    }
    let done = match (b.done_from(), b.done_to()) {
        (Some(f), Some(t)) => Some((epoch_id_of(f), epoch_id_of(t))),
        _ => None,
    };
    Ok(Batch {
        epoch: epoch_id_of(b.epoch().ok_or_else(|| invalid("batch without an epoch"))?),
        entries,
        offers,
        discard: b
            .discard_buckets()
            .map(|d| d.bytes().to_vec())
            .unwrap_or_default(),
        done,
    })
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
                    track: f.track,
                },
            );
            fbb.finish_minimal(t);
        }
        Message::ForwardReply(r) => {
            let value = to_wire(&mut fbb, &r.value);
            let applied = r.applied.map(version_to);
            let observed = r.observed.map(version_to);
            let t = fb::ForwardReply::create(
                &mut fbb,
                &fb::ForwardReplyArgs {
                    value: Some(value),
                    applied: applied.as_ref(),
                    epoch: Some(&epoch_id_to(r.epoch)),
                    observed: observed.as_ref(),
                    settled: r.settled,
                },
            );
            fbb.finish_minimal(t);
        }
        Message::Ping { nonce } | Message::Pong { nonce } => {
            let t = fb::Ping::create(&mut fbb, &fb::PingArgs { nonce: *nonce });
            fbb.finish_minimal(t);
        }
        Message::Gossip(g) => gossip_to(&mut fbb, g),
        Message::Replicate(b)
        | Message::HintReplay(b)
        | Message::Handoff(b)
        | Message::Entries(b) => batch_to(&mut fbb, b),
        Message::BatchAck(a) => {
            let want = blobs(&mut fbb, a.want.iter().map(|k| &k[..]));
            let t = fb::BatchAck::create(
                &mut fbb,
                &fb::BatchAckArgs {
                    applied: a.applied,
                    want: Some(want),
                    epoch: Some(&epoch_id_to(a.epoch)),
                    idle: a.idle,
                },
            );
            fbb.finish_minimal(t);
        }
        Message::Fetch { keys } => {
            let keys = blobs(&mut fbb, keys.iter().map(|k| &k[..]));
            let t = fb::Fetch::create(&mut fbb, &fb::FetchArgs { keys: Some(keys) });
            fbb.finish_minimal(t);
        }
        m @ (Message::RaftLeader(_)
        | Message::RaftConfig(_)
        | Message::RaftVote(_)
        | Message::RaftAppend(_)
        | Message::RaftSnapshot(_)
        | Message::RaftReply(_)
        | Message::CounterSync(_)
        | Message::CounterAck(_)) => {
            crate::counter::encode(&mut fbb, m);
        }
        Message::Digest { from, to, sums } => {
            let sums = fbb.create_vector(sums);
            let t = fb::Digest::create(
                &mut fbb,
                &fb::DigestArgs {
                    from: Some(&epoch_id_to(*from)),
                    to: Some(&epoch_id_to(*to)),
                    sums: Some(sums),
                },
            );
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
                track: f.track(),
            })
        }
        T::FORWARD_REPLY => {
            let r =
                flatbuffers::root_with_opts::<fb::ForwardReply>(&opts, payload).map_err(invalid)?;
            Message::ForwardReply(ForwardReply {
                value: from_wire(r.value().ok_or_else(|| invalid("reply without a value"))?)?,
                applied: r.applied().map(version_of),
                epoch: epoch_id_of(r.epoch().ok_or_else(|| invalid("reply without an epoch"))?),
                observed: r.observed().map(version_of),
                settled: r.settled(),
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
        T::GOSSIP => {
            let g = flatbuffers::root_with_opts::<fb::Gossip>(&opts, payload).map_err(invalid)?;
            Message::Gossip(gossip_of(g)?)
        }
        T::REPLICATE | T::HINT_REPLAY | T::HANDOFF | T::ENTRIES => {
            let b =
                flatbuffers::root_with_opts::<fb::EntryBatch>(&opts, payload).map_err(invalid)?;
            let b = batch_of(b)?;
            match env.type_() {
                T::REPLICATE => Message::Replicate(b),
                T::HINT_REPLAY => Message::HintReplay(b),
                T::HANDOFF => Message::Handoff(b),
                _ => Message::Entries(b),
            }
        }
        T::BATCH_ACK => {
            let a = flatbuffers::root_with_opts::<fb::BatchAck>(&opts, payload).map_err(invalid)?;
            Message::BatchAck(BatchAck {
                applied: a.applied(),
                want: blobs_of(a.want()),
                epoch: epoch_id_of(a.epoch().ok_or_else(|| invalid("ack without an epoch"))?),
                idle: a.idle(),
            })
        }
        T::FETCH => {
            let f = flatbuffers::root_with_opts::<fb::Fetch>(&opts, payload).map_err(invalid)?;
            Message::Fetch {
                keys: blobs_of(f.keys()),
            }
        }
        T::DIGEST => {
            let d = flatbuffers::root_with_opts::<fb::Digest>(&opts, payload).map_err(invalid)?;
            Message::Digest {
                from: epoch_id_of(d.from().ok_or_else(|| invalid("digest without an epoch"))?),
                to: epoch_id_of(d.to().ok_or_else(|| invalid("digest without an epoch"))?),
                sums: d.sums().map(|s| s.iter().collect()).unwrap_or_default(),
            }
        }
        other => match crate::counter::decode(other, payload, &opts) {
            Some(decoded) => decoded?,
            None => {
                return Err(DecodeError::UnknownType {
                    message_type: other.0,
                    request_id,
                    trace,
                })
            }
        },
    };
    Ok(Frame {
        request_id,
        trace,
        message,
    })
}
