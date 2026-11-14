//! The codec of the counter messages (spec sections 10 and 13.2).

use bytes::Bytes;
use discus_placement::NodeId;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};

use crate::codec::{bytes_of, id128, invalid, node_of, root, DecodeError};
use crate::fb;
use crate::message::*;
use crate::Limits;

fn regime_to(r: Regime) -> fb::Regime {
    fb::Regime::new(r.generation, &id128(r.origin.to_be_bytes()))
}

fn regime_of(r: Option<&fb::Regime>) -> Result<Regime, DecodeError> {
    let r = r.ok_or_else(|| invalid("counter message without a regime"))?;
    Ok(Regime {
        generation: r.generation(),
        origin: node_of(r.origin()),
    })
}

fn log_to(l: RaftLogId) -> fb::RaftLogId {
    fb::RaftLogId::new(l.term, l.leader, l.index)
}

fn log_of(l: &fb::RaftLogId) -> RaftLogId {
    RaftLogId {
        term: l.term(),
        leader: l.leader(),
        index: l.index(),
    }
}

fn vote_to(v: RaftVote) -> fb::RaftVote {
    fb::RaftVote::new(v.term, v.leader, v.committed)
}

fn vote_of(v: Option<&fb::RaftVote>) -> Result<RaftVote, DecodeError> {
    let v = v.ok_or_else(|| invalid("Raft message without a vote"))?;
    Ok(RaftVote {
        term: v.term(),
        leader: v.leader(),
        committed: v.committed(),
    })
}

fn ids_to<'a>(fbb: &mut FlatBufferBuilder<'a>, ids: &[NodeId]) -> WIPOffset<Vector<'a, fb::Id128>> {
    let ids: Vec<fb::Id128> = ids.iter().map(|n| id128(n.to_be_bytes())).collect();
    fbb.create_vector(&ids)
}

fn header_to<'a>(fbb: &mut FlatBufferBuilder<'a>, h: &RaftHeader) -> WIPOffset<fb::RaftHeader<'a>> {
    fb::RaftHeader::create(
        fbb,
        &fb::RaftHeaderArgs {
            group: h.group,
            regime: Some(&regime_to(h.regime)),
            target: h.target,
        },
    )
}

fn header_of(h: Option<fb::RaftHeader<'_>>) -> Result<RaftHeader, DecodeError> {
    let h = h.ok_or_else(|| invalid("Raft message without a header"))?;
    Ok(RaftHeader {
        group: h.group(),
        regime: regime_of(h.regime())?,
        target: h.target(),
    })
}

fn membership_to<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    m: &RaftMembership,
) -> WIPOffset<fb::RaftMembership<'a>> {
    let configs: Vec<_> = m
        .configs
        .iter()
        .map(|c| {
            let ids = fbb.create_vector(c);
            fb::RaftNodeIds::create(fbb, &fb::RaftNodeIdsArgs { ids: Some(ids) })
        })
        .collect();
    let configs = fbb.create_vector(&configs);
    let nodes: Vec<_> = m
        .nodes
        .iter()
        .map(|(replica, node)| {
            fb::RaftPeer::create(
                fbb,
                &fb::RaftPeerArgs {
                    replica: *replica,
                    node: Some(&id128(node.to_be_bytes())),
                },
            )
        })
        .collect();
    let nodes = fbb.create_vector(&nodes);
    fb::RaftMembership::create(
        fbb,
        &fb::RaftMembershipArgs {
            configs: Some(configs),
            nodes: Some(nodes),
        },
    )
}

fn membership_of(m: Option<fb::RaftMembership<'_>>) -> Result<RaftMembership, DecodeError> {
    let m = m.ok_or_else(|| invalid("Raft message without a membership"))?;
    let configs = m
        .configs()
        .into_iter()
        .flatten()
        .map(|c| c.ids().map(|ids| ids.iter().collect()).unwrap_or_default())
        .collect();
    let nodes = m
        .nodes()
        .into_iter()
        .flatten()
        .map(|p| {
            let node = p
                .node()
                .ok_or_else(|| invalid("Raft peer without a node"))?;
            Ok((p.replica(), node_of(node)))
        })
        .collect::<Result<_, DecodeError>>()?;
    Ok(RaftMembership { configs, nodes })
}

fn entry_to<'a>(fbb: &mut FlatBufferBuilder<'a>, e: &RaftEntry) -> WIPOffset<fb::RaftEntry<'a>> {
    let (kind, stamp_ms, db, command, membership) = match &e.payload {
        RaftPayload::Blank => (fb::RaftEntryKind::BLANK, 0, 0, None, None),
        RaftPayload::Command {
            stamp_ms,
            db,
            command,
        } => (
            fb::RaftEntryKind::COMMAND,
            *stamp_ms,
            *db,
            Some(fbb.create_vector(command)),
            None,
        ),
        RaftPayload::Membership(m) => (
            fb::RaftEntryKind::MEMBERSHIP,
            0,
            0,
            None,
            Some(membership_to(fbb, m)),
        ),
    };
    fb::RaftEntry::create(
        fbb,
        &fb::RaftEntryArgs {
            log_id: Some(&log_to(e.log_id)),
            kind,
            stamp_ms,
            command,
            membership,
            db,
        },
    )
}

fn entry_of(e: fb::RaftEntry<'_>) -> Result<RaftEntry, DecodeError> {
    let log_id = log_of(
        e.log_id()
            .ok_or_else(|| invalid("Raft entry without a log id"))?,
    );
    let payload = match e.kind() {
        fb::RaftEntryKind::BLANK => RaftPayload::Blank,
        fb::RaftEntryKind::COMMAND => RaftPayload::Command {
            stamp_ms: e.stamp_ms(),
            db: e.db(),
            command: e
                .command()
                .map(|c| Bytes::copy_from_slice(c.bytes()))
                .ok_or_else(|| invalid("Raft command entry without a command"))?,
        },
        fb::RaftEntryKind::MEMBERSHIP => RaftPayload::Membership(membership_of(e.membership())?),
        other => return Err(invalid(format!("unknown Raft entry kind {}", other.0))),
    };
    Ok(RaftEntry { log_id, payload })
}

fn hint_to<'a>(fbb: &mut FlatBufferBuilder<'a>, h: &GroupHint) -> WIPOffset<fb::GroupHint<'a>> {
    let voters = ids_to(fbb, &h.voters);
    let learners = ids_to(fbb, &h.learners);
    let follower = h.follower.map(|f| id128(f.to_be_bytes()));
    fb::GroupHint::create(
        fbb,
        &fb::GroupHintArgs {
            group: h.group,
            mode: match h.mode {
                CounterMode::LeaderFollower => fb::CounterMode::LEADER_FOLLOWER,
                CounterMode::Raft => fb::CounterMode::RAFT,
            },
            regime: Some(&regime_to(h.regime)),
            leader: Some(&id128(h.leader.to_be_bytes())),
            term: h.term,
            follower: follower.as_ref(),
            voters: Some(voters),
            learners: Some(learners),
        },
    )
}

fn hint_of(h: fb::GroupHint<'_>) -> Result<GroupHint, DecodeError> {
    Ok(GroupHint {
        group: h.group(),
        mode: match h.mode() {
            fb::CounterMode::LEADER_FOLLOWER => CounterMode::LeaderFollower,
            fb::CounterMode::RAFT => CounterMode::Raft,
            other => return Err(invalid(format!("unknown counter mode {}", other.0))),
        },
        regime: regime_of(h.regime())?,
        leader: node_of(h.leader().ok_or_else(|| invalid("hint without a leader"))?),
        term: h.term(),
        follower: h.follower().map(node_of),
        voters: h.voters().into_iter().flatten().map(node_of).collect(),
        learners: h.learners().into_iter().flatten().map(node_of).collect(),
    })
}

fn hints_body(fbb: &mut FlatBufferBuilder<'_>, hints: &[GroupHint]) {
    let groups: Vec<_> = hints.iter().map(|h| hint_to(fbb, h)).collect();
    let groups = fbb.create_vector(&groups);
    let t = fb::LeaderHints::create(
        fbb,
        &fb::LeaderHintsArgs {
            groups: Some(groups),
        },
    );
    fbb.finish_minimal(t);
}

fn hints_of(
    groups: Option<Vector<'_, ForwardsUOffset<fb::GroupHint<'_>>>>,
) -> Result<Vec<GroupHint>, DecodeError> {
    groups.into_iter().flatten().map(hint_of).collect()
}

/// The gossip `leader_hints` bytes of a member.
pub fn encode_hints(hints: &[GroupHint]) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    hints_body(&mut fbb, hints);
    fbb.finished_data().to_vec()
}

/// Verify and decode gossip `leader_hints` bytes. Empty bytes mean no hints.
pub fn decode_hints(bytes: &[u8]) -> Result<Vec<GroupHint>, DecodeError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let opts = Limits::default().verifier();
    let t = flatbuffers::root_with_opts::<fb::LeaderHints>(&opts, bytes).map_err(invalid)?;
    hints_of(t.groups())
}

/// The entries of a counter snapshot: database, key, value, and deadline (0 for none).
pub type SnapshotEntries = Vec<(u32, Bytes, Bytes, u64)>;

/// Encode a counter state as a snapshot: the clock and every `(database, key, value, deadline)`,
/// with deadline 0 for no expiry.
pub fn encode_counter_snapshot<'a>(
    clock_ms: u64,
    entries: impl IntoIterator<Item = (u32, &'a [u8], &'a [u8], u64)>,
) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let items: Vec<_> = entries
        .into_iter()
        .map(|(db, k, v, t)| {
            let key = fbb.create_vector(k);
            let value = fbb.create_vector(v);
            fb::CounterEntry::create(
                &mut fbb,
                &fb::CounterEntryArgs {
                    key: Some(key),
                    value: Some(value),
                    expires_at_ms: t,
                    db,
                },
            )
        })
        .collect();
    let items = fbb.create_vector(&items);
    let t = fb::CounterSnapshot::create(
        &mut fbb,
        &fb::CounterSnapshotArgs {
            clock_ms,
            entries: Some(items),
        },
    );
    fbb.finish_minimal(t);
    fbb.finished_data().to_vec()
}

/// Verify and decode a counter snapshot.
pub fn decode_counter_snapshot(bytes: &[u8]) -> Result<(u64, SnapshotEntries), DecodeError> {
    // A snapshot holds one table per key, so the table limit of a frame does not fit it.
    let opts = flatbuffers::VerifierOptions {
        max_tables: usize::MAX,
        max_apparent_size: usize::MAX,
        ..Limits::default().verifier()
    };
    let t = flatbuffers::root_with_opts::<fb::CounterSnapshot>(&opts, bytes).map_err(invalid)?;
    let entries = t
        .entries()
        .into_iter()
        .flatten()
        .map(|e| {
            let key = e
                .key()
                .ok_or_else(|| invalid("counter entry without a key"))?;
            let value = e.value().map(|v| v.bytes()).unwrap_or_default();
            Ok((
                e.db(),
                Bytes::copy_from_slice(key.bytes()),
                Bytes::copy_from_slice(value),
                e.expires_at_ms(),
            ))
        })
        .collect::<Result<_, DecodeError>>()?;
    Ok((t.clock_ms(), entries))
}

/// Encode the body of a counter message. Returns false for any other message.
pub(crate) fn encode(fbb: &mut FlatBufferBuilder<'_>, message: &Message) -> bool {
    match message {
        Message::RaftLeader(hints) => hints_body(fbb, hints),
        Message::RaftConfig(c) => {
            let t = fb::RaftConfig::create(
                fbb,
                &fb::RaftConfigArgs {
                    group: c.group,
                    regime: Some(&regime_to(c.regime)),
                    replica: c.replica,
                    leader: Some(&id128(c.leader.to_be_bytes())),
                },
            );
            fbb.finish_minimal(t);
        }
        Message::RaftVote(v) => {
            let header = header_to(fbb, &v.header);
            let last = v.last.map(log_to);
            let t = fb::RaftVoteRequest::create(
                fbb,
                &fb::RaftVoteRequestArgs {
                    header: Some(header),
                    vote: Some(&vote_to(v.vote)),
                    last: last.as_ref(),
                },
            );
            fbb.finish_minimal(t);
        }
        Message::RaftAppend(a) => {
            let header = header_to(fbb, &a.header);
            let entries: Vec<_> = a.entries.iter().map(|e| entry_to(fbb, e)).collect();
            let entries = fbb.create_vector(&entries);
            let (prev, committed) = (a.prev.map(log_to), a.committed.map(log_to));
            let t = fb::RaftAppend::create(
                fbb,
                &fb::RaftAppendArgs {
                    header: Some(header),
                    vote: Some(&vote_to(a.vote)),
                    prev: prev.as_ref(),
                    entries: Some(entries),
                    committed: committed.as_ref(),
                },
            );
            fbb.finish_minimal(t);
        }
        Message::RaftSnapshot(s) => {
            let header = header_to(fbb, &s.header);
            let membership = membership_to(fbb, &s.membership);
            let snapshot_id = fbb.create_string(&s.snapshot_id);
            let data = fbb.create_vector(&s.data);
            let (last, membership_log) = (s.last.map(log_to), s.membership_log.map(log_to));
            let t = fb::RaftSnapshot::create(
                fbb,
                &fb::RaftSnapshotArgs {
                    header: Some(header),
                    vote: Some(&vote_to(s.vote)),
                    last: last.as_ref(),
                    membership_log: membership_log.as_ref(),
                    membership: Some(membership),
                    snapshot_id: Some(snapshot_id),
                    offset: s.offset,
                    data: Some(data),
                    done: s.done,
                },
            );
            fbb.finish_minimal(t);
        }
        Message::RaftReply(r) => {
            let last = r.last.map(log_to);
            let t = fb::RaftReply::create(
                fbb,
                &fb::RaftReplyArgs {
                    outcome: match r.outcome {
                        RaftOutcome::Ok => fb::RaftOutcome::OK,
                        RaftOutcome::Conflict => fb::RaftOutcome::CONFLICT,
                        RaftOutcome::HigherVote => fb::RaftOutcome::HIGHER_VOTE,
                        RaftOutcome::Partial => fb::RaftOutcome::PARTIAL,
                        RaftOutcome::Refused => fb::RaftOutcome::REFUSED,
                    },
                    vote: Some(&vote_to(r.vote)),
                    granted: r.granted,
                    last: last.as_ref(),
                },
            );
            fbb.finish_minimal(t);
        }
        Message::CounterSync(s) => {
            let ops: Vec<_> = s
                .ops
                .iter()
                .map(|op| {
                    let command = fbb.create_vector(&op.command);
                    fb::CounterOp::create(
                        fbb,
                        &fb::CounterOpArgs {
                            stamp_ms: op.stamp_ms,
                            command: Some(command),
                            db: op.db,
                        },
                    )
                })
                .collect();
            let ops = fbb.create_vector(&ops);
            let snapshot = s.snapshot.as_ref().map(|b| fbb.create_vector(b));
            let t = fb::CounterSync::create(
                fbb,
                &fb::CounterSyncArgs {
                    group: s.group,
                    regime: Some(&regime_to(s.regime)),
                    seq: s.seq,
                    ops: Some(ops),
                    snapshot,
                    has_snapshot: s.snapshot.is_some(),
                },
            );
            fbb.finish_minimal(t);
        }
        Message::CounterAck(a) => {
            let t = fb::CounterAck::create(
                fbb,
                &fb::CounterAckArgs {
                    group: a.group,
                    regime: Some(&regime_to(a.regime)),
                    seq: a.seq,
                    need_snapshot: a.need_snapshot,
                },
            );
            fbb.finish_minimal(t);
        }
        _ => return false,
    }
    true
}

/// Decode the payload of a counter message type. `None` for any other type.
pub(crate) fn decode_known(
    message_type: fb::MessageType,
    payload: &[u8],
    opts: &flatbuffers::VerifierOptions,
) -> Result<Option<Message>, DecodeError> {
    use fb::MessageType as T;
    Ok(Some(match message_type {
        T::RAFT_LEADER => {
            let t = root::<fb::LeaderHints>(opts, payload)?;
            Message::RaftLeader(hints_of(t.groups())?)
        }
        T::RAFT_CONFIG => {
            let c = root::<fb::RaftConfig>(opts, payload)?;
            Message::RaftConfig(RaftConfig {
                group: c.group(),
                regime: regime_of(c.regime())?,
                replica: c.replica(),
                leader: node_of(
                    c.leader()
                        .ok_or_else(|| invalid("RAFT_CONFIG without a leader"))?,
                ),
            })
        }
        T::RAFT_VOTE => {
            let v = root::<fb::RaftVoteRequest>(opts, payload)?;
            Message::RaftVote(RaftVoteReq {
                header: header_of(v.header())?,
                vote: vote_of(v.vote())?,
                last: v.last().map(log_of),
            })
        }
        T::RAFT_APPEND => {
            let a = root::<fb::RaftAppend>(opts, payload)?;
            Message::RaftAppend(RaftAppend {
                header: header_of(a.header())?,
                vote: vote_of(a.vote())?,
                prev: a.prev().map(log_of),
                entries: a
                    .entries()
                    .into_iter()
                    .flatten()
                    .map(entry_of)
                    .collect::<Result<_, _>>()?,
                committed: a.committed().map(log_of),
            })
        }
        T::RAFT_SNAPSHOT => {
            let s = root::<fb::RaftSnapshot>(opts, payload)?;
            Message::RaftSnapshot(RaftSnapshot {
                header: header_of(s.header())?,
                vote: vote_of(s.vote())?,
                last: s.last().map(log_of),
                membership_log: s.membership_log().map(log_of),
                membership: membership_of(s.membership())?,
                snapshot_id: s.snapshot_id().unwrap_or_default().to_owned(),
                offset: s.offset(),
                data: bytes_of(s.data()),
                done: s.done(),
            })
        }
        T::RAFT_REPLY => {
            let r = root::<fb::RaftReply>(opts, payload)?;
            Message::RaftReply(RaftReply {
                outcome: match r.outcome() {
                    fb::RaftOutcome::OK => RaftOutcome::Ok,
                    fb::RaftOutcome::CONFLICT => RaftOutcome::Conflict,
                    fb::RaftOutcome::HIGHER_VOTE => RaftOutcome::HigherVote,
                    fb::RaftOutcome::PARTIAL => RaftOutcome::Partial,
                    // An outcome from a newer build is a refusal to this build.
                    _ => RaftOutcome::Refused,
                },
                vote: vote_of(r.vote())?,
                granted: r.granted(),
                last: r.last().map(log_of),
            })
        }
        T::COUNTER_SYNC => {
            let s = root::<fb::CounterSync>(opts, payload)?;
            Message::CounterSync(CounterSync {
                group: s.group(),
                regime: regime_of(s.regime())?,
                seq: s.seq(),
                ops: s
                    .ops()
                    .into_iter()
                    .flatten()
                    .map(|op| {
                        let command = op
                            .command()
                            .ok_or_else(|| invalid("counter op without a command"))?;
                        Ok(CounterOp {
                            stamp_ms: op.stamp_ms(),
                            db: op.db(),
                            command: Bytes::copy_from_slice(command.bytes()),
                        })
                    })
                    .collect::<Result<_, DecodeError>>()?,
                snapshot: s.has_snapshot().then(|| bytes_of(s.snapshot())),
            })
        }
        T::COUNTER_ACK => {
            let a = root::<fb::CounterAck>(opts, payload)?;
            Message::CounterAck(CounterAck {
                group: a.group(),
                regime: regime_of(a.regime())?,
                seq: a.seq(),
                need_snapshot: a.need_snapshot(),
            })
        }
        _ => return Ok(None),
    }))
}
