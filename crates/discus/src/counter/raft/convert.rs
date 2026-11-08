//! Conversion between the openraft types and the wire messages of `counter.fbs`.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use discus_resp::{Command, Limits as RespLimits, Parser};
use discus_wire::{
    encode_command, RaftAppend, RaftEntry, RaftHeader, RaftLogId, RaftMembership, RaftOutcome,
    RaftPayload, RaftReply, RaftSnapshot, RaftVote, RaftVoteReq,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LeaderId, LogId, Membership, SnapshotMeta,
    StoredMembership, Vote,
};

use super::{Op, Peer, TypeConfig};

pub fn log_to(l: &LogId<u64>) -> RaftLogId {
    RaftLogId {
        term: l.leader_id.term,
        leader: l.leader_id.node_id,
        index: l.index,
    }
}

pub fn log_of(l: RaftLogId) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(l.term, l.leader), l.index)
}

pub fn vote_to(v: &Vote<u64>) -> RaftVote {
    RaftVote {
        term: v.leader_id.term,
        leader: v.leader_id.node_id,
        committed: v.committed,
    }
}

pub fn vote_of(v: RaftVote) -> Vote<u64> {
    Vote {
        leader_id: LeaderId::new(v.term, v.leader),
        committed: v.committed,
    }
}

pub fn membership_to(m: &Membership<u64, Peer>) -> RaftMembership {
    RaftMembership {
        configs: m
            .get_joint_config()
            .iter()
            .map(|c| c.iter().copied().collect())
            .collect(),
        nodes: m.nodes().map(|(id, p)| (*id, p.node)).collect(),
    }
}

pub fn membership_of(m: RaftMembership) -> Membership<u64, Peer> {
    let configs: Vec<BTreeSet<u64>> = m
        .configs
        .into_iter()
        .map(|c| c.into_iter().collect())
        .collect();
    let nodes: BTreeMap<u64, Peer> = m
        .nodes
        .into_iter()
        .map(|(id, node)| (id, Peer { node }))
        .collect();
    Membership::new(configs, nodes)
}

/// Parse a raw RESP command of a log entry or a sync op.
pub fn parse_command(raw: &[u8]) -> Option<Command> {
    let mut parser = Parser::new(RespLimits {
        max_bulk_bytes: usize::MAX / 2,
        ..RespLimits::default()
    });
    parser.feed(raw);
    parser.next().ok().flatten()
}

fn entry_to(e: &Entry<TypeConfig>) -> RaftEntry {
    RaftEntry {
        log_id: log_to(&e.log_id),
        payload: match &e.payload {
            EntryPayload::Blank => RaftPayload::Blank,
            EntryPayload::Normal(op) => RaftPayload::Command {
                stamp_ms: op.stamp_ms,
                command: encode_command(&op.command),
            },
            EntryPayload::Membership(m) => RaftPayload::Membership(membership_to(m)),
        },
    }
}

fn entry_of(e: RaftEntry) -> Option<Entry<TypeConfig>> {
    Some(Entry {
        log_id: log_of(e.log_id),
        payload: match e.payload {
            RaftPayload::Blank => EntryPayload::Blank,
            RaftPayload::Command { stamp_ms, command } => EntryPayload::Normal(Op {
                stamp_ms,
                command: parse_command(&command)?,
            }),
            RaftPayload::Membership(m) => EntryPayload::Membership(membership_of(m)),
        },
    })
}

pub fn append_to(header: RaftHeader, r: &AppendEntriesRequest<TypeConfig>) -> RaftAppend {
    RaftAppend {
        header,
        vote: vote_to(&r.vote),
        prev: r.prev_log_id.as_ref().map(log_to),
        entries: r.entries.iter().map(entry_to).collect(),
        committed: r.leader_commit.as_ref().map(log_to),
    }
}

/// `None` when an entry carries a command that does not parse.
pub fn append_of(a: RaftAppend) -> Option<AppendEntriesRequest<TypeConfig>> {
    Some(AppendEntriesRequest {
        vote: vote_of(a.vote),
        prev_log_id: a.prev.map(log_of),
        entries: a.entries.into_iter().map(entry_of).collect::<Option<_>>()?,
        leader_commit: a.committed.map(log_of),
    })
}

pub fn append_reply_to(r: &AppendEntriesResponse<u64>) -> RaftReply {
    let (outcome, vote, last) = match r {
        AppendEntriesResponse::Success => (RaftOutcome::Ok, RaftVote::default(), None),
        AppendEntriesResponse::PartialSuccess(l) => (
            RaftOutcome::Partial,
            RaftVote::default(),
            l.as_ref().map(log_to),
        ),
        AppendEntriesResponse::Conflict => (RaftOutcome::Conflict, RaftVote::default(), None),
        AppendEntriesResponse::HigherVote(v) => (RaftOutcome::HigherVote, vote_to(v), None),
    };
    RaftReply {
        outcome,
        vote,
        granted: false,
        last,
    }
}

/// `None` for a refusal.
pub fn append_reply_of(r: RaftReply) -> Option<AppendEntriesResponse<u64>> {
    Some(match r.outcome {
        RaftOutcome::Ok => AppendEntriesResponse::Success,
        RaftOutcome::Partial => AppendEntriesResponse::PartialSuccess(r.last.map(log_of)),
        RaftOutcome::Conflict => AppendEntriesResponse::Conflict,
        RaftOutcome::HigherVote => AppendEntriesResponse::HigherVote(vote_of(r.vote)),
        RaftOutcome::Refused => return None,
    })
}

pub fn vote_req_to(header: RaftHeader, r: &VoteRequest<u64>) -> RaftVoteReq {
    RaftVoteReq {
        header,
        vote: vote_to(&r.vote),
        last: r.last_log_id.as_ref().map(log_to),
    }
}

pub fn vote_req_of(v: RaftVoteReq) -> VoteRequest<u64> {
    VoteRequest::new(vote_of(v.vote), v.last.map(log_of))
}

pub fn vote_reply_to(r: &VoteResponse<u64>) -> RaftReply {
    RaftReply {
        outcome: RaftOutcome::Ok,
        vote: vote_to(&r.vote),
        granted: r.vote_granted,
        last: r.last_log_id.as_ref().map(log_to),
    }
}

pub fn vote_reply_of(r: RaftReply) -> Option<VoteResponse<u64>> {
    if r.outcome == RaftOutcome::Refused {
        return None;
    }
    Some(VoteResponse {
        vote: vote_of(r.vote),
        vote_granted: r.granted,
        last_log_id: r.last.map(log_of),
    })
}

pub fn snapshot_to(header: RaftHeader, r: &InstallSnapshotRequest<TypeConfig>) -> RaftSnapshot {
    RaftSnapshot {
        header,
        vote: vote_to(&r.vote),
        last: r.meta.last_log_id.as_ref().map(log_to),
        membership_log: r.meta.last_membership.log_id().as_ref().map(log_to),
        membership: membership_to(r.meta.last_membership.membership()),
        snapshot_id: r.meta.snapshot_id.clone(),
        offset: r.offset,
        data: Bytes::copy_from_slice(&r.data),
        done: r.done,
    }
}

pub fn snapshot_of(s: RaftSnapshot) -> InstallSnapshotRequest<TypeConfig> {
    InstallSnapshotRequest {
        vote: vote_of(s.vote),
        meta: SnapshotMeta {
            last_log_id: s.last.map(log_of),
            last_membership: StoredMembership::new(
                s.membership_log.map(log_of),
                membership_of(s.membership),
            ),
            snapshot_id: s.snapshot_id,
        },
        offset: s.offset,
        data: s.data.to_vec(),
        done: s.done,
    }
}

pub fn snapshot_reply_to(r: &InstallSnapshotResponse<u64>) -> RaftReply {
    RaftReply {
        outcome: RaftOutcome::Ok,
        vote: vote_to(&r.vote),
        granted: false,
        last: None,
    }
}

pub fn snapshot_reply_of(r: RaftReply) -> Option<InstallSnapshotResponse<u64>> {
    if r.outcome == RaftOutcome::Refused {
        return None;
    }
    Some(InstallSnapshotResponse {
        vote: vote_of(r.vote),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counter::cmd;
    use discus_placement::NodeId;
    use discus_wire::Regime;

    fn header() -> RaftHeader {
        RaftHeader {
            group: 3,
            regime: Regime {
                generation: 4,
                origin: NodeId::from_u128(9),
            },
            target: 77,
        }
    }

    fn membership() -> Membership<u64, Peer> {
        let nodes: BTreeMap<u64, Peer> = [(1, 11u128), (2, 22), (5, 55)]
            .into_iter()
            .map(|(r, n)| {
                (
                    r,
                    Peer {
                        node: NodeId::from_u128(n),
                    },
                )
            })
            .collect();
        Membership::new(
            vec![BTreeSet::from([1, 2]), BTreeSet::from([1, 2, 5])],
            nodes,
        )
    }

    #[test]
    fn conversion_round_trips() {
        let lid = LogId::new(CommittedLeaderId::new(3, 2), 17);
        let append = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new_committed(3, 2),
            prev_log_id: Some(lid),
            entries: vec![
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(3, 2), 18),
                    payload: EntryPayload::Blank,
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(3, 2), 19),
                    payload: EntryPayload::Normal(Op {
                        stamp_ms: 1234,
                        command: cmd(&["INCR", "cnt:a", "EX", "5"]),
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(3, 2), 20),
                    payload: EntryPayload::Membership(membership()),
                },
            ],
            leader_commit: None,
        };
        let wire = append_to(header(), &append);
        assert_eq!(wire.header, header());
        let back = append_of(wire).expect("the commands parse");
        assert_eq!(back.vote, append.vote);
        assert_eq!(back.prev_log_id, append.prev_log_id);
        assert_eq!(back.leader_commit, None);
        assert_eq!(back.entries, append.entries);

        for r in [
            AppendEntriesResponse::Success,
            AppendEntriesResponse::PartialSuccess(Some(lid)),
            AppendEntriesResponse::PartialSuccess(None),
            AppendEntriesResponse::Conflict,
            AppendEntriesResponse::HigherVote(Vote::new(9, 1)),
        ] {
            assert_eq!(append_reply_of(append_reply_to(&r)), Some(r));
        }

        let vote = VoteRequest::new(Vote::new(5, 7), Some(lid));
        let back = vote_req_of(vote_req_to(header(), &vote));
        assert_eq!(back, vote);
        let reply = VoteResponse {
            vote: Vote::new(5, 7),
            vote_granted: true,
            last_log_id: Some(lid),
        };
        assert_eq!(vote_reply_of(vote_reply_to(&reply)), Some(reply));

        let snap = InstallSnapshotRequest::<TypeConfig> {
            vote: Vote::new_committed(6, 1),
            meta: SnapshotMeta {
                last_log_id: Some(lid),
                last_membership: StoredMembership::new(Some(lid), membership()),
                snapshot_id: "17-1".into(),
            },
            offset: 4096,
            data: vec![1, 2, 3],
            done: true,
        };
        let back = snapshot_of(snapshot_to(header(), &snap));
        assert_eq!(back, snap);
        let reply = InstallSnapshotResponse {
            vote: Vote::new(6, 1),
        };
        assert_eq!(snapshot_reply_of(snapshot_reply_to(&reply)), Some(reply));
        assert_eq!(append_reply_of(RaftReply::refused()), None);
    }
}
