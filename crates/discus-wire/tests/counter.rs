#![allow(clippy::unwrap_used)]

mod common;

use bytes::Bytes;
use common::round_trip;
use discus_placement::NodeId;
use discus_wire::{
    caps, decode_counter_snapshot, decode_frame, decode_hints, encode_counter_snapshot,
    encode_envelope, encode_hints, fb, CounterAck, CounterMode, CounterSync, DecodeError,
    GroupHint, Limits, Message, RaftAppend, RaftConfig, RaftEntry, RaftHeader, RaftLogId,
    RaftMembership, RaftOutcome, RaftPayload, RaftReply, RaftSnapshot, RaftVote, RaftVoteReq,
    Regime, TraceContext,
};

fn n(x: u128) -> NodeId {
    NodeId::from_u128(x)
}

fn regime() -> Regime {
    Regime {
        generation: 7,
        origin: n(3),
    }
}

fn header() -> RaftHeader {
    RaftHeader {
        group: 5,
        regime: regime(),
        target: 991,
    }
}

fn log(i: u64) -> RaftLogId {
    RaftLogId {
        term: 2,
        leader: 991,
        index: i,
    }
}

fn vote() -> RaftVote {
    RaftVote {
        term: 2,
        leader: 991,
        committed: true,
    }
}

fn membership() -> RaftMembership {
    RaftMembership {
        configs: vec![vec![1, 2], vec![1, 2, 3]],
        nodes: vec![(1, n(10)), (2, n(20)), (3, n(30))],
    }
}

fn hint(follower: Option<NodeId>) -> GroupHint {
    GroupHint {
        group: 2,
        mode: CounterMode::Raft,
        regime: regime(),
        leader: n(10),
        term: 4,
        follower,
        voters: vec![n(10), n(20), n(30)],
        learners: vec![n(40)],
    }
}

#[test]
fn every_counter_message_round_trips() {
    let messages = vec![
        Message::RaftLeader(vec![]),
        Message::RaftLeader(vec![hint(Some(n(20))), hint(None)]),
        Message::RaftConfig(RaftConfig {
            group: 1,
            regime: regime(),
            replica: 42,
            leader: n(10),
        }),
        Message::RaftVote(RaftVoteReq {
            header: header(),
            vote: vote(),
            last: Some(log(9)),
        }),
        Message::RaftVote(RaftVoteReq {
            header: header(),
            vote: vote(),
            last: None,
        }),
        Message::RaftAppend(RaftAppend {
            header: header(),
            vote: vote(),
            prev: Some(log(1)),
            entries: vec![
                RaftEntry {
                    log_id: log(2),
                    payload: RaftPayload::Blank,
                },
                RaftEntry {
                    log_id: log(3),
                    payload: RaftPayload::Command {
                        stamp_ms: 1_700_000_000_000,
                        command: Bytes::from_static(b"*2\r\n$4\r\nINCR\r\n$5\r\ncnt:a\r\n"),
                    },
                },
                RaftEntry {
                    log_id: log(4),
                    payload: RaftPayload::Membership(membership()),
                },
            ],
            committed: Some(log(3)),
        }),
        Message::RaftAppend(RaftAppend {
            header: header(),
            vote: vote(),
            prev: None,
            entries: vec![],
            committed: None,
        }),
        Message::RaftSnapshot(RaftSnapshot {
            header: header(),
            vote: vote(),
            last: Some(log(8)),
            membership_log: Some(log(4)),
            membership: membership(),
            snapshot_id: "8-1".into(),
            offset: 1 << 20,
            data: Bytes::from_static(b"chunk"),
            done: true,
        }),
        Message::RaftReply(RaftReply {
            outcome: RaftOutcome::Partial,
            vote: vote(),
            granted: true,
            last: Some(log(2)),
        }),
        Message::RaftReply(RaftReply::refused()),
        Message::CounterSync(CounterSync {
            group: 3,
            regime: regime(),
            seq: 12,
            ops: vec![
                (5, Bytes::from_static(b"op1")),
                (6, Bytes::from_static(b"op2")),
            ],
            snapshot: None,
        }),
        Message::CounterSync(CounterSync {
            group: 3,
            regime: regime(),
            seq: 0,
            ops: vec![],
            snapshot: Some(Bytes::new()),
        }),
        Message::CounterAck(CounterAck {
            group: 3,
            regime: regime(),
            seq: 13,
            need_snapshot: true,
        }),
    ];
    for m in messages {
        assert_eq!(round_trip(m.clone()), m);
    }
}

#[test]
fn hints_round_trip_and_reject_garbage() {
    let hints = vec![hint(None), hint(Some(n(30)))];
    assert_eq!(decode_hints(&encode_hints(&hints)).unwrap(), hints);
    assert_eq!(decode_hints(&[]).unwrap(), vec![]);
    assert!(decode_hints(b"not a flatbuffer").is_err());
}

#[test]
fn a_snapshot_round_trips() {
    let entries: Vec<(Bytes, Bytes, u64)> = (0..1000u64)
        .map(|i| {
            (
                Bytes::from(format!("cnt:{i}")),
                Bytes::from(i.to_string()),
                if i % 3 == 0 { 0 } else { 5000 + i },
            )
        })
        .collect();
    let bytes = encode_counter_snapshot(77, entries.iter().map(|(k, v, t)| (&k[..], &v[..], *t)));
    assert_eq!(decode_counter_snapshot(&bytes).unwrap(), (77, entries));
    assert!(decode_counter_snapshot(b"garbage").is_err());
}

#[test]
fn new_types_are_16_to_23() {
    use fb::MessageType as T;
    let types = [
        T::RAFT_LEADER,
        T::RAFT_CONFIG,
        T::RAFT_VOTE,
        T::RAFT_APPEND,
        T::RAFT_SNAPSHOT,
        T::RAFT_REPLY,
        T::COUNTER_SYNC,
        T::COUNTER_ACK,
    ];
    let codes: Vec<u16> = types.iter().map(|t| t.0).collect();
    assert_eq!(codes, (16..=23).collect::<Vec<u16>>());
}

#[test]
fn counters_is_bit_8() {
    assert_eq!(caps::COUNTERS, 8);
    assert_ne!(caps::KNOWN & caps::COUNTERS, 0);
}

#[test]
fn a_raft_entry_without_a_payload_is_invalid() {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let entry = fb::RaftEntry::create(
        &mut fbb,
        &fb::RaftEntryArgs {
            log_id: Some(&fb::RaftLogId::new(1, 1, 1)),
            kind: fb::RaftEntryKind::COMMAND,
            ..Default::default()
        },
    );
    let entries = fbb.create_vector(&[entry]);
    let header = fb::RaftHeader::create(
        &mut fbb,
        &fb::RaftHeaderArgs {
            group: 0,
            regime: Some(&fb::Regime::new(1, &fb::Id128::new(&[0; 16]))),
            target: 1,
        },
    );
    let t = fb::RaftAppend::create(
        &mut fbb,
        &fb::RaftAppendArgs {
            header: Some(header),
            vote: Some(&fb::RaftVote::new(1, 1, false)),
            entries: Some(entries),
            ..Default::default()
        },
    );
    fbb.finish_minimal(t);
    let frame = encode_envelope(
        fb::MessageType::RAFT_APPEND.0,
        1,
        &TraceContext::default(),
        fbb.finished_data(),
    );
    assert!(matches!(
        decode_frame(&frame, &Limits::default()),
        Err(DecodeError::Invalid(_))
    ));
}
