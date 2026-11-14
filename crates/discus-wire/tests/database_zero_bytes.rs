//! A message in database 0 has the same bytes as before the `db` fields existed. Each constant is
//! the encoding of the message below, captured from the build before those fields (commit
//! ac52ee1).

#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;

use bytes::Bytes;
use discus_placement::{EpochId, MemberState, NodeId, Version, Weight, NTP64};
use discus_store::{DbKey, Entry, Value};
use discus_wire::{
    decode_counter_snapshot, decode_frame, encode_counter_snapshot, encode_frame, Batch, BatchAck,
    Capacity, CounterOp, CounterSync, Forward, ForwardMode, Frame, Gossip, GossipKind, Limits,
    MemberInfo, Message, RaftAppend, RaftEntry, RaftHeader, RaftLogId, RaftPayload, RaftVote,
    Regime, TraceContext,
};

const FORWARD: &str = concat!(
    "1400000000000e00180006000c000000000008000e0000000000040010000000010000000000000000000000",
    "700000001c0000000000000000001200280008000c0000000600240000000700120000000000010134000000",
    "0200000000000000000000000000000000000000000000030400000001000000000000000000000000000000",
    "00000001140000002a320d0a24330d0a4745540d0a24310d0a6b0d0a",
);

const BATCH: &str = concat!(
    "1400000000000e00180006000c000000000008000e0000000000090010000000010000000000000000000000",
    "d000000014000000000000000c00280004001c00200024000c00000004000000000000000000000000000000",
    "0000000000000003480000000c0000000400000000000000010000000c000000080020000400080008000000",
    "1c000000070000000000000005000000000000000000000000000000010000006f0000000100000018000000",
    "000012002c000400000008000000000024000c00120000003000000024000000010000000000000005000000",
    "000000000000000000000000d2040000000000000300000076616c000100000073000000",
);

const ACK: &str = concat!(
    "1400000000000e00180006000c000000000008000e00000000000c0010000000010000000000000000000000",
    "58000000140000000000000000000a002400040008000c000a000000020000001c0000000300000000000000",
    "00000000000000000000000000000003010000000c0000000000060008000400060000000400000001000000",
    "6b000000",
);

const FETCH: &str = concat!(
    "1400000000000e00140006000c000000000008000e00000000000d000c00000001000000000000003c000000",
    "04000000deffffff04000000020000001c00000004000000f2ffffff04000000010000006200060008000400",
    "06000000040000000100000061000000",
);

const DIGEST: &str = concat!(
    "1400000000000e00180006000c000000000008000e00000000000f0010000000010000000000000000000000",
    "60000000140000000000000000000a00380004001c0034000a00000001000000000000000000000000000000",
    "0000000000000003020000000000000000000000000000000000000000000003040000000200000001000000",
    "000000000200000000000000",
);

const GOSSIP: &str = concat!(
    "1400000000000e00180006000c000000000008000e0000000000080010000000010000000000000000000000",
    "e00000002000000000000000000016001c000000040008000000000000000000000018001600000038000000",
    "00000000000000000000000000000000040000000000000000001a005c000800180007003c0000001c004400",
    "4c005400000038001a0000000000000100000000000000000000000000000001580000000400000000000000",
    "00000000000000000000000000000003000000002400000004000000000000007f0000000000000001000000",
    "0000000002000000000000000e0000003132372e302e302e313a3730303200000e0000003132372e302e302e",
    "313a373030310000",
);

const SYNC: &str = concat!(
    "1400000000000e00180006000c000000000008000e0000000000160010000000010000000000000000000000",
    "68000000100000000c00300006000800280024000c0000000000030007000000000000000000000000000000",
    "0000000000000003000000000c0000000c00000000000000010000000c000000080010000800040008000000",
    "0c0000000500000000000000020000006f700000",
);

const APPEND: &str = concat!(
    "1400000000000e00180006000c000000000008000e0000000000130010000000010000000000000000000000",
    "b8000000100000000c00240004000800000020000c0000007c00000002000000000000000900000000000000",
    "01000000000000000400000001000000100000000c00340008000700280024000c0000000000000102000000",
    "0000000009000000000000000300000000000000000000001000000011000000000000000000000003000000",
    "636d640000000a0028000600080020000a000000000005000700000000000000000000000000000000000000",
    "000000030900000000000000",
);

const SNAPSHOT: &str = concat!(
    "1000000000000000080014000800040008000000100000004d0000000000000000000000010000000c000000",
    "08000c0004000800080000001000000004000000010000003100000003000000636e7400",
);

fn epoch(g: u64) -> EpochId {
    EpochId {
        generation: g,
        origin: NodeId::from_u128(3),
    }
}

fn v(t: u64) -> Version {
    Version::new(NTP64(t), NodeId::from_u128(5))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn regime() -> Regime {
    Regime {
        generation: 7,
        origin: NodeId::from_u128(3),
    }
}

fn messages() -> Vec<(&'static str, Message)> {
    let mut entry = Entry::new(Value::Str(Bytes::from("val")), Some(1234));
    entry.version = v(1);
    let mut batch = Batch::new(epoch(4));
    batch.entries = vec![(DbKey::new(0, "s"), entry)];
    batch.offers = vec![(DbKey::new(0, "o"), v(7))];
    let info = MemberInfo {
        id: NodeId::from_u128(1),
        addr: SocketAddr::from(([127, 0, 0, 1], 7001)),
        rpc_addr: SocketAddr::from(([127, 0, 0, 1], 7002)),
        client_addr: None,
        client_host: None,
        state: MemberState::Normal,
        incarnation: 4,
        weight: Weight::new(1.0).unwrap(),
        epoch: epoch(4),
        capabilities: 127,
        capacity: Capacity { used: 1, max: 2 },
        leader_hints: Bytes::new(),
        databases: 1,
    };
    vec![
        (
            FORWARD,
            Message::Forward(Forward {
                command: Bytes::from_static(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
                epoch: epoch(2),
                epoch_body: None,
                hops: 1,
                path: vec![NodeId::from_u128(1)],
                mode: ForwardMode::Route,
                track: true,
                db: 0,
            }),
        ),
        (BATCH, Message::Replicate(batch)),
        (
            ACK,
            Message::BatchAck(BatchAck {
                applied: 2,
                want: vec![DbKey::new(0, "k")],
                epoch: epoch(3),
                idle: false,
            }),
        ),
        (
            FETCH,
            Message::Fetch {
                keys: vec![DbKey::new(0, "a"), DbKey::new(0, "b")],
            },
        ),
        (
            DIGEST,
            Message::Digest {
                from: epoch(1),
                to: epoch(2),
                db: 0,
                sums: vec![1, 2],
            },
        ),
        (GOSSIP, Message::Gossip(Gossip::new(GossipKind::Ping, info))),
        (
            SYNC,
            Message::CounterSync(CounterSync {
                group: 3,
                regime: regime(),
                seq: 12,
                ops: vec![CounterOp {
                    stamp_ms: 5,
                    db: 0,
                    command: Bytes::from_static(b"op"),
                }],
                snapshot: None,
            }),
        ),
        (
            APPEND,
            Message::RaftAppend(RaftAppend {
                header: RaftHeader {
                    group: 5,
                    regime: regime(),
                    target: 9,
                },
                vote: RaftVote {
                    term: 2,
                    leader: 9,
                    committed: true,
                },
                prev: None,
                entries: vec![RaftEntry {
                    log_id: RaftLogId {
                        term: 2,
                        leader: 9,
                        index: 3,
                    },
                    payload: RaftPayload::Command {
                        stamp_ms: 17,
                        db: 0,
                        command: Bytes::from_static(b"cmd"),
                    },
                }],
                committed: None,
            }),
        ),
    ]
}

#[test]
fn database_zero_messages_keep_their_bytes() {
    for (golden, message) in messages() {
        let frame = Frame {
            request_id: 1,
            trace: TraceContext::default(),
            message,
        };
        assert_eq!(hex(&encode_frame(&frame)), golden, "{:?}", frame.message);
        assert_eq!(
            decode_frame(&unhex(golden), &Limits::default()).unwrap(),
            frame
        );
    }
}

#[test]
fn a_database_zero_snapshot_keeps_its_bytes() {
    let bytes = encode_counter_snapshot(77, [(0, &b"cnt"[..], &b"1"[..], 0)]);
    assert_eq!(hex(&bytes), SNAPSHOT);
    assert_eq!(
        decode_counter_snapshot(&unhex(SNAPSHOT)).unwrap(),
        (77, vec![(0, Bytes::from("cnt"), Bytes::from("1"), 0)])
    );
}
