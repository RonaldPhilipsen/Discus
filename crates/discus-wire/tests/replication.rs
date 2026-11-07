#![allow(clippy::unwrap_used)]

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use discus_placement::{EpochId, NodeId, Version, NTP64};
use discus_resp::Value as Resp;
use discus_store::{Entry, Value, ZSet};
use discus_wire::{
    decode_frame, encode_frame, fb, Batch, BatchAck, DecodeError, Forward, ForwardMode,
    ForwardReply, Frame, Limits, Message, TraceContext,
};

fn epoch(g: u64) -> EpochId {
    EpochId {
        generation: g,
        origin: NodeId::from_u128(3),
    }
}

fn v(t: u64) -> Version {
    Version::new(NTP64(t), NodeId::from_u128(5))
}

fn round_trip(message: Message) -> Message {
    let frame = Frame {
        request_id: 9,
        trace: TraceContext::default(),
        message,
    };
    decode_frame(&encode_frame(&frame), &Limits::default())
        .unwrap()
        .message
}

fn entry(value: Value, version: Version, expires: Option<u64>) -> Entry {
    let mut e = Entry::new(value, expires);
    e.version = version;
    e
}

fn batch() -> Batch {
    let mut hash = HashMap::new();
    hash.insert(Bytes::from("f"), Bytes::from("x"));
    hash.insert(Bytes::from("g"), Bytes::from(""));
    let set: HashSet<Bytes> = ["a", "b"].into_iter().map(Bytes::from).collect();
    let mut zset = ZSet::new();
    zset.insert(Bytes::from("m"), 1.5);
    zset.insert(Bytes::from("n"), -2.0);
    let mut tomb = entry(Value::Str(Bytes::new()), v(5), Some(99));
    tomb.tombstone = true;
    let mut b = Batch::new(epoch(4));
    b.entries = vec![
        (
            Bytes::from("s"),
            entry(Value::Str("val".into()), v(1), Some(1234)),
        ),
        (Bytes::from("h"), entry(Value::Hash(hash), v(2), None)),
        (Bytes::from("set"), entry(Value::Set(set), v(3), None)),
        (Bytes::from("z"), entry(Value::ZSet(zset), v(4), None)),
        (Bytes::from("t"), tomb),
    ];
    b.offers = vec![(Bytes::from("o"), v(7))];
    b.discard = vec![3, 250];
    b
}

#[test]
fn every_value_kind_round_trips() {
    let b = batch();
    for m in [
        Message::Replicate(b.clone()),
        Message::HintReplay(b.clone()),
        Message::Handoff(b.clone()),
        Message::Entries(b.clone()),
    ] {
        assert_eq!(round_trip(m.clone()), m);
    }
}

#[test]
fn a_done_batch_carries_its_range() {
    let mut b = Batch::new(epoch(6));
    b.done = Some((epoch(2), epoch(6)));
    let m = Message::Handoff(b);
    assert_eq!(round_trip(m.clone()), m);
}

#[test]
fn a_batch_ack_round_trips() {
    let m = Message::BatchAck(BatchAck {
        applied: 12,
        want: vec![Bytes::from("k1"), Bytes::from("k2")],
        epoch: epoch(3),
    });
    assert_eq!(round_trip(m.clone()), m);
}

#[test]
fn fetch_and_digest_round_trip() {
    let m = Message::Fetch {
        keys: vec![Bytes::from("a"), Bytes::from("b")],
    };
    assert_eq!(round_trip(m.clone()), m);
    let m = Message::Digest {
        from: epoch(1),
        to: epoch(2),
        sums: (0..256).collect(),
    };
    assert_eq!(round_trip(m.clone()), m);
}

#[test]
fn new_types_are_9_to_15() {
    let b = Batch::new(epoch(1));
    let codes = [
        Message::Replicate(b.clone()).type_code(),
        Message::HintReplay(b.clone()).type_code(),
        Message::Handoff(b.clone()).type_code(),
        Message::BatchAck(BatchAck {
            applied: 0,
            want: vec![],
            epoch: epoch(1),
        })
        .type_code(),
        Message::Fetch { keys: vec![] }.type_code(),
        Message::Entries(b).type_code(),
        Message::Digest {
            from: epoch(1),
            to: epoch(1),
            sums: vec![],
        }
        .type_code(),
    ];
    assert_eq!(codes, [9, 10, 11, 12, 13, 14, 15]);
    assert_eq!(discus_wire::caps::REPLICATION, 4);
    assert_ne!(discus_wire::caps::KNOWN & discus_wire::caps::REPLICATION, 0);
}

#[test]
fn a_forward_reply_keeps_observed_and_settled() {
    let m = Message::ForwardReply(ForwardReply {
        value: Resp::bulk(b"x".to_vec()),
        applied: None,
        epoch: epoch(2),
        observed: Some(v(8)),
        settled: true,
    });
    assert_eq!(round_trip(m.clone()), m);
    let plain = Message::ForwardReply(ForwardReply {
        value: Resp::Null,
        applied: Some(v(1)),
        epoch: epoch(2),
        observed: None,
        settled: false,
    });
    assert_eq!(round_trip(plain.clone()), plain);
}

#[test]
fn a_tracked_forward_round_trips() {
    let m = Message::Forward(Forward {
        command: Bytes::from_static(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
        epoch: epoch(2),
        epoch_body: None,
        hops: 1,
        path: vec![NodeId::from_u128(1)],
        mode: ForwardMode::Route,
        track: true,
    });
    assert_eq!(round_trip(m.clone()), m);
}

#[test]
fn an_unknown_value_kind_is_invalid() {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let key = fbb.create_vector(b"k");
    let e = fb::StoredEntry::create(
        &mut fbb,
        &fb::StoredEntryArgs {
            key: Some(key),
            kind: fb::ValueKind(200),
            ..Default::default()
        },
    );
    let entries = fbb.create_vector(&[e]);
    let b = fb::EntryBatch::create(
        &mut fbb,
        &fb::EntryBatchArgs {
            entries: Some(entries),
            ..Default::default()
        },
    );
    fbb.finish_minimal(b);
    let env = discus_wire::encode_envelope(9, 1, &TraceContext::default(), fbb.finished_data());
    assert!(matches!(
        decode_frame(&env, &Limits::default()),
        Err(DecodeError::Invalid(_))
    ));
}
