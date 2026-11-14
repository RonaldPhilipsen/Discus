#![allow(clippy::unwrap_used)]

mod common;

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use common::round_trip;
use discus_placement::{EpochId, NodeId, Version, NTP64};
use discus_resp::Value as Resp;
use discus_store::{DbKey, Entry, Value, ZSet};
use discus_wire::{
    decode_frame, fb, Batch, BatchAck, DecodeError, Forward, ForwardMode, ForwardReply, Limits,
    Message, TraceContext,
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
            DbKey::new(0, "s"),
            entry(Value::Str("val".into()), v(1), Some(1234)),
        ),
        (DbKey::new(0, "h"), entry(Value::Hash(hash), v(2), None)),
        (DbKey::new(0, "set"), entry(Value::Set(set), v(3), None)),
        (DbKey::new(0, "z"), entry(Value::ZSet(zset), v(4), None)),
        (DbKey::new(0, "t"), tomb),
    ];
    b.offers = vec![(DbKey::new(0, "o"), v(7))];
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
        want: vec![DbKey::new(0, "k1"), DbKey::new(0, "k2")],
        epoch: epoch(3),
        idle: true,
    });
    assert_eq!(round_trip(m.clone()), m);
}

#[test]
fn fetch_and_digest_round_trip() {
    let m = Message::Fetch {
        keys: vec![DbKey::new(0, "a"), DbKey::new(0, "b")],
    };
    assert_eq!(round_trip(m.clone()), m);
    let m = Message::Digest {
        from: epoch(1),
        to: epoch(2),
        db: 0,
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
            idle: false,
        })
        .type_code(),
        Message::Fetch { keys: vec![] }.type_code(),
        Message::Entries(b).type_code(),
        Message::Digest {
            from: epoch(1),
            to: epoch(1),
            db: 0,
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
        db: 0,
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

fn fetch_frame(keys: &[&[u8]], dbs: Option<&[u32]>) -> Vec<u8> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let blobs: Vec<_> = keys
        .iter()
        .map(|k| {
            let data = fbb.create_vector(k);
            fb::Blob::create(&mut fbb, &fb::BlobArgs { data: Some(data) })
        })
        .collect();
    let keys = fbb.create_vector(&blobs);
    let dbs = dbs.map(|d| fbb.create_vector(d));
    let t = fb::Fetch::create(
        &mut fbb,
        &fb::FetchArgs {
            keys: Some(keys),
            dbs,
        },
    );
    fbb.finish_minimal(t);
    discus_wire::encode_envelope(
        fb::MessageType::FETCH.0,
        1,
        &TraceContext::default(),
        fbb.finished_data(),
    )
}

#[test]
fn an_entry_without_db_decodes_to_database_zero() {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let key = fbb.create_vector(b"k");
    let bytes = fbb.create_vector(b"v");
    let start = fbb.start_table();
    fbb.push_slot_always(fb::StoredEntry::VT_KEY, key);
    fbb.push_slot_always(fb::StoredEntry::VT_BYTES, bytes);
    fbb.push_slot_always(
        fb::StoredEntry::VT_VERSION,
        fb::Version::new(1, &fb::Id128::new(&[0; 16])),
    );
    let e = flatbuffers::WIPOffset::<fb::StoredEntry>::new(fbb.end_table(start).value());
    let entries = fbb.create_vector(&[e]);
    let b = fb::EntryBatch::create(
        &mut fbb,
        &fb::EntryBatchArgs {
            epoch: Some(&fb::EpochId::new(1, &fb::Id128::new(&[0; 16]))),
            entries: Some(entries),
            ..Default::default()
        },
    );
    fbb.finish_minimal(b);
    let env = discus_wire::encode_envelope(
        fb::MessageType::REPLICATE.0,
        1,
        &TraceContext::default(),
        fbb.finished_data(),
    );
    let Message::Replicate(b) = decode_frame(&env, &Limits::default()).unwrap().message else {
        panic!("not a batch");
    };
    assert_eq!(b.entries[0].0, DbKey::new(0, "k"));
}

#[test]
fn entries_offers_wants_and_fetches_keep_their_databases() {
    let mut b = Batch::new(epoch(4));
    b.entries = vec![
        (
            DbKey::new(0, "a"),
            entry(Value::Str("x".into()), v(1), None),
        ),
        (
            DbKey::new(3, "a"),
            entry(Value::Str("y".into()), v(2), None),
        ),
    ];
    b.offers = vec![(DbKey::new(0, "o"), v(3)), (DbKey::new(5, "o"), v(4))];
    for m in [
        Message::Handoff(b),
        Message::BatchAck(BatchAck {
            applied: 0,
            want: vec![DbKey::new(0, "w"), DbKey::new(5, "w")],
            epoch: epoch(4),
            idle: false,
        }),
        Message::Fetch {
            keys: vec![DbKey::new(0, "f"), DbKey::new(2, "f")],
        },
        Message::Digest {
            from: epoch(1),
            to: epoch(2),
            db: 4,
            sums: vec![9],
        },
    ] {
        assert_eq!(round_trip(m.clone()), m);
    }
}

#[test]
fn a_short_db_vector_means_database_zero() {
    let frame = fetch_frame(&[b"a", b"b", b"c"], Some(&[2]));
    let Message::Fetch { keys } = decode_frame(&frame, &Limits::default()).unwrap().message else {
        panic!("not a fetch");
    };
    let dbs: Vec<u32> = keys.iter().map(|k| k.db).collect();
    assert_eq!(dbs, [2, 0, 0]);
    let frame = fetch_frame(&[b"a"], None);
    let Message::Fetch { keys } = decode_frame(&frame, &Limits::default()).unwrap().message else {
        panic!("not a fetch");
    };
    assert_eq!(keys, [DbKey::new(0, "a")]);
}
