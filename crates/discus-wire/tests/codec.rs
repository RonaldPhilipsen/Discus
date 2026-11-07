#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;

use bytes::Bytes;
use discus_placement::{
    ClusterId, Epoch, EpochId, Member, MemberState, NodeId, Version, Weight, NTP64,
};
use discus_resp::Value;
use discus_wire::fb;
use discus_wire::{
    decode_frame, encode_envelope, encode_frame, DecodeError, Forward, ForwardMode, ForwardReply,
    Frame, HandshakeCode, HandshakeFailed, Hello, Limits, Message, TraceContext,
};

fn id(n: u128) -> NodeId {
    NodeId::from_u128(n)
}

fn epoch_id(generation: u64, origin: u128) -> EpochId {
    EpochId {
        generation,
        origin: id(origin),
    }
}

fn trace() -> TraceContext {
    TraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: Some("discus=1".into()),
    }
}

fn limits() -> Limits {
    Limits::default()
}

fn member(n: u128, port: u16, state: MemberState) -> Member {
    Member {
        id: id(n),
        addr: SocketAddr::from(([127, 0, 0, 1], port)),
        state,
        weight: Weight::new(1.5).unwrap(),
    }
}

fn hello() -> Hello {
    Hello {
        node_id: id(0x1234_5678_9abc_def0_1122_3344_5566_7788),
        cluster_id: ClusterId::from_u128(77),
        capabilities: discus_wire::caps::KNOWN,
        epoch: epoch_id(3, 9),
        max_frame: 1 << 20,
    }
}

fn all_kinds() -> Value {
    Value::Array(vec![
        Value::simple("OK"),
        Value::error("ERR nope"),
        Value::Integer(-42),
        Value::bulk(Bytes::from_static(b"bytes\x00\xff")),
        Value::Null,
        Value::Map(vec![(Value::bulk("k"), Value::Double(1.5))]),
        Value::Boolean(true),
        Value::Array(vec![]),
    ])
}

fn round_trip(message: Message) {
    let frame = Frame {
        request_id: 7,
        trace: trace(),
        message,
    };
    let bytes = encode_frame(&frame);
    assert_eq!(decode_frame(&bytes, &limits()).unwrap(), frame);
}

#[test]
fn every_message_round_trips() {
    round_trip(Message::Hello(hello()));
    round_trip(Message::HandshakeFailed(HandshakeFailed {
        code: HandshakeCode::ClusterMismatch,
        reason: "another cluster".into(),
    }));
    round_trip(Message::Unsupported { message_type: 999 });
    let cluster = ClusterId::from_u128(5);
    let body = Epoch::new(
        epoch_id(4, 2),
        cluster,
        vec![
            member(2, 7001, MemberState::Normal),
            member(1, 7002, MemberState::Dead),
        ],
    )
    .unwrap();
    round_trip(Message::Forward(Forward {
        command: discus_wire::encode_command(&[Bytes::from("SET"), Bytes::from("k")]),
        epoch: epoch_id(4, 2),
        epoch_body: Some(body),
        hops: 1,
        path: vec![id(1), id(2)],
        mode: ForwardMode::Route,
    }));
    round_trip(Message::Forward(Forward {
        command: Bytes::from_static(b"*1\r\n$6\r\nDBSIZE\r\n"),
        epoch: epoch_id(1, 1),
        epoch_body: None,
        hops: 1,
        path: vec![],
        mode: ForwardMode::Local,
    }));
    round_trip(Message::ForwardReply(ForwardReply {
        value: all_kinds(),
        applied: Some(Version::new(NTP64(123_456), id(99))),
        epoch: epoch_id(4, 2),
    }));
    round_trip(Message::ForwardReply(ForwardReply {
        value: Value::Null,
        applied: None,
        epoch: epoch_id(1, 1),
    }));
    round_trip(Message::Ping { nonce: 11 });
    round_trip(Message::Pong { nonce: 12 });
}

#[test]
fn version_struct_is_the_uhlc_layout() {
    assert_eq!(std::mem::size_of::<fb::Version>(), 24);
    let origin = id(0xdead_beef);
    let reply = Message::ForwardReply(ForwardReply {
        value: Value::ok(),
        applied: Some(Version::new(NTP64(5), origin)),
        epoch: epoch_id(1, 1),
    });
    let frame = Frame {
        request_id: 1,
        trace: TraceContext::default(),
        message: reply,
    };
    let decoded = decode_frame(&encode_frame(&frame), &limits()).unwrap();
    let Message::ForwardReply(r) = decoded.message else {
        panic!("not a reply");
    };
    let v = r.applied.unwrap();
    assert_eq!(v.origin(), origin);
    assert_eq!(v.time(), NTP64(5));
}

#[test]
fn garbage_fails_the_verifier() {
    assert!(matches!(
        decode_frame(&[0xff; 64], &limits()),
        Err(DecodeError::Invalid(_))
    ));
    assert!(matches!(
        decode_frame(&[], &limits()),
        Err(DecodeError::Invalid(_))
    ));
}

#[test]
fn a_bad_nested_payload_fails_the_verifier() {
    let bytes = encode_envelope(
        fb::MessageType::HELLO.0,
        3,
        &TraceContext::default(),
        &[1, 2, 3],
    );
    assert!(matches!(
        decode_frame(&bytes, &limits()),
        Err(DecodeError::Invalid(_))
    ));
}

fn nested(depth: usize) -> Value {
    let mut v = Value::Integer(1);
    for _ in 0..depth {
        v = Value::Array(vec![v]);
    }
    v
}

#[test]
fn deep_nesting_fails_the_depth_limit() {
    let frame = |depth| Frame {
        request_id: 1,
        trace: TraceContext::default(),
        message: Message::ForwardReply(ForwardReply {
            value: nested(depth),
            applied: None,
            epoch: epoch_id(1, 1),
        }),
    };
    assert!(matches!(
        decode_frame(&encode_frame(&frame(40)), &limits()),
        Err(DecodeError::Invalid(_))
    ));
    assert_eq!(
        decode_frame(&encode_frame(&frame(3)), &limits()).unwrap(),
        frame(3)
    );
}

#[test]
fn a_frame_over_the_size_limit_fails() {
    let frame = Frame {
        request_id: 1,
        trace: TraceContext::default(),
        message: Message::Forward(Forward {
            command: Bytes::from(vec![b'x'; 2000]),
            epoch: epoch_id(1, 1),
            epoch_body: None,
            hops: 1,
            path: vec![],
            mode: ForwardMode::Route,
        }),
    };
    let small = Limits { max_frame: 1000 };
    assert!(matches!(
        decode_frame(&encode_frame(&frame), &small),
        Err(DecodeError::Invalid(_))
    ));
}

#[test]
fn an_unknown_type_reports_its_code_and_request_id() {
    let bytes = encode_envelope(999, 42, &trace(), &[]);
    match decode_frame(&bytes, &limits()) {
        Err(DecodeError::UnknownType {
            message_type,
            request_id,
            trace: t,
        }) => {
            assert_eq!(message_type, 999);
            assert_eq!(request_id, 42);
            assert_eq!(t, trace());
        }
        other => panic!("expected UnknownType, got {other:?}"),
    }
}

#[test]
fn unknown_fields_are_ignored() {
    let h = hello();
    // A Hello table built by hand: every known field plus a u64 in field id 20.
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let start = fbb.start_table();
    fbb.push_slot_always(
        fb::Hello::VT_NODE_ID,
        fb::Id128::new(&h.node_id.to_be_bytes()),
    );
    fbb.push_slot_always(
        fb::Hello::VT_CLUSTER_ID,
        fb::Id128::new(&h.cluster_id.to_be_bytes()),
    );
    fbb.push_slot::<u64>(fb::Hello::VT_CAPABILITIES, h.capabilities, 0);
    fbb.push_slot_always(
        fb::Hello::VT_EPOCH,
        fb::EpochId::new(
            h.epoch.generation,
            &fb::Id128::new(&h.epoch.origin.to_be_bytes()),
        ),
    );
    fbb.push_slot::<u32>(fb::Hello::VT_MAX_FRAME, h.max_frame, 0);
    fbb.push_slot::<u64>(4 + 2 * 20, 0xabcdef, 0);
    let table = fbb.end_table(start);
    fbb.finish_minimal(table);
    let payload = fbb.finished_data().to_vec();

    let bytes = encode_envelope(
        fb::MessageType::HELLO.0,
        8,
        &TraceContext::default(),
        &payload,
    );
    let decoded = decode_frame(&bytes, &limits()).unwrap();
    assert_eq!(decoded.message, Message::Hello(h.clone()));

    // An Envelope with an extra field id 9 also decodes.
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let body = fbb.create_vector(&payload);
    let start = fbb.start_table();
    fbb.push_slot::<u16>(fb::Envelope::VT_TYPE_, fb::MessageType::HELLO.0, 0);
    fbb.push_slot::<u64>(fb::Envelope::VT_REQUEST_ID, 8, 0);
    fbb.push_slot_always(fb::Envelope::VT_PAYLOAD, body);
    fbb.push_slot::<u32>(4 + 2 * 9, 5, 0);
    let table = fbb.end_table(start);
    fbb.finish_minimal(table);
    let decoded = decode_frame(fbb.finished_data(), &limits()).unwrap();
    assert_eq!(decoded.request_id, 8);
    assert_eq!(decoded.message, Message::Hello(h));
}

#[test]
fn trace_context_is_optional() {
    let frame = Frame {
        request_id: 2,
        trace: TraceContext::default(),
        message: Message::Ping { nonce: 1 },
    };
    let decoded = decode_frame(&encode_frame(&frame), &limits()).unwrap();
    assert_eq!(decoded.trace.traceparent, None);
    assert_eq!(decoded.trace.tracestate, None);
}

#[test]
fn encode_command_is_a_resp_multibulk() {
    let wire = discus_wire::encode_command(&[Bytes::from("GET"), Bytes::from("key")]);
    assert_eq!(&wire[..], b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
}
