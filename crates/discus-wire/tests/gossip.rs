#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;

use bytes::Bytes;
use discus_placement::{ClusterId, Epoch, EpochId, Member, MemberState, NodeId, Weight};
use discus_wire::fb;
use discus_wire::{
    decode_frame, encode_envelope, encode_frame, Capacity, DecodeError, Frame, Gossip, GossipKind,
    Limits, Liveness, MemberInfo, Message, Rumor, TraceContext,
};

fn id(n: u128) -> NodeId {
    NodeId::from_u128(n)
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn info(n: u128, port: u16, state: MemberState) -> MemberInfo {
    MemberInfo {
        id: id(n),
        addr: addr(port),
        state,
        incarnation: n as u64 + 3,
        weight: Weight::new(2.0).unwrap(),
        epoch: EpochId {
            generation: 4,
            origin: id(1),
        },
        capabilities: discus_wire::caps::KNOWN,
        capacity: Capacity {
            used: 100,
            max: 1000,
        },
        leader_hints: Bytes::new(),
    }
}

fn epoch() -> Epoch {
    let members = (1..=3)
        .map(|n| Member {
            id: id(n),
            addr: addr(7000 + n as u16),
            state: MemberState::Normal,
            weight: Weight::default(),
        })
        .collect();
    Epoch::new(
        EpochId {
            generation: 4,
            origin: id(1),
        },
        ClusterId::from_u128(9),
        members,
    )
    .unwrap()
}

#[test]
fn a_gossip_frame_round_trips() {
    let mut from = info(1, 7001, MemberState::Normal);
    from.leader_hints = Bytes::from_static(&[1, 2, 3]);
    let gossip = Gossip {
        kind: GossipKind::JoinReply,
        from,
        cluster_id: ClusterId::from_u128(9),
        cluster_size: 3,
        epoch_body: Some(epoch()),
        hlc: 42,
        target: Some((id(2), addr(7002))),
        rumors: vec![
            Rumor {
                info: info(2, 7002, MemberState::Normal),
                liveness: Liveness::Suspect,
                accuser: Some(id(3)),
            },
            Rumor {
                info: info(3, 7003, MemberState::Leaving),
                liveness: Liveness::Dead,
                accuser: None,
            },
        ],
        retry_after_ms: 1500,
    };
    let frame = Frame {
        request_id: 5,
        trace: TraceContext::default(),
        message: Message::Gossip(gossip),
    };
    let decoded = decode_frame(&encode_frame(&frame), &Limits::default()).unwrap();
    assert_eq!(decoded, frame);
}

#[test]
fn every_kind_round_trips() {
    for kind in [
        GossipKind::Ping,
        GossipKind::Ack,
        GossipKind::Nack,
        GossipKind::PingReq,
        GossipKind::Sync,
        GossipKind::SyncReply,
        GossipKind::Join,
        GossipKind::JoinReply,
        GossipKind::Leave,
    ] {
        let frame = Frame {
            request_id: 1,
            trace: TraceContext::default(),
            message: Message::Gossip(Gossip::new(kind, info(1, 7001, MemberState::Joining))),
        };
        let decoded = decode_frame(&encode_frame(&frame), &Limits::default()).unwrap();
        assert_eq!(decoded, frame);
    }
}

/// Build a gossip body by hand with the given kind byte, and with or without a sender.
fn raw_gossip(kind: u8, with_sender: bool) -> Vec<u8> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let from = with_sender.then(|| {
        let a = fbb.create_string("127.0.0.1:7001");
        fb::MemberInfo::create(
            &mut fbb,
            &fb::MemberInfoArgs {
                id: Some(&fb::Id128::new(&[1; 16])),
                addr: Some(a),
                epoch: Some(&fb::EpochId::new(1, &fb::Id128::new(&[1; 16]))),
                weight: 1.0,
                ..Default::default()
            },
        )
    });
    let g = fb::Gossip::create(
        &mut fbb,
        &fb::GossipArgs {
            kind: fb::GossipKind(kind),
            from,
            ..Default::default()
        },
    );
    fbb.finish_minimal(g);
    let body = fbb.finished_data().to_vec();
    encode_envelope(
        fb::MessageType::GOSSIP.0,
        1,
        &TraceContext::default(),
        &body,
    )
}

#[test]
fn an_unknown_gossip_kind_is_invalid() {
    let err = decode_frame(&raw_gossip(200, true), &Limits::default()).unwrap_err();
    assert!(matches!(err, DecodeError::Invalid(_)), "{err:?}");
    // The same body with a known kind decodes.
    assert!(decode_frame(&raw_gossip(0, true), &Limits::default()).is_ok());
}

#[test]
fn a_gossip_without_a_sender_is_invalid() {
    let err = decode_frame(&raw_gossip(0, false), &Limits::default()).unwrap_err();
    assert!(matches!(err, DecodeError::Invalid(_)), "{err:?}");
}

#[test]
fn gossip_is_message_type_8() {
    let m = Message::Gossip(Gossip::new(
        GossipKind::Ping,
        info(1, 1, MemberState::Normal),
    ));
    assert_eq!(m.type_code(), 8);
    assert_eq!(fb::MessageType::GOSSIP.0, 8);
}

#[test]
fn gossip_is_a_capability() {
    assert_eq!(discus_wire::caps::GOSSIP, 2);
    assert_ne!(discus_wire::caps::KNOWN & discus_wire::caps::GOSSIP, 0);
}
