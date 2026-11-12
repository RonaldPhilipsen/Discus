#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;

use discus_placement::{
    Adopt, ClusterId, Epoch, EpochError, EpochHistory, EpochId, Member, MemberState, NodeId,
    Weight, MIN_HISTORY,
};

fn member(id: u128) -> Member {
    Member {
        id: NodeId::from_u128(id),
        addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        rpc_addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        client_addr: None,
        client_host: None,
        state: MemberState::Normal,
        weight: Weight::default(),
    }
}

fn id(generation: u64, origin: u128) -> EpochId {
    EpochId {
        generation,
        origin: NodeId::from_u128(origin),
    }
}

const CLUSTER: ClusterId = ClusterId::from_u128(42);

fn epoch(generation: u64) -> Epoch {
    Epoch::new(id(generation, 1), CLUSTER, vec![member(1)]).unwrap()
}

fn history_to(last: u64) -> EpochHistory {
    let mut h = EpochHistory::new(Epoch::bootstrap(member(1), CLUSTER));
    for g in 2..=last {
        assert_eq!(h.adopt(epoch(g)).unwrap(), Adopt::Newer);
    }
    h
}

fn generations(h: &EpochHistory) -> Vec<u64> {
    h.ids().iter().map(|i| i.generation).collect()
}

#[test]
fn epoch_ids_order_by_generation_then_origin() {
    assert!(id(1, 9) < id(2, 1));
    assert!(id(2, 1) < id(2, 2));
    assert_eq!(id(3, 1).to_string(), format!("3:{}", NodeId::from_u128(1)));
}

#[test]
fn an_epoch_of_another_cluster_is_refused() {
    let theirs = Epoch::new(id(5, 2), ClusterId::from_u128(7), vec![member(2)]).unwrap();
    let mut h = EpochHistory::new(Epoch::bootstrap(member(1), CLUSTER));
    assert!(matches!(
        h.adopt(theirs),
        Err(EpochError::ForeignCluster { .. })
    ));
    assert_eq!(h.current().id(), id(1, 1));
}

#[test]
fn bootstrap_epoch_is_generation_one_from_self() {
    let e = Epoch::bootstrap(member(9), CLUSTER);
    assert_eq!(e.id(), id(1, 9));
    assert_eq!(e.cluster_id(), CLUSTER);
    assert_eq!(e.members(), &[member(9)]);
    assert_eq!(e.primary(b"k"), Some(NodeId::from_u128(9)));
    assert_eq!(e.owners(b"k", 3)[..], [NodeId::from_u128(9)]);
}

#[test]
fn members_are_sorted_and_unique() {
    let e = Epoch::new(id(1, 1), CLUSTER, vec![member(3), member(1), member(2)]).unwrap();
    let ids: Vec<u128> = e.members().iter().map(|m| m.id.as_u128()).collect();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(e.member(NodeId::from_u128(2)), Some(&member(2)));
    assert_eq!(
        Epoch::new(id(1, 1), CLUSTER, vec![member(1), member(1)]).unwrap_err(),
        EpochError::DuplicateMember(NodeId::from_u128(1))
    );
}

#[test]
fn higher_epoch_wins() {
    let mut h = history_to(3);
    assert_eq!(h.current().id(), id(3, 1));
    assert_eq!(h.previous().unwrap().id(), id(2, 1));
    assert_eq!(h.adopt(epoch(2)).unwrap(), Adopt::Stale);
    assert_eq!(h.adopt(epoch(3)).unwrap(), Adopt::Stale);
    assert_eq!(h.current().id(), id(3, 1));
    let tie = Epoch::new(id(3, 2), CLUSTER, vec![member(1)]).unwrap();
    assert_eq!(h.adopt(tie).unwrap(), Adopt::Newer);
    assert_eq!(h.current().id(), id(3, 2));
}

#[test]
fn next_generation_is_max_seen_plus_one() {
    let mut h = history_to(7);
    assert_eq!(h.next_generation(), 8);
    assert_eq!(h.adopt(epoch(3)).unwrap(), Adopt::Stale);
    assert_eq!(h.next_generation(), 8);
    let p = h
        .propose(NodeId::from_u128(5), vec![member(1), member(5)])
        .unwrap();
    assert_eq!(p.id(), id(8, 5));
    assert_eq!(p.cluster_id(), CLUSTER);
}

#[test]
fn history_keeps_the_last_four() {
    assert_eq!(MIN_HISTORY, 4);
    let h = history_to(10);
    assert_eq!(generations(&h), vec![7, 8, 9, 10]);
    assert!(h.get(id(6, 1)).is_none());
    assert!(h.get(id(9, 1)).is_some());
}

#[test]
fn pinned_epochs_survive_pruning() {
    let mut h = history_to(2);
    h.pin(id(2, 1)).unwrap();
    for g in 3..=10 {
        h.adopt(epoch(g)).unwrap();
    }
    assert_eq!(generations(&h), vec![2, 7, 8, 9, 10]);
    h.unpin(id(2, 1));
    assert_eq!(generations(&h), vec![7, 8, 9, 10]);
}

#[test]
fn pins_are_counted() {
    let mut h = history_to(2);
    h.pin(id(1, 1)).unwrap();
    h.pin(id(1, 1)).unwrap();
    h.pin(id(2, 1)).unwrap();
    h.unpin(id(1, 1));
    for g in 3..=8 {
        h.adopt(epoch(g)).unwrap();
    }
    assert!(h.get(id(1, 1)).is_some());
    h.unpin(id(1, 1));
    h.unpin(id(1, 1));
    assert!(h.get(id(1, 1)).is_none());
}

#[test]
fn pin_of_an_unknown_epoch_fails() {
    let mut h = history_to(1);
    assert_eq!(h.pin(id(9, 9)), Err(EpochError::Unknown(id(9, 9))));
}

#[test]
fn the_current_epoch_is_never_pruned() {
    let mut h = history_to(1);
    h.unpin(id(1, 1));
    assert_eq!(generations(&h), vec![1]);
    assert_eq!(h.current().id(), id(1, 1));
    assert!(h.previous().is_none());
}
