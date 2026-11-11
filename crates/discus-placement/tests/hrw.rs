#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::net::SocketAddr;

use discus_placement::hrw::{owners, score, unit};
use discus_placement::{Member, MemberState, NodeId, Weight, DEFAULT_RF};
use proptest::prelude::*;

fn member(id: u128, state: MemberState, weight: f64) -> Member {
    Member {
        id: NodeId::from_u128(id),
        addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        rpc_addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        client_addr: None,
        client_host: None,
        state,
        weight: Weight::new(weight).unwrap(),
    }
}

fn normal(ids: &[u128]) -> Vec<Member> {
    ids.iter()
        .map(|&i| member(i, MemberState::Normal, 1.0))
        .collect()
}

fn key(i: u32) -> Vec<u8> {
    format!("key:{i}").into_bytes()
}

fn primary(k: &[u8], members: &[Member]) -> NodeId {
    owners(k, members, 1)[0]
}

#[test]
fn unit_is_strictly_inside_zero_and_one() {
    for i in 0..10_000 {
        for n in [1u128, u128::MAX] {
            let u = unit(&key(i), NodeId::from_u128(n));
            assert!(u > 0.0 && u < 1.0, "u = {u}");
        }
    }
}

#[test]
fn the_hash_is_pinned() {
    // A change to the hash, the seed, or the byte order changes placement for every cluster.
    assert_eq!(
        unit(b"key", NodeId::from_u128(1)).to_bits(),
        PINNED_UNIT_BITS,
        "placement hash changed: {}",
        unit(b"key", NodeId::from_u128(1))
    );
}

const PINNED_UNIT_BITS: u64 = 0x3fe0_3ffb_5be0_26a7;

#[test]
fn the_hash_input_is_key_then_big_endian_id() {
    use discus_placement::PLACEMENT_SEED;
    let id = NodeId::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    let mut input = b"some key".to_vec();
    input.extend_from_slice(&id.to_be_bytes());
    let m = xxhash_rust::xxh3::xxh3_64_with_seed(&input, PLACEMENT_SEED) >> 12;
    let expected = (m as f64 + 0.5) / 4_503_599_627_370_496.0;
    assert_eq!(unit(b"some key", id), expected);
}

proptest! {
    #[test]
    fn unit_matches_the_streaming_hash(key in proptest::collection::vec(any::<u8>(), 0..600), id in any::<u128>()) {
        use discus_placement::PLACEMENT_SEED;
        let id = NodeId::from_u128(id);
        let mut h = xxhash_rust::xxh3::Xxh3::with_seed(PLACEMENT_SEED);
        h.update(&key);
        h.update(&id.to_be_bytes());
        let expected = ((h.digest() >> 12) as f64 + 0.5) / 4_503_599_627_370_496.0;
        prop_assert_eq!(unit(&key, id).to_bits(), expected.to_bits());
    }
}

#[test]
fn score_scales_with_weight() {
    let a = member(1, MemberState::Normal, 1.0);
    let b = member(1, MemberState::Normal, 3.0);
    assert_eq!(score(b"k", &b), 3.0 * score(b"k", &a));
    assert!(score(b"k", &a) > 0.0);
}

#[test]
fn only_normal_members_own_keys() {
    let members = vec![
        member(1, MemberState::Joining, 1.0),
        member(2, MemberState::Normal, 1.0),
        member(3, MemberState::Leaving, 1.0),
        member(4, MemberState::Dead, 1.0),
    ];
    for i in 0..100 {
        assert_eq!(owners(&key(i), &members, 3), vec![NodeId::from_u128(2)]);
    }
}

#[test]
fn no_normal_member_gives_no_owner() {
    let members = vec![member(1, MemberState::Dead, 1.0)];
    assert!(owners(b"k", &members, 3).is_empty());
    assert!(owners(b"k", &[], 3).is_empty());
}

#[test]
fn rf_caps_at_member_count() {
    assert_eq!(DEFAULT_RF, 3);
    assert_eq!(owners(b"k", &normal(&[1, 2]), DEFAULT_RF).len(), 2);
    assert_eq!(owners(b"k", &normal(&[1]), DEFAULT_RF).len(), 1);
    assert_eq!(owners(b"k", &normal(&[1, 2, 3, 4, 5]), DEFAULT_RF).len(), 3);
}

/// Share of `keys` primaries that changed between two member lists.
fn moved(before: &[Member], after: &[Member], keys: u32) -> f64 {
    let n = (0..keys)
        .filter(|&i| primary(&key(i), before) != primary(&key(i), after))
        .count();
    n as f64 / keys as f64
}

#[test]
fn adding_a_node_moves_about_its_fair_share() {
    let before = normal(&[11, 22, 33, 44, 55, 66, 77, 88, 99]);
    let mut after = before.clone();
    after.push(member(1010, MemberState::Normal, 1.0));
    let share = moved(&before, &after, 100_000);
    assert!((0.08..=0.12).contains(&share), "moved {share}");
}

#[test]
fn weight_is_proportional() {
    let mut members = normal(&[1, 2, 3, 4]);
    members.push(member(5, MemberState::Normal, 2.0));
    let heavy = (0..100_000)
        .filter(|&i| primary(&key(i), &members) == NodeId::from_u128(5))
        .count() as f64
        / 100_000.0;
    assert!((0.30..=0.37).contains(&heavy), "heavy share {heavy}");
}

fn member_ids() -> impl Strategy<Value = Vec<u128>> {
    prop::collection::hash_set(any::<u128>(), 1..12).prop_map(|s| s.into_iter().collect())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn owners_are_deterministic_and_order_free(ids in member_ids(), k in any::<Vec<u8>>(), rf in 1usize..5) {
        let members = normal(&ids);
        let mut reversed = members.clone();
        reversed.reverse();
        prop_assert_eq!(owners(&k, &members, rf), owners(&k, &members, rf));
        prop_assert_eq!(owners(&k, &members, rf), owners(&k, &reversed, rf));
    }

    #[test]
    fn one_primary_per_key(ids in member_ids(), dead in any::<Vec<bool>>(), k in any::<Vec<u8>>(), rf in 1usize..5) {
        let members: Vec<Member> = ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                let gone = dead.get(i).copied().unwrap_or(false);
                member(id, if gone { MemberState::Dead } else { MemberState::Normal }, 1.0)
            })
            .collect();
        let normal_count = members.iter().filter(|m| m.state == MemberState::Normal).count();
        let set = owners(&k, &members, rf);
        prop_assert_eq!(set.len(), rf.min(normal_count));
        prop_assert_eq!(set.iter().collect::<HashSet<_>>().len(), set.len());
        for id in &set {
            let m = members.iter().find(|m| m.id == *id).unwrap();
            prop_assert_eq!(m.state, MemberState::Normal);
        }
    }

    #[test]
    fn adding_a_node_moves_only_keys_to_it(ids in member_ids(), extra in any::<u128>(), rf in 1usize..5) {
        prop_assume!(!ids.contains(&extra));
        let before = normal(&ids);
        let mut after = before.clone();
        after.push(member(extra, MemberState::Normal, 1.0));
        let x = NodeId::from_u128(extra);
        for i in 0..2000 {
            let k = key(i);
            let (old, new) = (owners(&k, &before, rf), owners(&k, &after, rf));
            if old[0] != new[0] {
                prop_assert_eq!(new[0], x);
            }
            // The new set is the old one with x inserted, cut to rf.
            let without_x: Vec<NodeId> = new.iter().copied().filter(|&n| n != x).collect();
            prop_assert_eq!(&without_x[..], &old[..without_x.len()]);
        }
    }

    #[test]
    fn removing_a_node_moves_only_its_keys(ids in member_ids(), pick in any::<prop::sample::Index>()) {
        prop_assume!(ids.len() > 1);
        let before = normal(&ids);
        let gone = before[pick.index(before.len())].id;
        let after: Vec<Member> = before.iter().filter(|m| m.id != gone).cloned().collect();
        for i in 0..2000 {
            let k = key(i);
            let (old, new) = (primary(&k, &before), primary(&k, &after));
            if old != new {
                prop_assert_eq!(old, gone);
            }
        }
    }
}
