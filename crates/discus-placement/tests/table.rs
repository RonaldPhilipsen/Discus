#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;

use discus_placement::{hrw, range_of, slot, ClusterId, Epoch, EpochId, Member, MemberState};
use discus_placement::{NodeId, Weight, RANGES};
use proptest::prelude::*;

fn member(id: u128, state: MemberState) -> Member {
    Member {
        id: NodeId::from_u128(id),
        addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        rpc_addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        state,
        weight: Weight::default(),
    }
}

fn epoch(members: Vec<Member>) -> Epoch {
    let origin = members[0].id;
    Epoch::new(
        EpochId {
            generation: 1,
            origin,
        },
        ClusterId::from_u128(1),
        members,
    )
    .unwrap()
}

fn members_strategy() -> impl Strategy<Value = Vec<Member>> {
    proptest::collection::btree_map(any::<u128>(), 0u8..4, 1..=12).prop_map(|map| {
        map.into_iter()
            .map(|(id, s)| {
                let state = match s {
                    0 => MemberState::Joining,
                    1 => MemberState::Leaving,
                    _ => MemberState::Normal,
                };
                member(id, state)
            })
            .collect()
    })
}

fn normal_count(members: &[Member]) -> usize {
    members
        .iter()
        .filter(|m| m.state == MemberState::Normal)
        .count()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn table_matches_hrw_per_range(members in members_strategy(), rf in 1usize..5) {
        let e = epoch(members);
        let table = e.table(rf);
        for r in 0..RANGES {
            let got: Vec<NodeId> = table.owners(r).map(|m| m.id).collect();
            prop_assert_eq!(got, hrw::owners(&r.to_be_bytes(), e.members(), rf));
        }
    }

    #[test]
    fn table_ignores_member_order(
        members in members_strategy(),
        rf in 1usize..5,
        keys in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..24), 200),
    ) {
        let mut shuffled = members.clone();
        shuffled.reverse();
        shuffled.rotate_left(members.len() / 2);
        let a = epoch(members);
        let b = epoch(shuffled);
        for key in &keys {
            prop_assert_eq!(a.owners(key, rf), b.owners(key, rf));
        }
    }

    #[test]
    fn owners_follow_the_slot(
        members in members_strategy(),
        rf in 1usize..5,
        key in proptest::collection::vec(any::<u8>(), 0..24),
    ) {
        let e = epoch(members);
        prop_assert_eq!(e.owners(&key, rf), e.range_owners(range_of(slot(&key)), rf));
        prop_assert_eq!(e.owners(&key, rf), e.slot_owners(slot(&key), rf));
        prop_assert_eq!(e.primary(&key), e.owners(&key, rf).first().copied());
    }

    #[test]
    fn owner_lists_are_distinct_normal_members(members in members_strategy(), rf in 1usize..5) {
        let normal = normal_count(&members);
        let e = epoch(members);
        for r in 0..RANGES {
            let owners = e.range_owners(r, rf);
            prop_assert_eq!(owners.len(), rf.min(normal));
            let mut sorted = owners.clone();
            sorted.sort();
            sorted.dedup();
            prop_assert_eq!(sorted.len(), owners.len());
            prop_assert!(owners.iter().all(|&id| e.is_normal(id)));
        }
    }

    #[test]
    fn adding_a_member_moves_only_ranges_to_it(
        members in members_strategy(),
        extra in any::<u128>(),
        rf in 1usize..5,
    ) {
        prop_assume!(members.iter().all(|m| m.id != NodeId::from_u128(extra)));
        let old = epoch(members.clone());
        let mut grown = members;
        grown.push(member(extra, MemberState::Normal));
        let new = epoch(grown);
        let added = NodeId::from_u128(extra);
        let changed = old.changed_ranges(&new, rf);
        for r in 0..RANGES {
            if changed.contains(r) {
                prop_assert!(new.range_owners(r, rf).contains(&added));
            } else {
                prop_assert_eq!(old.range_owners(r, rf), new.range_owners(r, rf));
            }
        }
    }

    #[test]
    fn removing_a_member_moves_only_its_ranges(
        members in members_strategy(),
        rf in 1usize..5,
    ) {
        prop_assume!(members.len() > 1);
        let old = epoch(members.clone());
        let mut shrunk = members;
        let removed = shrunk.pop().unwrap().id;
        let new = epoch(shrunk);
        let changed = old.changed_ranges(&new, rf);
        for r in 0..RANGES {
            if changed.contains(r) {
                prop_assert!(old.range_owners(r, rf).contains(&removed));
            } else {
                prop_assert_eq!(old.range_owners(r, rf), new.range_owners(r, rf));
            }
        }
    }
}

#[test]
fn changed_ranges_counts_set_bits() {
    let a = epoch(vec![member(1, MemberState::Normal)]);
    assert!(a.changed_ranges(&a.clone(), 3).is_empty());
    let b = epoch(vec![
        member(1, MemberState::Normal),
        member(2, MemberState::Normal),
    ]);
    let changed = a.changed_ranges(&b, 1);
    let counted = (0..RANGES).filter(|&r| changed.contains(r)).count();
    assert_eq!(changed.len(), counted);
    assert!(!changed.is_empty());
}

#[test]
fn a_different_rf_bypasses_the_cached_table() {
    let e = epoch((1..=5).map(|i| member(i, MemberState::Normal)).collect());
    assert_eq!(e.table(3).rf(), 3);
    for r in [0, 1, 700, RANGES - 1] {
        assert_eq!(
            e.range_owners(r, 2),
            hrw::owners(&r.to_be_bytes(), e.members(), 2)
        );
    }
}

#[test]
fn balance_stays_near_the_mean() {
    let mut worst = 0.0f64;
    for n in [3u128, 6, 12] {
        for seed in 0..50u128 {
            let members = (0..n)
                .map(|i| member(seed * 1000 + i + 1, MemberState::Normal))
                .collect();
            let e = epoch(members);
            let mut counts = std::collections::HashMap::new();
            for r in 0..RANGES {
                *counts.entry(e.range_owners(r, 3)[0]).or_insert(0usize) += 1;
            }
            let mean = f64::from(RANGES) / n as f64;
            let share = *counts.values().max().unwrap() as f64 / mean;
            worst = worst.max(share);
            assert!(share < 1.5, "n={n} seed={seed} share={share}");
        }
    }
    eprintln!("largest primary share seen: {worst:.3} times the mean");
}

#[test]
fn smaller_rf_reads_the_cached_table_prefix() {
    let e = epoch((1..=5).map(|i| member(i, MemberState::Normal)).collect());
    assert_eq!(e.table(3).rf(), 3);
    for key in [&b"a"[..], b"b", b"{tag}x", b"zzz"] {
        let r = range_of(slot(key));
        let direct = |rf| hrw::owners(&r.to_be_bytes(), e.members(), rf);
        assert_eq!(e.primary(key), direct(1).first().copied());
        assert_eq!(e.owners(key, 2), direct(2));
        assert_eq!(e.owners(key, 3), direct(3));
        assert_eq!(e.owners(key, 4), direct(4));
    }
    assert_eq!(e.table(1).rf(), 3);
}

#[test]
fn primary_first_does_not_pin_the_table_to_rf_one() {
    let e = epoch((1..=5).map(|i| member(i, MemberState::Normal)).collect());
    let _ = e.table(3);
    let _ = e.primary(b"k");
    assert_eq!(e.table(1).rf(), 3);
}

#[test]
fn a_table_listing_every_member_answers_a_larger_rf() {
    let e = epoch((1..=2).map(|i| member(i, MemberState::Normal)).collect());
    assert_eq!(e.table(3).rf(), 3);
    assert!(e.table(3).lists_all_normal());
    let r = range_of(slot(b"k"));
    assert_eq!(
        e.range_owners(r, 5),
        hrw::owners(&r.to_be_bytes(), e.members(), 5)
    );
}
