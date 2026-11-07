#![allow(clippy::unwrap_used)]

use discus_placement::{ClusterId, MemberState, NodeId, Weight};

#[test]
fn node_ids_are_random_and_round_trip_as_hex() {
    let a = NodeId::random();
    assert_ne!(a, NodeId::random());
    assert_eq!(a.to_string().len(), 32);
    assert_eq!(a.to_string().parse::<NodeId>().unwrap(), a);
    assert!("xyz".parse::<NodeId>().is_err());
    assert_eq!(NodeId::from_u128(255).to_string(), format!("{:032x}", 255));
}

#[test]
fn cluster_ids_are_minted_fresh() {
    let c = ClusterId::mint();
    assert_ne!(c, ClusterId::mint());
    assert_ne!(c, ClusterId::NIL);
    assert_eq!(c.to_string().parse::<ClusterId>().unwrap(), c);
}

#[test]
fn weight_rejects_zero_negative_and_non_finite() {
    assert_eq!(Weight::default().get(), 1.0);
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(Weight::new(bad).is_none());
    }
    assert_eq!(Weight::new(2.5).unwrap().get(), 2.5);
}

#[test]
fn member_state_names_match_the_spec() {
    assert_eq!(MemberState::Joining.name(), "JOINING");
    assert_eq!(MemberState::Normal.name(), "NORMAL");
    assert_eq!(MemberState::Leaving.name(), "LEAVING");
    assert_eq!(MemberState::Dead.name(), "DEAD");
}
