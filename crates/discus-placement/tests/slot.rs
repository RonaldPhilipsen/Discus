use discus_placement::{range_of, slot, RANGES, SLOTS, SLOTS_PER_RANGE};
use proptest::prelude::*;

// Values from CLUSTER KEYSLOT on Valkey 9 with cluster mode enabled.
#[test]
fn slot_matches_valkey() {
    let table: &[(&[u8], u16)] = &[
        (b"foo", 12182),
        (b"bar", 5061),
        (b"{user1000}.following", 3443),
        (b"{user1000}.followers", 3443),
        (b"foo{}{bar}", 8363),
        (b"foo{{bar}}zap", 4015),
        (b"foo{bar}{zap}", 5061),
        (b"{", 4092),
        (b"}{", 12793),
        (b"", 0),
        (b"a\x00\xffb", 8317),
        (b"\x00\xff", 7920),
    ];
    for (key, expected) in table {
        assert_eq!(slot(key), *expected, "key {key:?}");
    }
}

#[test]
fn range_of_groups_eight_slots() {
    assert_eq!(range_of(0), 0);
    assert_eq!(range_of(7), 0);
    assert_eq!(range_of(8), 1);
    assert_eq!(range_of(16383), 2047);
    assert_eq!(SLOTS, 16384);
    assert_eq!(SLOTS_PER_RANGE, 8);
    assert_eq!(RANGES, 2048);
}

proptest! {
    #[test]
    fn tag_decides_the_slot(
        prefix in proptest::collection::vec(any::<u8>().prop_filter("no brace", |b| *b != b'{'), 0..8),
        tag in proptest::collection::vec(any::<u8>().prop_filter("no brace", |b| *b != b'}'), 1..8),
        suffix in proptest::collection::vec(any::<u8>(), 0..8),
    ) {
        let mut key = prefix;
        key.push(b'{');
        key.extend_from_slice(&tag);
        key.push(b'}');
        key.extend_from_slice(&suffix);
        prop_assert_eq!(slot(&key), slot(&tag));
    }
}
