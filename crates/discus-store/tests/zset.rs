use discus_store::ZSet;
use std::ops::Bound;

#[test]
fn orders_by_score_then_member_and_supports_negative_ranges() {
    let mut z = ZSet::new();
    assert!(z.insert("b".into(), 1.0));
    assert!(z.insert("a".into(), 1.0));
    assert!(z.insert("c".into(), 0.5));
    assert!(!z.insert("a".into(), 2.0));
    let all = z.range(0, -1, false);
    let members: Vec<&[u8]> = all.iter().map(|(m, _)| m.as_ref()).collect();
    assert_eq!(members, vec![b"c" as &[u8], b"b", b"a"]);
    let top = z.range(0, 0, true);
    let rev: Vec<&[u8]> = top.iter().map(|(m, _)| m.as_ref()).collect();
    assert_eq!(rev, vec![b"a" as &[u8]]);
    assert_eq!(z.count(Bound::Included(1.0), Bound::Unbounded), 2);
    assert_eq!(z.count(Bound::Excluded(1.0), Bound::Unbounded), 1);
    assert!(z.remove(b"b"));
    assert_eq!(z.len(), 2);
    assert_eq!(z.score(b"a"), Some(2.0));
    assert_eq!(z.score(b"b"), None);
}

#[test]
fn range_edges_are_clamped_like_redis() {
    let mut z = ZSet::new();
    for (i, m) in ["a", "b", "c", "d"].iter().enumerate() {
        z.insert((*m).into(), i as f64);
    }
    assert_eq!(z.range(-2, -1, false).len(), 2);
    assert_eq!(z.range(0, 100, false).len(), 4);
    assert!(z.range(5, 10, false).is_empty());
    assert!(z.range(2, 1, false).is_empty());
    assert_eq!(z.range(-100, 0, false).len(), 1);
}

#[test]
fn range_by_score_respects_bounds() {
    let mut z = ZSet::new();
    for (i, m) in ["a", "b", "c", "d"].iter().enumerate() {
        z.insert((*m).into(), i as f64);
    }
    let r = z.range_by_score(Bound::Excluded(0.0), Bound::Included(2.0));
    let names: Vec<&[u8]> = r.iter().map(|(m, _)| m.as_ref()).collect();
    assert_eq!(names, vec![b"b" as &[u8], b"c"]);
}

/// redis-server 8.10.2: `ZADD z -0 b 0 a` orders `a` before `b`, and `ZSCORE z b` is `0`.
#[test]
fn minus_zero_is_the_same_score_as_zero() {
    let mut z = ZSet::new();
    z.insert("b".into(), -0.0);
    z.insert("a".into(), 0.0);
    let members: Vec<&[u8]> = z
        .range(0, -1, false)
        .into_iter()
        .map(|(m, _)| m.as_ref())
        .collect();
    assert_eq!(members, vec![b"a" as &[u8], b"b"]);
    assert!(z.score(b"b").is_some_and(|s| s.is_sign_positive()));
    assert_eq!(z.count(Bound::Excluded(-0.0), Bound::Unbounded), 0);
    assert_eq!(z.count(Bound::Included(0.0), Bound::Included(-0.0)), 2);
}
