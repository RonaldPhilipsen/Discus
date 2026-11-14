use discus_telemetry::attrs::{
    check, Attribute, CommandLabel, GuardError, MemberState, MisrouteReason, NodeLabel,
    ReloadOutcome, Status, TopologyCommand, TransportChannel, ALLOWED_KEYS,
};

#[test]
fn guard_accepts_the_allowed_keys() {
    check([
        ("command", "GET"),
        ("status", "ok"),
        ("node", "0f"),
        ("peer", "ab12"),
        ("state", "normal"),
    ])
    .unwrap();
    check([("command", "unknown"), ("command", "unsupported")]).unwrap();
}

#[test]
fn guard_rejects_disallowed_keys() {
    for key in [
        "key",
        "value",
        "client.id",
        "connection.id",
        "error",
        "signal",
    ] {
        assert!(
            matches!(check([(key, "x")]), Err(GuardError::DisallowedKey(_))),
            "{key}"
        );
    }
}

#[test]
fn guard_rejects_values_outside_their_domain() {
    for (key, value) in [
        ("command", "get user:42"),
        ("command", ""),
        ("command", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ("status", "ERR wrong type"),
        ("state", "zombie"),
        ("peer", "10.0.0.1:7000"),
        ("node", "AB"),
    ] {
        assert!(
            matches!(check([(key, value)]), Err(GuardError::InvalidValue { .. })),
            "{key}={value}"
        );
    }
}

#[test]
fn node_labels_are_short_lower_case_hex() {
    assert!(NodeLabel::new("0123456789abcdef0123456789abcdef").is_ok());
    assert!(NodeLabel::new("0123456789abcdef0123456789abcdef0").is_err());
    assert!(NodeLabel::new("").is_err());
    assert!(NodeLabel::new("10.0.0.1:7000").is_err());
}

#[test]
fn every_typed_attribute_passes_the_guard() {
    let peer = NodeLabel::new("ab").unwrap();
    let all = [
        Attribute::Command(CommandLabel::new("ZRANGEBYSCORE")),
        Attribute::Command(CommandLabel::UNKNOWN),
        Attribute::Command(CommandLabel::UNSUPPORTED),
        Attribute::Node(peer.clone()),
        Attribute::Peer(peer),
    ]
    .into_iter()
    .chain(Status::ALL.into_iter().map(Attribute::Status))
    .chain(MemberState::ALL.into_iter().map(Attribute::State))
    .chain(TransportChannel::ALL.into_iter().map(Attribute::Channel))
    .chain(MisrouteReason::ALL.into_iter().map(Attribute::Reason))
    .chain(TopologyCommand::ALL.into_iter().map(Attribute::Topology))
    .chain(ReloadOutcome::ALL.into_iter().map(Attribute::Outcome));
    for a in all {
        assert!(ALLOWED_KEYS.contains(&a.key()));
        check([(a.key(), a.value())]).unwrap();
    }
}
