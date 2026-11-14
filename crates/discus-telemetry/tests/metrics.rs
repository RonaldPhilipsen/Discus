#![cfg(all(feature = "otel", feature = "testing"))]

use std::time::Duration;

use discus_telemetry::attrs::{
    check, CommandLabel, CounterMode, CounterRole, MemberState, NodeLabel, Status,
    TransportChannel, ALLOWED_KEYS,
};
use discus_telemetry::metrics::{StoreStats, TransportCounts, LATENCY_BUCKETS_MS};
use discus_telemetry::testing::{MetricHarness, Point};

fn find<'a>(points: &'a [Point], name: &str, attr: Option<(&str, &str)>) -> Option<&'a Point> {
    points.iter().find(|p| {
        p.name == name
            && attr.is_none_or(|(k, v)| p.attrs.iter().any(|(pk, pv)| pk == k && pv == v))
    })
}

fn assert_bounded(points: &[Point]) {
    for p in points {
        for (k, _) in &p.attrs {
            assert!(ALLOWED_KEYS.contains(&k.as_str()), "{}: {k}", p.name);
        }
        if let Err(e) = check(p.attrs.iter().map(|(k, v)| (k.as_str(), v.as_str()))) {
            panic!("{}: {e}", p.name);
        }
    }
}

#[test]
fn command_metrics_use_only_allowed_attributes_and_the_spec_buckets() {
    let h = MetricHarness::new();
    h.metrics.command(
        CommandLabel::new("GET"),
        Status::Ok,
        Duration::from_micros(300),
    );
    h.metrics.command(
        CommandLabel::new("GET"),
        Status::Ok,
        Duration::from_micros(200),
    );
    h.metrics.command(
        CommandLabel::new("SET"),
        Status::Error,
        Duration::from_millis(3),
    );
    let points = h.points();

    let get = find(&points, "discus.commands", Some(("command", "GET"))).unwrap();
    assert_eq!(get.value, 2.0);
    assert!(get.attrs.contains(&("status".into(), "ok".into())));
    let set = find(&points, "discus.commands", Some(("command", "SET"))).unwrap();
    assert!(set.attrs.contains(&("status".into(), "error".into())));

    let dur = find(&points, "discus.command.duration", Some(("command", "GET"))).unwrap();
    assert_eq!(dur.bounds, LATENCY_BUCKETS_MS);
    assert_eq!(dur.value, 2.0);
    assert_bounded(&points);
}

#[test]
fn auth_results_are_counted_by_status() {
    let h = MetricHarness::new();
    h.metrics.auth(false);
    h.metrics.auth(false);
    h.metrics.auth(true);
    let points = h.points();
    let fail = find(&points, "discus.auth.attempts", Some(("status", "failure"))).unwrap();
    let ok = find(&points, "discus.auth.attempts", Some(("status", "success"))).unwrap();
    assert_eq!((fail.value, ok.value), (2.0, 1.0));
}

#[test]
fn health_stats_are_observed() {
    let h = MetricHarness::new();
    h.metrics
        .observe_health(|| discus_telemetry::metrics::HealthStats {
            lag_ms: 12,
            score_ms: 3.5,
            multiplier: 1.5,
            evidence_dropped: 4,
        });
    let points = h.points();
    for (name, value) in [
        ("discus.local_health.lag", 12.0),
        ("discus.local_health.score", 3.5),
        ("discus.local_health.multiplier", 1.5),
        ("discus.local_health.evidence_dropped", 4.0),
    ] {
        assert_eq!(find(&points, name, None).unwrap().value, value, "{name}");
    }
}

#[test]
fn store_stats_are_observed() {
    let h = MetricHarness::new();
    h.metrics.observe_store(|| StoreStats {
        hits: 7,
        misses: 2,
        evictions: 3,
        used_memory: 1024,
        max_memory: 4096,
        keys: 5,
    });
    let points = h.points();
    for (name, value) in [
        ("discus.keyspace.hits", 7.0),
        ("discus.keyspace.misses", 2.0),
        ("discus.evictions", 3.0),
        ("discus.memory.used", 1024.0),
        ("discus.memory.limit", 4096.0),
        ("discus.keys", 5.0),
    ] {
        assert_eq!(find(&points, name, None).unwrap().value, value, "{name}");
    }
    assert_bounded(&points);
}

#[test]
fn transport_counters_export() {
    let h = MetricHarness::new();
    h.metrics
        .observe_transport(TransportChannel::Gossip, || TransportCounts {
            frames_rejected: 1,
            unsupported: 2,
            handshake_failures: 3,
            requests_in: 4,
            requests_out: 5,
        });
    h.metrics
        .observe_transport(TransportChannel::Rpc, || TransportCounts {
            frames_rejected: 10,
            unsupported: 20,
            handshake_failures: 30,
            requests_in: 40,
            requests_out: 50,
        });
    let points = h.points();
    for (name, gossip, rpc) in [
        ("discus.transport.frames.rejected", 1.0, 10.0),
        ("discus.transport.unsupported", 2.0, 20.0),
        ("discus.transport.handshake.failures", 3.0, 30.0),
        ("discus.transport.requests.in", 4.0, 40.0),
        ("discus.transport.requests.out", 5.0, 50.0),
    ] {
        let of = |channel| {
            find(&points, name, Some(("channel", channel)))
                .unwrap()
                .value
        };
        assert_eq!(of("gossip"), gossip, "{name}");
        assert_eq!(of("rpc"), rpc, "{name}");
        assert_eq!(
            points.iter().filter(|p| p.name == name).count(),
            2,
            "{name}"
        );
    }
    assert_bounded(&points);
}

#[test]
fn peer_events_carry_the_channel() {
    let h = MetricHarness::new();
    let peer = NodeLabel::new("ab").unwrap();
    h.metrics
        .quic_failure(&peer, Status::Reset, TransportChannel::Gossip);
    h.metrics
        .peer_error(&peer, Status::Timeout, TransportChannel::Rpc);
    let points = h.points();
    let q = find(&points, "discus.quic.failures", Some(("channel", "gossip")));
    assert_eq!(q.unwrap().value, 1.0);
    let e = find(&points, "discus.peer.errors", Some(("channel", "rpc")));
    assert_eq!(e.unwrap().value, 1.0);
    assert_bounded(&points);
}

#[test]
fn later_plan_instruments_record_with_bounded_attributes() {
    let h = MetricHarness::new();
    let peer = NodeLabel::new("ab").unwrap();
    let m = &h.metrics;
    m.epoch(4);
    m.members(MemberState::Normal, 3);
    m.replication_depth(&peer, 10);
    m.replication_lag(&peer, Duration::from_millis(5));
    m.hint_recorded(&peer);
    m.hint_replayed(&peer, 2);
    m.hint_evicted(&peer, 1);
    m.gossip_rtt(&peer, Duration::from_millis(1));
    m.peer_error(&peer, Status::Timeout, TransportChannel::Rpc);
    m.forward(CommandLabel::new("GET"), Status::Ok, 1);
    m.quic_failure(&peer, Status::Handshake, TransportChannel::Rpc);
    m.tombstones(9);
    let points = h.points();
    for name in [
        "discus.ring.epoch",
        "discus.members",
        "discus.replication.queue.depth",
        "discus.replication.lag",
        "discus.hints.recorded",
        "discus.hints.replayed",
        "discus.hints.evicted",
        "discus.gossip.rtt",
        "discus.peer.errors",
        "discus.forwards",
        "discus.forward.hops",
        "discus.quic.failures",
        "discus.tombstones",
    ] {
        assert!(find(&points, name, None).is_some(), "{name}");
    }
    assert_eq!(find(&points, "discus.ring.epoch", None).unwrap().value, 4.0);
    let rtt = find(&points, "discus.gossip.rtt", None).unwrap();
    assert_eq!(rtt.bounds, LATENCY_BUCKETS_MS);
    assert_bounded(&points);
}

#[test]
fn telemetry_counters_are_exported() {
    let h = MetricHarness::new();
    h.counters
        .spans_dropped
        .fetch_add(3, std::sync::atomic::Ordering::Relaxed);
    let points = h.points();
    let dropped = find(&points, "discus.telemetry.spans.dropped", None).unwrap();
    assert_eq!(dropped.value, 3.0);
    for name in [
        "discus.telemetry.spans.export_failures",
        "discus.telemetry.metrics.export_failures",
        "discus.logs.suppressed",
    ] {
        assert!(find(&points, name, None).is_some(), "{name}");
    }
}

#[test]
fn counter_metrics_export() {
    let h = MetricHarness::new();
    let m = &h.metrics;
    m.counter_roles(CounterRole::Leader, 3);
    m.counter_roles(CounterRole::Voter, 2);
    m.counter_memory(4096);
    m.counter_mode_switch(CounterMode::Raft);
    m.counter_failover();
    m.raft_single_voter_groups(1);
    let points = h.points();
    for name in [
        "discus.counter.roles",
        "discus.counter.memory",
        "discus.counter.mode.switches",
        "discus.counter.failovers",
        "discus.raft.single_voter_groups",
    ] {
        assert!(find(&points, name, None).is_some(), "{name}");
    }
    let leader = find(&points, "discus.counter.roles", Some(("state", "leader"))).unwrap();
    assert_eq!(leader.value, 3.0);
    assert_eq!(
        find(&points, "discus.counter.memory", None).unwrap().value,
        4096.0
    );
    assert!(find(
        &points,
        "discus.counter.mode.switches",
        Some(("state", "raft"))
    )
    .is_some());
    assert_bounded(&points);
}

#[test]
fn counter_roles_use_bounded_states() {
    for role in CounterRole::ALL {
        assert!(check([("state", role.as_str())]).is_ok(), "{role:?}");
    }
    for mode in CounterMode::ALL {
        assert!(check([("state", mode.as_str())]).is_ok(), "{mode:?}");
    }
    assert!(check([("state", "group-3")]).is_err());
}

#[test]
fn config_metrics_export() {
    let h = MetricHarness::new();
    let m = &h.metrics;
    m.config_reload(true);
    m.config_reload(true);
    m.config_reload(false);
    m.config_pending(true);
    let points = h.points();
    let applied = find(
        &points,
        "discus.config.reloads",
        Some(("outcome", "applied")),
    )
    .unwrap();
    assert_eq!(applied.value, 2.0);
    let failed = find(
        &points,
        "discus.config.reloads",
        Some(("outcome", "failed")),
    )
    .unwrap();
    assert_eq!(failed.value, 1.0);
    assert_eq!(
        find(&points, "discus.config.pending", None).unwrap().value,
        1.0
    );
    assert_bounded(&points);
}
