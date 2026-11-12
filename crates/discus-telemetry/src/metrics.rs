//! The metrics handle (spec §15.4).
//!
//! [`Metrics`] has the same methods in both feature modes. Without `otel`, it is a unit struct and
//! every method does nothing. With `otel`, it records into OpenTelemetry instruments. Every
//! attribute is typed (see [`crate::attrs`]), so a caller cannot attach a key, a value, a client
//! ID, or a raw error.

use std::sync::atomic::AtomicU64;
use std::sync::OnceLock;
use std::time::Duration;

use crate::attrs::{
    CommandLabel, CounterMode, CounterRole, MemberState, MisrouteReason, NodeLabel, Status,
    TopologyCommand, TransportChannel,
};

/// Latency histogram bucket bounds in milliseconds.
pub const LATENCY_BUCKETS_MS: [f64; 10] = [0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0];

/// Forward hop histogram bounds. The hop budget is 2 (spec §7.2).
pub const HOP_BUCKETS: [f64; 3] = [0.0, 1.0, 2.0];

/// A snapshot of the store counters, read at each metric collection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub used_memory: u64,
    pub max_memory: u64,
    pub keys: u64,
}

/// A snapshot of the local health, read at each metric collection.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HealthStats {
    pub lag_ms: u64,
    pub score_ms: f64,
    pub multiplier: f64,
    pub evidence_dropped: u64,
}

/// A snapshot of the transport counters, read at each metric collection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportCounts {
    pub frames_rejected: u64,
    pub unsupported: u64,
    pub handshake_failures: u64,
    pub requests_in: u64,
    pub requests_out: u64,
}

/// Counters that the export pipeline updates and the metrics read.
#[derive(Debug, Default)]
pub struct Counters {
    pub spans_dropped: AtomicU64,
    pub span_export_failures: AtomicU64,
    pub metric_export_failures: AtomicU64,
}

static GLOBAL: OnceLock<Metrics> = OnceLock::new();
static NOOP: Metrics = Metrics::noop();

/// The installed handle, else a handle that records nothing.
pub fn global() -> &'static Metrics {
    GLOBAL.get().unwrap_or(&NOOP)
}

/// Install the process-wide handle. Returns false when one is already installed.
pub fn install_global(metrics: Metrics) -> bool {
    GLOBAL.set(metrics).is_ok()
}

#[cfg(not(feature = "otel"))]
mod imp {
    use super::*;

    /// The metrics handle. Without `otel`, it records nothing.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Metrics;

    #[allow(unused_variables)]
    impl Metrics {
        pub const fn noop() -> Metrics {
            Metrics
        }
        /// Always false: this build has no `otel` feature.
        #[inline]
        pub fn is_enabled(&self) -> bool {
            false
        }
        #[inline]
        pub fn command(&self, cmd: CommandLabel, status: Status, elapsed: Duration) {}
        #[inline]
        pub fn auth(&self, success: bool) {}
        #[inline]
        pub fn observe_store(&self, source: impl Fn() -> StoreStats + Send + Sync + 'static) {}
        #[inline]
        pub fn observe_health(&self, source: impl Fn() -> HealthStats + Send + Sync + 'static) {}
        #[inline]
        pub fn observe_transport(
            &self,
            channel: TransportChannel,
            source: impl Fn() -> TransportCounts + Send + Sync + 'static,
        ) {
        }
        #[inline]
        pub fn epoch(&self, generation: u64) {}
        #[inline]
        pub fn members(&self, state: MemberState, count: u64) {}
        #[inline]
        pub fn replication_depth(&self, peer: &NodeLabel, depth: u64) {}
        #[inline]
        pub fn replication_lag(&self, peer: &NodeLabel, lag: Duration) {}
        #[inline]
        pub fn hint_recorded(&self, peer: &NodeLabel) {}
        #[inline]
        pub fn hint_replayed(&self, peer: &NodeLabel, n: u64) {}
        #[inline]
        pub fn hint_evicted(&self, peer: &NodeLabel, n: u64) {}
        #[inline]
        pub fn gossip_rtt(&self, peer: &NodeLabel, rtt: Duration) {}
        #[inline]
        pub fn peer_error(&self, peer: &NodeLabel, kind: Status, channel: TransportChannel) {}
        #[inline]
        pub fn forward(&self, cmd: CommandLabel, status: Status, hops: u8) {}
        #[inline]
        pub fn redirect(&self) {}
        #[inline]
        pub fn misrouted(&self, reason: MisrouteReason) {}
        #[inline]
        pub fn topology_request(&self, command: TopologyCommand) {}
        #[inline]
        pub fn quic_failure(&self, peer: &NodeLabel, kind: Status, channel: TransportChannel) {}
        #[inline]
        pub fn tombstones(&self, count: u64) {}
        #[inline]
        pub fn primary_read_delete_unacked(&self, count: u64) {}
        #[inline]
        pub fn counter_roles(&self, role: CounterRole, count: u64) {}
        #[inline]
        pub fn counter_memory(&self, bytes: u64) {}
        #[inline]
        pub fn counter_mode_switch(&self, mode: CounterMode) {}
        #[inline]
        pub fn counter_failover(&self) {}
        #[inline]
        pub fn raft_single_voter_groups(&self, count: u64) {}
    }
}

#[cfg(feature = "otel")]
mod imp {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, RwLock};

    use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
    use opentelemetry::{KeyValue, StringValue, Value};

    use super::*;
    use crate::attrs::{check, Attribute};

    type Source = Box<dyn Fn() -> StoreStats + Send + Sync>;
    type HealthSource = Box<dyn Fn() -> HealthStats + Send + Sync>;
    type TransportSource = (
        TransportChannel,
        Box<dyn Fn() -> TransportCounts + Send + Sync>,
    );

    /// The metrics handle. Cheap to clone.
    #[derive(Clone, Default)]
    pub struct Metrics {
        inner: Option<Arc<Instruments>>,
    }

    impl std::fmt::Debug for Metrics {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Metrics")
                .field("enabled", &self.inner.is_some())
                .finish()
        }
    }

    struct Instruments {
        commands: Counter<u64>,
        command_duration: Histogram<f64>,
        auth: Counter<u64>,
        epoch: Gauge<u64>,
        members: Gauge<u64>,
        replication_depth: Gauge<u64>,
        replication_lag: Gauge<f64>,
        hints_recorded: Counter<u64>,
        hints_replayed: Counter<u64>,
        hints_evicted: Counter<u64>,
        gossip_rtt: Histogram<f64>,
        peer_errors: Counter<u64>,
        forwards: Counter<u64>,
        forward_hops: Histogram<u64>,
        redirects: Counter<u64>,
        misrouted: Counter<u64>,
        topology_requests: Counter<u64>,
        quic_failures: Counter<u64>,
        tombstones: Gauge<u64>,
        primary_read_delete_unacked: Counter<u64>,
        counter_roles: Gauge<u64>,
        counter_memory: Gauge<u64>,
        counter_mode_switches: Counter<u64>,
        counter_failovers: Counter<u64>,
        raft_single_voter_groups: Gauge<u64>,
        store: Arc<RwLock<Option<Source>>>,
        transport: Arc<RwLock<Vec<TransportSource>>>,
        health: Arc<RwLock<Option<HealthSource>>>,
        // The observable instruments stay registered while these handles live.
        _observers: Vec<Box<dyn std::any::Any + Send + Sync>>,
    }

    fn kv(a: Attribute) -> KeyValue {
        debug_assert!(check([(a.key(), a.value())]).is_ok(), "{a:?}");
        let key = a.key();
        let value: StringValue = match a {
            Attribute::Command(c) => c.as_str().into(),
            Attribute::Status(s) => s.as_str().into(),
            Attribute::State(s) => s.as_str().into(),
            Attribute::Role(r) => r.as_str().into(),
            Attribute::Mode(m) => m.as_str().into(),
            Attribute::Channel(c) => c.as_str().into(),
            Attribute::Reason(r) => r.as_str().into(),
            Attribute::Topology(t) => t.as_str().into(),
            Attribute::Node(n) | Attribute::Peer(n) => n.shared().into(),
        };
        KeyValue::new(key, Value::String(value))
    }

    fn ms(d: Duration) -> f64 {
        d.as_secs_f64() * 1000.0
    }

    impl Metrics {
        pub const fn noop() -> Metrics {
            Metrics { inner: None }
        }

        /// True when this handle records into instruments.
        pub fn is_enabled(&self) -> bool {
            self.inner.is_some()
        }

        /// Build every instrument on `meter`.
        pub fn new(meter: &Meter, counters: Arc<Counters>) -> Metrics {
            let store: Arc<RwLock<Option<Source>>> = Arc::new(RwLock::new(None));
            let mut observers: Vec<Box<dyn std::any::Any + Send + Sync>> = Vec::new();

            let store_counter =
                |name: &'static str, desc: &'static str, pick: fn(&StoreStats) -> u64| {
                    let store = store.clone();
                    meter
                        .u64_observable_counter(name)
                        .with_description(desc)
                        .with_callback(move |o| {
                            if let Some(f) = store.read().ok().as_ref().and_then(|s| s.as_ref()) {
                                o.observe(pick(&f()), &[]);
                            }
                        })
                        .build()
                };
            observers.push(Box::new(store_counter(
                "discus.keyspace.hits",
                "Reads that found a live key",
                |s| s.hits,
            )));
            observers.push(Box::new(store_counter(
                "discus.keyspace.misses",
                "Reads that found no live key",
                |s| s.misses,
            )));
            observers.push(Box::new(store_counter(
                "discus.evictions",
                "Entries evicted under memory pressure",
                |s| s.evictions,
            )));

            let store_gauge = |name: &'static str,
                               desc: &'static str,
                               unit: &'static str,
                               pick: fn(&StoreStats) -> u64| {
                let store = store.clone();
                meter
                    .u64_observable_gauge(name)
                    .with_description(desc)
                    .with_unit(unit)
                    .with_callback(move |o| {
                        if let Some(f) = store.read().ok().as_ref().and_then(|s| s.as_ref()) {
                            o.observe(pick(&f()), &[]);
                        }
                    })
                    .build()
            };
            observers.push(Box::new(store_gauge(
                "discus.memory.used",
                "Estimated bytes used by the store",
                "By",
                |s| s.used_memory,
            )));
            observers.push(Box::new(store_gauge(
                "discus.memory.limit",
                "The memory budget (maxmemory)",
                "By",
                |s| s.max_memory,
            )));
            observers.push(Box::new(store_gauge(
                "discus.keys",
                "Stored entries",
                "{key}",
                |s| s.keys,
            )));

            let atomic_counter =
                |name: &'static str,
                 desc: &'static str,
                 read: Box<dyn Fn() -> u64 + Send + Sync>| {
                    meter
                        .u64_observable_counter(name)
                        .with_description(desc)
                        .with_callback(move |o| o.observe(read(), &[]))
                        .build()
                };
            let c = counters.clone();
            observers.push(Box::new(atomic_counter(
                "discus.telemetry.spans.dropped",
                "Spans dropped because the export queue was full",
                Box::new(move || c.spans_dropped.load(Ordering::Relaxed)),
            )));
            let c = counters.clone();
            observers.push(Box::new(atomic_counter(
                "discus.telemetry.spans.export_failures",
                "Failed span exports",
                Box::new(move || c.span_export_failures.load(Ordering::Relaxed)),
            )));
            let c = counters;
            observers.push(Box::new(atomic_counter(
                "discus.telemetry.metrics.export_failures",
                "Failed metric exports",
                Box::new(move || c.metric_export_failures.load(Ordering::Relaxed)),
            )));
            observers.push(Box::new(atomic_counter(
                "discus.logs.suppressed",
                "Log lines dropped by the rate limit",
                Box::new(crate::logging::suppressed),
            )));

            let health: Arc<RwLock<Option<HealthSource>>> = Arc::new(RwLock::new(None));
            let health_gauge = |name: &'static str,
                                desc: &'static str,
                                unit: &'static str,
                                pick: fn(&HealthStats) -> f64| {
                let health = health.clone();
                meter
                    .f64_observable_gauge(name)
                    .with_description(desc)
                    .with_unit(unit)
                    .with_callback(move |o| {
                        if let Some(f) = health.read().ok().as_ref().and_then(|s| s.as_ref()) {
                            o.observe(pick(&f()), &[]);
                        }
                    })
                    .build()
            };
            observers.push(Box::new(health_gauge(
                "discus.local_health.lag",
                "Lag of the data runtime",
                "ms",
                |h| h.lag_ms as f64,
            )));
            observers.push(Box::new(health_gauge(
                "discus.local_health.score",
                "Smoothed lag that scales the membership timeouts",
                "ms",
                |h| h.score_ms,
            )));
            observers.push(Box::new(health_gauge(
                "discus.local_health.multiplier",
                "Factor applied to the membership timeouts",
                "1",
                |h| h.multiplier,
            )));
            let evidence = health.clone();
            observers.push(Box::new(
                meter
                    .u64_observable_counter("discus.local_health.evidence_dropped")
                    .with_description("Data-path evidence dropped while the local health was at or above the evidence cutoff")
                    .with_callback(move |o| {
                        if let Some(f) = evidence.read().ok().as_ref().and_then(|s| s.as_ref()) {
                            o.observe(f().evidence_dropped, &[]);
                        }
                    })
                    .build(),
            ));

            let transport: Arc<RwLock<Vec<TransportSource>>> = Arc::new(RwLock::new(Vec::new()));
            let transport_counter =
                |name: &'static str, desc: &'static str, pick: fn(&TransportCounts) -> u64| {
                    let transport = transport.clone();
                    meter
                        .u64_observable_counter(name)
                        .with_description(desc)
                        .with_callback(move |o| {
                            let Ok(sources) = transport.read() else {
                                return;
                            };
                            for (channel, f) in sources.iter() {
                                o.observe(pick(&f()), &[kv(Attribute::Channel(*channel))]);
                            }
                        })
                        .build()
                };
            observers.push(Box::new(transport_counter(
                "discus.transport.frames.rejected",
                "Frames that failed the verifier",
                |t| t.frames_rejected,
            )));
            observers.push(Box::new(transport_counter(
                "discus.transport.unsupported",
                "Requests of a message type this build does not know",
                |t| t.unsupported,
            )));
            observers.push(Box::new(transport_counter(
                "discus.transport.handshake.failures",
                "Failed QUIC handshakes and HELLO exchanges",
                |t| t.handshake_failures,
            )));
            observers.push(Box::new(transport_counter(
                "discus.transport.requests.in",
                "Node-to-node requests served",
                |t| t.requests_in,
            )));
            observers.push(Box::new(transport_counter(
                "discus.transport.requests.out",
                "Node-to-node requests sent",
                |t| t.requests_out,
            )));

            let latency = |name: &'static str, desc: &'static str| {
                meter
                    .f64_histogram(name)
                    .with_description(desc)
                    .with_unit("ms")
                    .with_boundaries(LATENCY_BUCKETS_MS.to_vec())
                    .build()
            };
            let counter = |name: &'static str, desc: &'static str| {
                meter.u64_counter(name).with_description(desc).build()
            };
            let gauge = |name: &'static str, desc: &'static str| {
                meter.u64_gauge(name).with_description(desc).build()
            };

            let instruments = Instruments {
                commands: counter("discus.commands", "Commands by command and status"),
                command_duration: latency("discus.command.duration", "Command latency"),
                auth: counter("discus.auth.attempts", "Authentication attempts by result"),
                epoch: gauge("discus.ring.epoch", "The generation of the current epoch"),
                members: gauge("discus.members", "Members by state"),
                replication_depth: gauge(
                    "discus.replication.queue.depth",
                    "Entries waiting in a replication queue",
                ),
                replication_lag: meter
                    .f64_gauge("discus.replication.lag")
                    .with_description("Replication lag toward a peer")
                    .with_unit("ms")
                    .build(),
                hints_recorded: counter("discus.hints.recorded", "Hints recorded"),
                hints_replayed: counter("discus.hints.replayed", "Hints replayed"),
                hints_evicted: counter("discus.hints.evicted", "Hints evicted"),
                gossip_rtt: latency("discus.gossip.rtt", "Gossip round-trip time"),
                peer_errors: counter("discus.peer.errors", "Data-path errors toward a peer"),
                forwards: counter("discus.forwards", "Forwarded commands"),
                forward_hops: meter
                    .u64_histogram("discus.forward.hops")
                    .with_description("Hops that a forwarded command took")
                    .with_boundaries(HOP_BUCKETS.to_vec())
                    .build(),
                redirects: counter("discus.client.redirects", "MOVED replies"),
                misrouted: counter(
                    "discus.client.misrouted",
                    "Commands forwarded where a MOVED reply was due",
                ),
                topology_requests: counter(
                    "discus.client.topology_requests",
                    "CLUSTER topology requests",
                ),
                quic_failures: counter("discus.quic.failures", "QUIC connection failures"),
                tombstones: gauge("discus.tombstones", "Stored tombstones"),
                primary_read_delete_unacked: counter(
                    "discus.primary_read.delete_unacked",
                    "Deletes of listed keys that replied before a secondary acknowledged them",
                ),
                counter_roles: gauge(
                    "discus.counter.roles",
                    "Counter groups by the role of this node",
                ),
                counter_memory: meter
                    .u64_gauge("discus.counter.memory")
                    .with_description("Bytes of counter state that this node holds")
                    .with_unit("By")
                    .build(),
                counter_mode_switches: counter(
                    "discus.counter.mode.switches",
                    "Counter group mode switches by the new mode",
                ),
                counter_failovers: counter(
                    "discus.counter.failovers",
                    "Leader-follower groups that this node took over from another leader",
                ),
                raft_single_voter_groups: gauge(
                    "discus.raft.single_voter_groups",
                    "Raft groups led by this node that have one voter",
                ),
                store,
                transport,
                health,
                _observers: observers,
            };
            Metrics {
                inner: Some(Arc::new(instruments)),
            }
        }

        fn with(&self, f: impl FnOnce(&Instruments)) {
            if let Some(i) = &self.inner {
                f(i);
            }
        }

        pub fn command(&self, cmd: CommandLabel, status: Status, elapsed: Duration) {
            self.with(|i| {
                let c = kv(Attribute::Command(cmd));
                i.commands
                    .add(1, &[c.clone(), kv(Attribute::Status(status))]);
                i.command_duration.record(ms(elapsed), &[c]);
            });
        }

        pub fn auth(&self, success: bool) {
            let status = if success {
                Status::Success
            } else {
                Status::Failure
            };
            self.with(|i| i.auth.add(1, &[kv(Attribute::Status(status))]));
        }

        /// Read the store counters from `source` at each collection. A later call replaces the
        /// source.
        pub fn observe_store(&self, source: impl Fn() -> StoreStats + Send + Sync + 'static) {
            self.with(|i| {
                if let Ok(mut slot) = i.store.write() {
                    *slot = Some(Box::new(source));
                }
            });
        }

        /// Read the local health from `source` at each collection. A later call replaces the
        /// source.
        pub fn observe_health(&self, source: impl Fn() -> HealthStats + Send + Sync + 'static) {
            self.with(|i| {
                if let Ok(mut slot) = i.health.write() {
                    *slot = Some(Box::new(source));
                }
            });
        }

        /// Read the transport counters of the endpoint `channel` from `source` at each collection.
        /// Each channel reports its own data points. A later call for the same channel replaces
        /// its source.
        pub fn observe_transport(
            &self,
            channel: TransportChannel,
            source: impl Fn() -> TransportCounts + Send + Sync + 'static,
        ) {
            self.with(|i| {
                if let Ok(mut sources) = i.transport.write() {
                    sources.retain(|(c, _)| *c != channel);
                    sources.push((channel, Box::new(source)));
                }
            });
        }

        pub fn epoch(&self, generation: u64) {
            self.with(|i| i.epoch.record(generation, &[]));
        }

        pub fn members(&self, state: MemberState, count: u64) {
            self.with(|i| i.members.record(count, &[kv(Attribute::State(state))]));
        }

        pub fn replication_depth(&self, peer: &NodeLabel, depth: u64) {
            self.with(|i| {
                i.replication_depth
                    .record(depth, &[kv(Attribute::Peer(peer.clone()))])
            });
        }

        pub fn replication_lag(&self, peer: &NodeLabel, lag: Duration) {
            self.with(|i| {
                i.replication_lag
                    .record(ms(lag), &[kv(Attribute::Peer(peer.clone()))])
            });
        }

        pub fn hint_recorded(&self, peer: &NodeLabel) {
            self.with(|i| {
                i.hints_recorded
                    .add(1, &[kv(Attribute::Peer(peer.clone()))])
            });
        }

        pub fn hint_replayed(&self, peer: &NodeLabel, n: u64) {
            self.with(|i| {
                i.hints_replayed
                    .add(n, &[kv(Attribute::Peer(peer.clone()))])
            });
        }

        pub fn hint_evicted(&self, peer: &NodeLabel, n: u64) {
            self.with(|i| i.hints_evicted.add(n, &[kv(Attribute::Peer(peer.clone()))]));
        }

        pub fn gossip_rtt(&self, peer: &NodeLabel, rtt: Duration) {
            self.with(|i| {
                i.gossip_rtt
                    .record(ms(rtt), &[kv(Attribute::Peer(peer.clone()))])
            });
        }

        pub fn peer_error(&self, peer: &NodeLabel, kind: Status, channel: TransportChannel) {
            self.with(|i| {
                i.peer_errors.add(
                    1,
                    &[
                        kv(Attribute::Peer(peer.clone())),
                        kv(Attribute::Status(kind)),
                        kv(Attribute::Channel(channel)),
                    ],
                )
            });
        }

        pub fn forward(&self, cmd: CommandLabel, status: Status, hops: u8) {
            self.with(|i| {
                i.forwards.add(
                    1,
                    &[kv(Attribute::Command(cmd)), kv(Attribute::Status(status))],
                );
                i.forward_hops.record(u64::from(hops), &[]);
            });
        }

        pub fn redirect(&self) {
            self.with(|i| i.redirects.add(1, &[]));
        }

        pub fn misrouted(&self, reason: MisrouteReason) {
            self.with(|i| i.misrouted.add(1, &[kv(Attribute::Reason(reason))]));
        }

        pub fn topology_request(&self, command: TopologyCommand) {
            self.with(|i| {
                i.topology_requests
                    .add(1, &[kv(Attribute::Topology(command))])
            });
        }

        pub fn quic_failure(&self, peer: &NodeLabel, kind: Status, channel: TransportChannel) {
            self.with(|i| {
                i.quic_failures.add(
                    1,
                    &[
                        kv(Attribute::Peer(peer.clone())),
                        kv(Attribute::Status(kind)),
                        kv(Attribute::Channel(channel)),
                    ],
                )
            });
        }

        pub fn tombstones(&self, count: u64) {
            self.with(|i| i.tombstones.record(count, &[]));
        }

        pub fn primary_read_delete_unacked(&self, count: u64) {
            self.with(|i| i.primary_read_delete_unacked.add(count, &[]));
        }

        pub fn counter_roles(&self, role: CounterRole, count: u64) {
            self.with(|i| i.counter_roles.record(count, &[kv(Attribute::Role(role))]));
        }

        pub fn counter_memory(&self, bytes: u64) {
            self.with(|i| i.counter_memory.record(bytes, &[]));
        }

        pub fn counter_mode_switch(&self, mode: CounterMode) {
            self.with(|i| i.counter_mode_switches.add(1, &[kv(Attribute::Mode(mode))]));
        }

        pub fn counter_failover(&self) {
            self.with(|i| i.counter_failovers.add(1, &[]));
        }

        pub fn raft_single_voter_groups(&self, count: u64) {
            self.with(|i| i.raft_single_voter_groups.record(count, &[]));
        }
    }
}

pub use imp::Metrics;
