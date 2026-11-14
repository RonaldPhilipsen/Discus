//! The metrics handle (spec §15.4).
//!
//! [`Metrics`] has the same methods in both feature modes. Without `otel`, it is a zero-sized
//! struct and every method does nothing. With `otel`, it records into OpenTelemetry instruments. Every
//! attribute is typed (see [`crate::attrs`]), so a caller cannot attach a key, a value, a client
//! ID, or a raw error.

use std::sync::atomic::AtomicU64;
#[cfg(feature = "otel")]
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
#[cfg(feature = "otel")]
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[cfg(feature = "otel")]
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
#[cfg(feature = "otel")]
use opentelemetry::{KeyValue, StringValue, Value};

#[cfg(feature = "otel")]
use crate::attrs::{check, Attribute, ReloadOutcome};
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

/// The metrics handle. Cheap to clone. Without `otel`, it is zero-sized and every method is an
/// empty inline function.
#[derive(Clone, Default)]
#[cfg_attr(not(feature = "otel"), derive(Copy))]
pub struct Metrics {
    #[cfg(feature = "otel")]
    inner: Option<Arc<Instruments>>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

#[cfg(feature = "otel")]
type Source<T> = Box<dyn Fn() -> T + Send + Sync>;

/// One transport source per channel.
#[cfg(feature = "otel")]
type TransportSources = RwLock<Vec<(TransportChannel, Source<TransportCounts>)>>;

/// A replaceable source that observable instruments read at each collection.
#[cfg(feature = "otel")]
struct Slot<T>(RwLock<Option<Source<T>>>);

#[cfg(feature = "otel")]
impl<T> Slot<T> {
    fn new() -> Arc<Slot<T>> {
        Arc::new(Slot(RwLock::new(None)))
    }

    fn get(&self) -> Option<T> {
        self.0.read().ok()?.as_ref().map(|f| f())
    }

    fn set(&self, source: impl Fn() -> T + Send + Sync + 'static) {
        if let Ok(mut slot) = self.0.write() {
            *slot = Some(Box::new(source));
        }
    }
}

#[cfg(feature = "otel")]
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
    databases_dropped: Counter<u64>,
    raft_single_voter_groups: Gauge<u64>,
    config_reloads: Counter<u64>,
    config_pending: Gauge<u64>,
    store: Arc<Slot<StoreStats>>,
    health: Arc<Slot<HealthStats>>,
    transport: Arc<TransportSources>,
}

#[cfg(feature = "otel")]
fn kv(a: Attribute) -> KeyValue {
    debug_assert!(check([(a.key(), a.value())]).is_ok(), "{a:?}");
    let value: StringValue = match a.static_value() {
        Ok(s) => s.into(),
        Err(node) => node.shared().into(),
    };
    KeyValue::new(a.key(), Value::String(value))
}

#[cfg(feature = "otel")]
fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(feature = "otel")]
impl Metrics {
    /// Build every instrument on `meter`. An observable instrument registers its callback with
    /// the meter at `build()`, so nothing needs to keep the returned handle.
    pub fn new(meter: &Meter, counters: Arc<Counters>) -> Metrics {
        let store = Slot::<StoreStats>::new();
        let store_counter =
            |name: &'static str, desc: &'static str, pick: fn(&StoreStats) -> u64| {
                let store = store.clone();
                meter
                    .u64_observable_counter(name)
                    .with_description(desc)
                    .with_callback(move |o| {
                        if let Some(s) = store.get() {
                            o.observe(pick(&s), &[]);
                        }
                    })
                    .build();
            };
        store_counter("discus.keyspace.hits", "Reads that found a live key", |s| {
            s.hits
        });
        store_counter(
            "discus.keyspace.misses",
            "Reads that found no live key",
            |s| s.misses,
        );
        store_counter(
            "discus.evictions",
            "Entries evicted under memory pressure",
            |s| s.evictions,
        );

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
                    if let Some(s) = store.get() {
                        o.observe(pick(&s), &[]);
                    }
                })
                .build();
        };
        store_gauge(
            "discus.memory.used",
            "Estimated bytes used by the store",
            "By",
            |s| s.used_memory,
        );
        store_gauge(
            "discus.memory.limit",
            "The memory budget (maxmemory)",
            "By",
            |s| s.max_memory,
        );
        store_gauge("discus.keys", "Stored entries", "{key}", |s| s.keys);

        let atomic_counter =
            |name: &'static str, desc: &'static str, read: Box<dyn Fn() -> u64 + Send + Sync>| {
                meter
                    .u64_observable_counter(name)
                    .with_description(desc)
                    .with_callback(move |o| o.observe(read(), &[]))
                    .build();
            };
        let c = counters.clone();
        atomic_counter(
            "discus.telemetry.spans.dropped",
            "Spans dropped because the export queue was full",
            Box::new(move || c.spans_dropped.load(Ordering::Relaxed)),
        );
        let c = counters.clone();
        atomic_counter(
            "discus.telemetry.spans.export_failures",
            "Failed span exports",
            Box::new(move || c.span_export_failures.load(Ordering::Relaxed)),
        );
        let c = counters;
        atomic_counter(
            "discus.telemetry.metrics.export_failures",
            "Failed metric exports",
            Box::new(move || c.metric_export_failures.load(Ordering::Relaxed)),
        );
        atomic_counter(
            "discus.logs.suppressed",
            "Log lines dropped by the rate limit",
            Box::new(crate::logging::suppressed),
        );

        let health = Slot::<HealthStats>::new();
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
                    if let Some(h) = health.get() {
                        o.observe(pick(&h), &[]);
                    }
                })
                .build();
        };
        health_gauge(
            "discus.local_health.lag",
            "Lag of the data runtime",
            "ms",
            |h| h.lag_ms as f64,
        );
        health_gauge(
            "discus.local_health.score",
            "Smoothed lag that scales the membership timeouts",
            "ms",
            |h| h.score_ms,
        );
        health_gauge(
            "discus.local_health.multiplier",
            "Factor applied to the membership timeouts",
            "1",
            |h| h.multiplier,
        );
        let evidence = health.clone();
        meter
            .u64_observable_counter("discus.local_health.evidence_dropped")
            .with_description("Data-path evidence dropped while the local health was at or above the evidence cutoff")
            .with_callback(move |o| {
                if let Some(h) = evidence.get() {
                    o.observe(h.evidence_dropped, &[]);
                }
            })
            .build();

        let transport: Arc<TransportSources> = Arc::new(RwLock::new(Vec::new()));
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
                    .build();
            };
        transport_counter(
            "discus.transport.frames.rejected",
            "Frames that failed the verifier",
            |t| t.frames_rejected,
        );
        transport_counter(
            "discus.transport.unsupported",
            "Requests of a message type this build does not know",
            |t| t.unsupported,
        );
        transport_counter(
            "discus.transport.handshake.failures",
            "Failed QUIC handshakes and HELLO exchanges",
            |t| t.handshake_failures,
        );
        transport_counter(
            "discus.transport.requests.in",
            "Node-to-node requests served",
            |t| t.requests_in,
        );
        transport_counter(
            "discus.transport.requests.out",
            "Node-to-node requests sent",
            |t| t.requests_out,
        );

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
            databases_dropped: counter(
                "discus.databases.dropped",
                "Replicated entries dropped because their database is above the local count",
            ),
            raft_single_voter_groups: gauge(
                "discus.raft.single_voter_groups",
                "Raft groups led by this node that have one voter",
            ),
            config_reloads: counter(
                "discus.config.reloads",
                "Reloads of the settings file by outcome",
            ),
            config_pending: gauge(
                "discus.config.pending",
                "1 while a changed setting waits for a restart",
            ),
            store,
            health,
            transport,
        };
        Metrics {
            inner: Some(Arc::new(instruments)),
        }
    }

    #[inline]
    fn with(&self, f: impl FnOnce(&Instruments)) {
        if let Some(i) = &self.inner {
            f(i);
        }
    }
}

#[cfg_attr(not(feature = "otel"), allow(unused_variables))]
impl Metrics {
    pub const fn noop() -> Metrics {
        Metrics {
            #[cfg(feature = "otel")]
            inner: None,
        }
    }

    /// True when this handle records into instruments. Always false without `otel`.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        #[cfg(feature = "otel")]
        return self.inner.is_some();
        #[cfg(not(feature = "otel"))]
        false
    }

    #[inline]
    pub fn command(&self, cmd: CommandLabel, status: Status, elapsed: Duration) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            let c = kv(Attribute::Command(cmd));
            i.commands
                .add(1, &[c.clone(), kv(Attribute::Status(status))]);
            i.command_duration.record(ms(elapsed), &[c]);
        });
    }

    #[inline]
    pub fn auth(&self, success: bool) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            let status = if success {
                Status::Success
            } else {
                Status::Failure
            };
            i.auth.add(1, &[kv(Attribute::Status(status))]);
        });
    }

    /// Read the store counters from `source` at each collection. A later call replaces the
    /// source.
    #[inline]
    pub fn observe_store(&self, source: impl Fn() -> StoreStats + Send + Sync + 'static) {
        #[cfg(feature = "otel")]
        self.with(|i| i.store.set(source));
    }

    /// Read the local health from `source` at each collection. A later call replaces the
    /// source.
    #[inline]
    pub fn observe_health(&self, source: impl Fn() -> HealthStats + Send + Sync + 'static) {
        #[cfg(feature = "otel")]
        self.with(|i| i.health.set(source));
    }

    /// Read the transport counters of the endpoint `channel` from `source` at each collection.
    /// Each channel reports its own data points. A later call for the same channel replaces
    /// its source.
    #[inline]
    pub fn observe_transport(
        &self,
        channel: TransportChannel,
        source: impl Fn() -> TransportCounts + Send + Sync + 'static,
    ) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            if let Ok(mut sources) = i.transport.write() {
                sources.retain(|(c, _)| *c != channel);
                sources.push((channel, Box::new(source)));
            }
        });
    }

    #[inline]
    pub fn epoch(&self, generation: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.epoch.record(generation, &[]));
    }

    #[inline]
    pub fn members(&self, state: MemberState, count: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.members.record(count, &[kv(Attribute::State(state))]));
    }

    #[inline]
    pub fn replication_depth(&self, peer: &NodeLabel, depth: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            i.replication_depth
                .record(depth, &[kv(Attribute::Peer(peer.clone()))])
        });
    }

    #[inline]
    pub fn replication_lag(&self, peer: &NodeLabel, lag: Duration) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            i.replication_lag
                .record(ms(lag), &[kv(Attribute::Peer(peer.clone()))])
        });
    }

    #[inline]
    pub fn hint_recorded(&self, peer: &NodeLabel) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            i.hints_recorded
                .add(1, &[kv(Attribute::Peer(peer.clone()))])
        });
    }

    #[inline]
    pub fn hint_replayed(&self, peer: &NodeLabel, n: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            i.hints_replayed
                .add(n, &[kv(Attribute::Peer(peer.clone()))])
        });
    }

    #[inline]
    pub fn hint_evicted(&self, peer: &NodeLabel, n: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.hints_evicted.add(n, &[kv(Attribute::Peer(peer.clone()))]));
    }

    #[inline]
    pub fn gossip_rtt(&self, peer: &NodeLabel, rtt: Duration) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            i.gossip_rtt
                .record(ms(rtt), &[kv(Attribute::Peer(peer.clone()))])
        });
    }

    #[inline]
    pub fn peer_error(&self, peer: &NodeLabel, kind: Status, channel: TransportChannel) {
        #[cfg(feature = "otel")]
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

    #[inline]
    pub fn forward(&self, cmd: CommandLabel, status: Status, hops: u8) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            i.forwards.add(
                1,
                &[kv(Attribute::Command(cmd)), kv(Attribute::Status(status))],
            );
            i.forward_hops.record(u64::from(hops), &[]);
        });
    }

    #[inline]
    pub fn redirect(&self) {
        #[cfg(feature = "otel")]
        self.with(|i| i.redirects.add(1, &[]));
    }

    #[inline]
    pub fn misrouted(&self, reason: MisrouteReason) {
        #[cfg(feature = "otel")]
        self.with(|i| i.misrouted.add(1, &[kv(Attribute::Reason(reason))]));
    }

    #[inline]
    pub fn topology_request(&self, command: TopologyCommand) {
        #[cfg(feature = "otel")]
        self.with(|i| {
            i.topology_requests
                .add(1, &[kv(Attribute::Topology(command))])
        });
    }

    #[inline]
    pub fn quic_failure(&self, peer: &NodeLabel, kind: Status, channel: TransportChannel) {
        #[cfg(feature = "otel")]
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

    #[inline]
    pub fn tombstones(&self, count: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.tombstones.record(count, &[]));
    }

    #[inline]
    pub fn primary_read_delete_unacked(&self, count: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.primary_read_delete_unacked.add(count, &[]));
    }

    #[inline]
    pub fn counter_roles(&self, role: CounterRole, count: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.counter_roles.record(count, &[kv(Attribute::Role(role))]));
    }

    #[inline]
    pub fn counter_memory(&self, bytes: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.counter_memory.record(bytes, &[]));
    }

    #[inline]
    pub fn counter_mode_switch(&self, mode: CounterMode) {
        #[cfg(feature = "otel")]
        self.with(|i| i.counter_mode_switches.add(1, &[kv(Attribute::Mode(mode))]));
    }

    #[inline]
    pub fn counter_failover(&self) {
        #[cfg(feature = "otel")]
        self.with(|i| i.counter_failovers.add(1, &[]));
    }

    #[inline]
    pub fn database_dropped(&self, n: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.databases_dropped.add(n, &[]));
    }

    #[inline]
    pub fn raft_single_voter_groups(&self, count: u64) {
        #[cfg(feature = "otel")]
        self.with(|i| i.raft_single_voter_groups.record(count, &[]));
    }

    #[inline]
    pub fn config_reload(&self, ok: bool) {
        #[cfg(feature = "otel")]
        {
            let outcome = if ok {
                ReloadOutcome::Applied
            } else {
                ReloadOutcome::Failed
            };
            self.with(|i| i.config_reloads.add(1, &[kv(Attribute::Outcome(outcome))]));
        }
    }

    #[inline]
    pub fn config_pending(&self, pending: bool) {
        #[cfg(feature = "otel")]
        self.with(|i| i.config_pending.record(u64::from(pending), &[]));
    }
}
