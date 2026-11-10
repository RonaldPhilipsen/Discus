//! Telemetry glue for the node: flags, the startup check, the subscriber, and the per-command
//! span and metrics (spec §15.4, §15.5). The telemetry itself lives in `discus-telemetry`.

use std::path::PathBuf;
use std::sync::Arc;

use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_telemetry::attrs::NodeLabel;
use discus_telemetry::context::{TraceContext, TRACEPARENT, TRACESTATE};
use discus_telemetry::pipeline::{
    ResourceInfo, DEFAULT_ENDPOINT, DEFAULT_EXPORT_INTERVAL, DEFAULT_SAMPLING_RATIO,
    DEFAULT_SPAN_QUEUE,
};
use discus_telemetry::Metrics;

use crate::command::CommandSpec;
use crate::node::{ConnState, Node};
use crate::Config;

/// Telemetry flags. `Config` flattens them.
#[derive(Debug, Clone, clap::Args)]
pub struct TelemetryArgs {
    /// Export metrics and traces over OTLP/gRPC. Needs the `otel` build feature.
    #[arg(long)]
    pub otel: bool,
    /// The OTLP/gRPC collector endpoint. An https endpoint turns on TLS.
    #[arg(long, default_value = DEFAULT_ENDPOINT)]
    pub otel_endpoint: String,
    /// A PEM CA bundle for the collector. It replaces the trust roots of the build.
    #[arg(long)]
    pub otel_ca_file: Option<PathBuf>,
    /// A PEM client certificate for mTLS to the collector. Needs --otel-key-file.
    #[arg(long)]
    pub otel_cert_file: Option<PathBuf>,
    /// The PEM private key for --otel-cert-file.
    #[arg(long)]
    pub otel_key_file: Option<PathBuf>,
    /// The fraction of root spans to sample, from 0 to 1. Child spans follow their parent.
    #[arg(long, default_value_t = DEFAULT_SAMPLING_RATIO)]
    pub otel_sampling_ratio: f64,
    /// Seconds between metric exports.
    #[arg(long, default_value_t = DEFAULT_EXPORT_INTERVAL.as_secs())]
    pub otel_export_interval_secs: u64,
    /// The most finished spans that wait for export. More are dropped and counted.
    #[arg(long, default_value_t = DEFAULT_SPAN_QUEUE)]
    pub otel_span_queue: usize,
    /// Log lines per second per call site. 0 is unlimited.
    #[arg(long, default_value_t = 100)]
    pub log_rate_limit: u32,
}

impl TelemetryArgs {
    /// Check the flags. `otel_compiled` is true when the binary has the `otel` feature.
    pub fn validate(&self, otel_compiled: bool) -> Result<(), String> {
        if self.otel && !otel_compiled {
            return Err(
                "telemetry is enabled (--otel) but this binary was built without the \"otel\" \
                 feature"
                    .into(),
            );
        }
        let ratio = self.otel_sampling_ratio;
        if !(0.0..=1.0).contains(&ratio) {
            return Err(format!(
                "--otel-sampling-ratio must be from 0 to 1, not {ratio}"
            ));
        }
        if self.otel_cert_file.is_some() != self.otel_key_file.is_some() {
            return Err("--otel-cert-file and --otel-key-file must be set together".into());
        }
        let https = self.otel_endpoint.starts_with("https://");
        if !https && !self.otel_endpoint.starts_with("http://") {
            return Err(format!(
                "--otel-endpoint must start with http:// or https://, not {:?}",
                self.otel_endpoint
            ));
        }
        let tls_files = self.otel_ca_file.is_some() || self.otel_cert_file.is_some();
        if tls_files && !https {
            return Err("TLS files for the collector need an https:// --otel-endpoint".into());
        }
        if self.otel_export_interval_secs == 0 {
            return Err("--otel-export-interval-secs must be at least 1".into());
        }
        if self.otel_span_queue == 0 {
            return Err("--otel-span-queue must be at least 1".into());
        }
        Ok(())
    }
}

/// The node ID for telemetry: the `NodeId` in 32 lower case hex digits.
pub fn node_id(id: NodeId) -> String {
    id.to_string()
}

/// The metric label of a node: its `NodeId` in hex.
pub fn node_label(id: NodeId) -> Option<NodeLabel> {
    NodeLabel::new(&node_id(id)).ok()
}

/// The resource attributes of the node that `config` starts. Call `Config::resolve_identity`
/// first, so the node ID is the one the node uses. The epoch is the first one, because a
/// resource cannot change. The live value is the gauge.
pub fn resource(config: &Config) -> ResourceInfo {
    ResourceInfo {
        node_id: node_id(config.node_id.unwrap_or_default()),
        ring_epoch: discus_placement::FIRST_GENERATION,
        k8s: discus_telemetry::pipeline::k8s_from_env(),
    }
}

/// Keeps the export pipeline alive. Dropping it flushes and stops the pipeline.
#[derive(Default)]
pub struct TelemetryGuard {
    #[cfg(feature = "otel")]
    telemetry: Option<discus_telemetry::pipeline::Telemetry>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some(t) = &self.telemetry {
            t.shutdown();
        }
    }
}

/// Check the flags, install the global subscriber, and with `--otel` start the export pipeline
/// and install the global metrics handle. Call it once, inside the Tokio runtime.
pub fn init(config: &Config) -> Result<TelemetryGuard, String> {
    use tracing_subscriber::layer::SubscriberExt;

    let args = &config.telemetry;
    args.validate(cfg!(feature = "otel"))?;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let (logs, slot) =
        discus_telemetry::logging::json_layer(std::io::stdout, filter, args.log_rate_limit);

    #[cfg(feature = "otel")]
    if args.otel {
        use tracing_subscriber::Layer;
        let telemetry = discus_telemetry::pipeline::Telemetry::otlp(&pipeline_config(config))
            .map_err(|e| e.to_string())?;
        // Only Discus spans become OpenTelemetry spans. The exporter's own crates (tonic, h2)
        // stay out, so an export never creates spans to export.
        let targets =
            tracing_subscriber::filter::Targets::new().with_target("discus", tracing::Level::INFO);
        let subscriber = tracing_subscriber::registry()
            .with(logs)
            .with(telemetry.layer().with_filter(targets));
        install(tracing::Dispatch::new(subscriber), &slot)?;
        discus_telemetry::install_global(telemetry.metrics());
        return Ok(TelemetryGuard {
            telemetry: Some(telemetry),
        });
    }

    install(
        tracing::Dispatch::new(tracing_subscriber::registry().with(logs)),
        &slot,
    )?;
    Ok(TelemetryGuard::default())
}

/// Make `dispatch` the global subscriber, and give the log formatter its handle for trace IDs.
fn install(
    dispatch: tracing::Dispatch,
    slot: &discus_telemetry::logging::DispatchSlot,
) -> Result<(), String> {
    let _ = slot.set(dispatch.downgrade());
    tracing::dispatcher::set_global_default(dispatch).map_err(|e| e.to_string())
}

#[cfg(feature = "otel")]
fn pipeline_config(config: &Config) -> discus_telemetry::pipeline::TelemetryConfig {
    use discus_telemetry::pipeline::TelemetryConfig;
    let args = &config.telemetry;
    TelemetryConfig {
        endpoint: args.otel_endpoint.clone(),
        ca_file: args.otel_ca_file.clone(),
        client_identity: args.otel_cert_file.clone().zip(args.otel_key_file.clone()),
        sampling_ratio: args.otel_sampling_ratio,
        export_interval: std::time::Duration::from_secs(args.otel_export_interval_secs),
        span_queue: args.otel_span_queue,
        resource: resource(config),
    }
}

/// Report the store counters of `node` through the global metrics handle.
pub fn observe_store(node: &Arc<Node>) {
    observe_store_on(discus_telemetry::global(), node);
}

/// Report the store counters of `node` through `metrics`.
pub fn observe_store_on(metrics: &Metrics, node: &Arc<Node>) {
    let store = node.store.clone();
    let max_memory = node.config.max_memory;
    metrics.observe_store(move || discus_telemetry::metrics::StoreStats {
        hits: store.hits(),
        misses: store.misses(),
        evictions: store.evictions(),
        used_memory: store.used_memory(),
        max_memory,
        keys: store.len() as u64,
    });
}

/// Report the local health of `membership` through `metrics`.
pub fn observe_health(metrics: &Metrics, membership: &Arc<crate::membership::Membership>) {
    let membership = membership.clone();
    metrics.observe_health(move || membership.health());
}

/// Report the transport counters of `transports` through `metrics`. The metrics carry no
/// endpoint attribute, so they are the sums over the gossip and RPC endpoints.
pub fn observe_transport(metrics: &Metrics, transports: &[discus_transport::Transport]) {
    let transports = transports.to_vec();
    metrics.observe_transport(move || {
        let mut sum = discus_telemetry::metrics::TransportCounts::default();
        for t in &transports {
            let s = t.stats().snapshot();
            sum.frames_rejected += s.frames_rejected;
            sum.unsupported += s.unsupported;
            sum.handshake_failures += s.handshake_failures;
            sum.requests_in += s.requests_in;
            sum.requests_out += s.requests_out;
        }
        sum
    });
}

/// The telemetry of one command, from the entry checks to the reply (spec §15.4). With export
/// off it holds no span, and `route` and `store` only run their closure.
pub struct CommandTrace {
    span: tracing::Span,
    #[cfg(feature = "otel")]
    active: Option<Active>,
}

#[cfg(feature = "otel")]
struct Active {
    metrics: Metrics,
    label: discus_telemetry::attrs::CommandLabel,
    start: std::time::Instant,
}

impl CommandTrace {
    /// Start the telemetry of `cmd`, whose name looked up as `spec`. With an enabled `metrics`,
    /// this opens the command span.
    pub fn begin(metrics: &Metrics, spec: Option<&CommandSpec>, cmd: &Command) -> CommandTrace {
        #[cfg(feature = "otel")]
        if metrics.is_enabled() {
            let label = command_label(spec, cmd);
            let span = tracing::info_span!(
                "command",
                otel.status_code = tracing::field::Empty,
                db.system = "redis",
                db.operation.name = label.as_str(),
            );
            return CommandTrace {
                span,
                active: Some(Active {
                    metrics: metrics.clone(),
                    label,
                    start: std::time::Instant::now(),
                }),
            };
        }
        let _ = (metrics, spec, cmd);
        CommandTrace {
            span: tracing::Span::none(),
            #[cfg(feature = "otel")]
            active: None,
        }
    }

    /// The command span. It is disabled when export is off.
    pub fn span(&self) -> &tracing::Span {
        &self.span
    }

    /// A new `route` child span. The caller runs the entry checks and the owner choice in it,
    /// and drops it before the command runs or forwards. Disabled when export is off.
    pub fn route_span(&self) -> tracing::Span {
        if self.span.is_none() {
            return tracing::Span::none();
        }
        tracing::info_span!(parent: &self.span, "route")
    }

    /// Record the reply: the span status and the command metrics.
    pub fn finish(self, reply: &Value) {
        #[cfg(feature = "otel")]
        if let Some(a) = self.active {
            use discus_telemetry::attrs::Status;
            let failed = matches!(reply, Value::Error(_));
            if failed {
                self.span.record("otel.status_code", "ERROR");
            }
            let status = if failed { Status::Error } else { Status::Ok };
            a.metrics.command(a.label, status, a.start.elapsed());
        }
        let _ = reply;
    }
}

/// The metric label of a command whose name looked up as `spec`: the table name, never the name
/// the client sent.
pub fn command_label(
    spec: Option<&CommandSpec>,
    cmd: &Command,
) -> discus_telemetry::attrs::CommandLabel {
    use discus_telemetry::attrs::CommandLabel;
    if let Some(spec) = spec {
        return CommandLabel::new(spec.name);
    }
    let Some(name) = cmd.first() else {
        return CommandLabel::UNKNOWN;
    };
    let upper = String::from_utf8_lossy(name).to_ascii_uppercase();
    if crate::command::table::is_unsupported(&upper) {
        CommandLabel::UNSUPPORTED
    } else {
        CommandLabel::UNKNOWN
    }
}

/// Run one command on this node with its span and metrics: the entry checks, then local
/// execution, as on a node without peers. The connection loop uses `CommandTrace` directly,
/// because it also forwards. When `metrics` records nothing, no span opens.
pub fn dispatch_with(metrics: &Metrics, node: &Node, conn: &mut ConnState, cmd: &Command) -> Value {
    let spec = crate::command::lookup(&cmd[0]);
    let trace = CommandTrace::begin(metrics, spec, cmd);
    let span = trace.span().clone();
    let _entered = span.enter();
    let prepared = trace
        .route_span()
        .in_scope(|| crate::cluster::prepare(node, conn, spec, cmd.clone()));
    let value = match prepared {
        crate::cluster::Prepared::Done(value) => value,
        crate::cluster::Prepared::Data(job) => {
            crate::cluster::local(node, job.spec, &job.cmd).value
        }
    };
    trace.finish(&value);
    value
}

/// The wire form of a trace context, as the telemetry carrier. An invalid `traceparent` is
/// dropped with its `tracestate`, so it is never forwarded.
pub fn from_wire(wire: &discus_wire::TraceContext) -> TraceContext {
    let parent = wire.traceparent.as_deref().map(|v| (TRACEPARENT, v));
    let state = wire.tracestate.as_deref().map(|v| (TRACESTATE, v));
    TraceContext::from_pairs(parent.into_iter().chain(state))
}

/// The telemetry carrier in its wire form.
pub fn to_wire(cx: &TraceContext) -> discus_wire::TraceContext {
    let mut wire = discus_wire::TraceContext::default();
    for (name, value) in cx.to_pairs() {
        match name {
            TRACEPARENT => wire.traceparent = Some(value),
            TRACESTATE => wire.tracestate = Some(value),
            _ => {}
        }
    }
    wire
}

/// The trace context for a forward (spec §15.4). With `otel`, it carries the context of the
/// current span when that span records. Otherwise the inbound context passes through unchanged.
pub fn outbound(inbound: &discus_wire::TraceContext) -> discus_wire::TraceContext {
    to_wire(&discus_telemetry::context::outbound(&from_wire(inbound)))
}

/// Child spans of the current span. Each opens only under a command span, so a node without
/// export opens no span.
macro_rules! child_spans {
    ($($(#[$doc:meta])* $fn:ident => $name:literal;)+) => {
        $(
            $(#[$doc])*
            pub fn $fn() -> tracing::Span {
                if tracing::Span::current().is_none() {
                    return tracing::Span::none();
                }
                tracing::info_span!($name)
            }
        )+
    };
}

child_spans! {
    /// The `forward` span: one forward to another node.
    forward_span => "forward";
    /// The `store` span: the command runs on this node.
    store_span => "store";
    /// The `raft` span: a counter command on its Raft leader.
    raft_span => "raft";
    /// The `counter.sync` span: a leader-follower write that waits for its follower.
    counter_sync_span => "counter.sync";
    /// The `route` span. A node that receives a forward routes in it.
    route_span => "route";
}

/// Record one forward: the command, the outcome, and the hop count of the forward.
pub fn record_forward(
    metrics: &Metrics,
    cmd: &Command,
    result: &Result<crate::cluster::Reply, discus_transport::TransportError>,
    hops: u8,
) {
    use discus_telemetry::attrs::Status;
    let status = match result {
        Ok(_) => Status::Ok,
        Err(discus_transport::TransportError::Timeout) => Status::Timeout,
        Err(_) => Status::Error,
    };
    let spec = cmd.first().and_then(|name| crate::command::lookup(name));
    metrics.forward(command_label(spec, cmd), status, hops);
}

/// Record the epoch gauge and the member count of each state.
pub fn record_epoch(metrics: &Metrics, epoch: &discus_placement::Epoch) {
    use discus_placement::MemberState as Placement;
    use discus_telemetry::attrs::MemberState;
    metrics.epoch(epoch.id().generation);
    for state in MemberState::ALL {
        let ours = match state {
            MemberState::Joining => Placement::Joining,
            MemberState::Normal => Placement::Normal,
            MemberState::Leaving => Placement::Leaving,
            MemberState::Dead => Placement::Dead,
        };
        let count = epoch.members().iter().filter(|m| m.state == ours).count();
        metrics.members(state, count as u64);
    }
}

/// Record one transport event. A failed handshake and a lost connection are QUIC failures. A
/// request without a reply in time is a peer error. An event without a known peer has no label,
/// so it is not recorded.
pub fn record_peer_event(metrics: &Metrics, event: &discus_transport::PeerEvent) {
    use discus_telemetry::attrs::Status;
    use discus_transport::PeerEventKind;
    let Some(peer) = event.peer.and_then(node_label) else {
        return;
    };
    match event.kind {
        PeerEventKind::HandshakeFailed(_) => metrics.quic_failure(&peer, Status::Handshake),
        PeerEventKind::Reset(_) => metrics.quic_failure(&peer, Status::Reset),
        PeerEventKind::ForwardTimeout { .. } => metrics.peer_error(&peer, Status::Timeout),
    }
}

/// Record every event of `transport` until `stop`. Membership (plan 3) reads the same events on
/// its own receiver.
pub async fn observe_peer_events(
    metrics: &'static Metrics,
    transport: &discus_transport::Transport,
    stop: tokio_util::sync::CancellationToken,
) {
    use tokio::sync::broadcast::error::RecvError;
    let mut events = transport.events();
    loop {
        let event = tokio::select! {
            _ = stop.cancelled() => return,
            event = events.recv() => event,
        };
        match event {
            Ok(event) => record_peer_event(metrics, &event),
            Err(RecvError::Lagged(missed)) => {
                tracing::debug!(missed, "telemetry missed transport events");
            }
            Err(RecvError::Closed) => return,
        }
    }
}

/// The command span of a command that another node forwarded. Its parent is the span context
/// that the forward carries, so the trace continues across the hop. With export off it is
/// disabled.
pub fn forwarded_span(metrics: &Metrics, inbound: &discus_wire::TraceContext) -> tracing::Span {
    if !metrics.is_enabled() {
        return tracing::Span::none();
    }
    let span = tracing::info_span!(
        "command",
        otel.status_code = tracing::field::Empty,
        db.system = "redis",
        db.operation.name = tracing::field::Empty,
        discus.forwarded = true,
    );
    discus_telemetry::context::set_parent(&span, &from_wire(inbound));
    span
}

/// Record the table name of a forwarded command on the current command span.
pub fn record_operation(spec: Option<&CommandSpec>, cmd: &Command) {
    #[cfg(feature = "otel")]
    tracing::Span::current().record("db.operation.name", command_label(spec, cmd).as_str());
    let _ = (spec, cmd);
}

/// Mark the current command span as failed when the reply is an error.
pub fn record_reply(reply: &Value) {
    if matches!(reply, Value::Error(_)) {
        tracing::Span::current().record("otel.status_code", "ERROR");
    }
}
