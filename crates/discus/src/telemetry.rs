//! Telemetry glue for the node: flags, the startup check, the subscriber, and the per-command
//! span and metrics (spec §15.4, §15.5). The telemetry itself lives in `discus-telemetry`.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use discus_resp::{Command, Value};
use discus_telemetry::pipeline::{
    DEFAULT_ENDPOINT, DEFAULT_EXPORT_INTERVAL, DEFAULT_SAMPLING_RATIO, DEFAULT_SPAN_QUEUE,
};
use discus_telemetry::Metrics;

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

/// The node ID for telemetry. Placeholder until the placement plan provides `NodeId`: 32 random
/// hex digits, fixed for the life of the process.
pub fn node_id() -> String {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| format!("{:032x}", fastrand::u128(..)))
        .clone()
}

/// The epoch generation for the resource. Placeholder until the placement plan merges.
pub fn ring_epoch() -> u64 {
    0
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
        let telemetry = discus_telemetry::pipeline::Telemetry::otlp(&pipeline_config(args))
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
fn pipeline_config(args: &TelemetryArgs) -> discus_telemetry::pipeline::TelemetryConfig {
    use discus_telemetry::pipeline::{k8s_from_env, ResourceInfo, TelemetryConfig};
    TelemetryConfig {
        endpoint: args.otel_endpoint.clone(),
        ca_file: args.otel_ca_file.clone(),
        client_identity: args.otel_cert_file.clone().zip(args.otel_key_file.clone()),
        sampling_ratio: args.otel_sampling_ratio,
        export_interval: std::time::Duration::from_secs(args.otel_export_interval_secs),
        span_queue: args.otel_span_queue,
        resource: ResourceInfo {
            node_id: node_id(),
            ring_epoch: ring_epoch(),
            k8s: k8s_from_env(),
        },
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

/// Run one command with its span and metrics, through the global metrics handle.
pub fn dispatch(node: &Node, conn: &mut ConnState, cmd: &Command) -> Value {
    dispatch_with(discus_telemetry::global(), node, conn, cmd)
}

/// Run one command with its span and metrics. When `metrics` records nothing (no `otel` feature,
/// or no `--otel`), this is the plain dispatcher.
pub fn dispatch_with(metrics: &Metrics, node: &Node, conn: &mut ConnState, cmd: &Command) -> Value {
    if !metrics.is_enabled() {
        return crate::command::dispatch(node, conn, cmd);
    }
    #[cfg(feature = "otel")]
    {
        instrumented(metrics, node, conn, cmd)
    }
    #[cfg(not(feature = "otel"))]
    {
        crate::command::dispatch(node, conn, cmd)
    }
}

#[cfg(feature = "otel")]
fn instrumented(metrics: &Metrics, node: &Node, conn: &mut ConnState, cmd: &Command) -> Value {
    use discus_telemetry::attrs::{CommandLabel, Status};
    use tracing::field::Empty;

    let start = std::time::Instant::now();
    let span = tracing::info_span!(
        "command",
        otel.status_code = Empty,
        db.system = "redis",
        db.operation.name = Empty,
    );
    let _entered = span.enter();
    // One node has no router yet. The route span covers the command table lookup, and the
    // transport plan moves the owner computation into it.
    let label = tracing::info_span!("route").in_scope(|| label(cmd));
    span.record("db.operation.name", label.as_str());
    let reply = tracing::info_span!("store").in_scope(|| crate::command::dispatch(node, conn, cmd));
    let failed = matches!(reply, Value::Error(_));
    if failed {
        span.record("otel.status_code", "ERROR");
    }
    let status = if failed { Status::Error } else { Status::Ok };
    metrics.command(label, status, start.elapsed());
    if is_auth(label, cmd) {
        metrics.auth(!failed);
    }

    /// The metric label: the table name, never the name the client sent.
    fn label(cmd: &Command) -> CommandLabel {
        let Some(name) = cmd.first() else {
            return CommandLabel::UNKNOWN;
        };
        if let Some(spec) = crate::command::lookup(name) {
            return CommandLabel::new(spec.name);
        }
        let upper = String::from_utf8_lossy(name).to_ascii_uppercase();
        if crate::command::table::is_unsupported(&upper) {
            CommandLabel::UNSUPPORTED
        } else {
            CommandLabel::UNKNOWN
        }
    }

    /// `AUTH`, or `HELLO` with an `AUTH` option.
    fn is_auth(label: CommandLabel, cmd: &Command) -> bool {
        match label.as_str() {
            "AUTH" => true,
            "HELLO" => cmd.iter().skip(2).any(|a| a.eq_ignore_ascii_case(b"AUTH")),
            _ => false,
        }
    }

    reply
}
