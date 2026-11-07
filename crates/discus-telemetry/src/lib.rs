//! Telemetry for Discus: metrics, traces, and logs (spec §15.4).
//!
//! The crate compiles with and without the `otel` feature. Without it, there are no metrics or
//! traces, and the trace context carrier passes the context through unchanged.

use std::sync::atomic::AtomicU64;

pub mod attrs;
pub mod context;
pub mod metrics;
pub mod pipeline;
#[cfg(feature = "otel")]
pub mod processor;
#[cfg(all(feature = "otel", feature = "testing"))]
pub mod testing;

pub use metrics::{global, install_global, Metrics};

/// Log lines dropped by the rate limit since start.
pub static LOGS_SUPPRESSED: AtomicU64 = AtomicU64::new(0);
