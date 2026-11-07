//! Telemetry for Discus: metrics, traces, and logs (spec §15.4).
//!
//! The crate compiles with and without the `otel` feature. Without it, there are no metrics or
//! traces, and the trace context carrier passes the context through unchanged.

pub mod context;
