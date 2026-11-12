use discus_telemetry::context::{outbound, set_parent, TraceContext};

const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

fn cx(traceparent: &str, tracestate: &str) -> TraceContext {
    TraceContext::new(Some(traceparent.into()), Some(tracestate.into()))
}

#[test]
fn a_valid_context_keeps_both_values() {
    let unsampled = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    for tp in [TP, unsampled] {
        let c = cx(tp, "congo=t61rcWkgMzE");
        assert!(!c.is_empty(), "{tp}");
        assert_eq!(c.traceparent.as_deref(), Some(tp));
        assert_eq!(c.tracestate.as_deref(), Some("congo=t61rcWkgMzE"));
    }
}

#[test]
fn malformed_traceparent_drops_the_whole_context() {
    for bad in [
        "",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0G",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        "00_4bf92f3577b34da6a3ce929d0e0e4736_00f067aa0ba902b7_01",
    ] {
        assert_eq!(cx(bad, "a=1"), TraceContext::default(), "{bad}");
    }
}

#[test]
fn tracestate_without_traceparent_is_dropped() {
    let c = TraceContext::new(None, Some("a=1".into()));
    assert!(c.is_empty());
    assert_eq!(c, TraceContext::default());
}

#[test]
fn an_empty_tracestate_counts_as_none() {
    let c = cx(TP, "");
    assert_eq!(c.traceparent.as_deref(), Some(TP));
    assert_eq!(c.tracestate, None);
    assert_eq!(TraceContext::new(Some(TP.into()), None), c);
}

#[cfg(not(feature = "otel"))]
#[test]
fn without_otel_the_context_passes_through_unchanged() {
    let inbound = cx(TP, "k=v");
    let span = tracing::info_span!("forward");
    set_parent(&span, &inbound);
    let _e = span.enter();
    assert_eq!(outbound(&inbound), inbound);
}

#[cfg(feature = "otel")]
#[test]
fn with_otel_the_outbound_context_continues_the_inbound_trace() {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt;

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
    let inbound = cx(TP, "k=v");
    let out = tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("forward");
        set_parent(&span, &inbound);
        let _e = span.enter();
        outbound(&inbound)
    });
    let got = out.traceparent.as_deref().unwrap();
    assert!(!TraceContext::new(Some(got.into()), None).is_empty());
    // Same trace ID, a new parent ID, and still sampled.
    assert_eq!(got[3..35], TP[3..35]);
    assert_ne!(got[36..52], TP[36..52]);
    assert!(got.ends_with("-01"));
    assert_eq!(out.tracestate.as_deref(), Some("k=v"));
}

#[cfg(feature = "otel")]
#[test]
fn with_otel_and_no_active_span_the_inbound_context_is_kept() {
    let inbound = TraceContext::new(Some(TP.into()), None);
    assert_eq!(outbound(&inbound), inbound);
}
