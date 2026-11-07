use discus_telemetry::context::{outbound, set_parent, TraceContext, TraceParent};

const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[test]
fn traceparent_roundtrips() {
    let tp = TraceParent::parse(TP).unwrap();
    assert!(tp.is_sampled());
    assert_eq!(tp.to_string(), TP);
    let unsampled = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    assert!(!TraceParent::parse(unsampled).unwrap().is_sampled());
}

#[test]
fn malformed_traceparent_is_rejected() {
    for bad in [
        "",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        "00_4bf92f3577b34da6a3ce929d0e0e4736_00f067aa0ba902b7_01",
    ] {
        assert!(TraceParent::parse(bad).is_none(), "{bad}");
        let cx = TraceContext::from_pairs([("traceparent", bad), ("tracestate", "a=1")]);
        assert!(cx.is_empty(), "{bad}");
    }
}

#[test]
fn carrier_roundtrips_through_pairs() {
    let cx = TraceContext::from_pairs([("traceparent", TP), ("tracestate", "congo=t61rcWkgMzE")]);
    assert!(!cx.is_empty());
    let pairs = cx.to_pairs();
    let back = TraceContext::from_pairs(pairs.iter().map(|(k, v)| (*k, v.as_str())));
    assert_eq!(back, cx);
}

#[test]
fn unknown_pairs_are_ignored() {
    let cx = TraceContext::from_pairs([("traceparent", TP), ("baggage", "x=1")]);
    assert_eq!(cx.traceparent, TP);
    assert_eq!(cx.tracestate, "");
}

#[cfg(not(feature = "otel"))]
#[test]
fn without_otel_the_context_passes_through_unchanged() {
    let inbound = TraceContext::from_pairs([("traceparent", TP), ("tracestate", "k=v")]);
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
    let inbound = TraceContext::from_pairs([("traceparent", TP), ("tracestate", "k=v")]);
    let out = tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("forward");
        set_parent(&span, &inbound);
        let _e = span.enter();
        outbound(&inbound)
    });
    let sent = TraceParent::parse(TP).unwrap();
    let got = TraceParent::parse(&out.traceparent).unwrap();
    assert_eq!(got.trace_id, sent.trace_id);
    assert_ne!(got.parent_id, sent.parent_id);
    assert!(got.is_sampled());
    assert_eq!(out.tracestate, "k=v");
}

#[cfg(feature = "otel")]
#[test]
fn with_otel_and_no_active_span_the_inbound_context_is_kept() {
    let inbound = TraceContext::from_pairs([("traceparent", TP)]);
    assert_eq!(outbound(&inbound), inbound);
}
