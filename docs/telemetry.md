# Telemetry

Discus exports metrics and traces with OpenTelemetry. It sends them over OTLP/gRPC to a collector.
Logs go to stdout as JSON lines.

Telemetry needs the `otel` build feature. The default build has it. Export is off until you pass
`--otel`.

## Turn on export

Start the node with `--otel`. The node then sends metrics and traces to
`http://otel-collector:4317`.

```sh
discus --otel --otel-endpoint http://otel-collector.observability:4317
```

If the binary lacks the `otel` feature, `--otel` stops the start. The node prints this message and
exits with code 2:

```text
discus: telemetry is enabled (--otel) but this binary was built without the "otel" feature
```

Export never blocks a command. If the collector is down or slow, the node keeps serving. It drops
the spans that do not fit in the queue and counts them. At shutdown, the node makes a last export.
That export waits about 5 seconds when the collector does not answer.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--otel` | off | Export metrics and traces. |
| `--otel-endpoint` | `http://otel-collector:4317` | The collector address. `https://` turns on TLS. |
| `--otel-ca-file` | none | A PEM CA bundle for the collector certificate. |
| `--otel-cert-file` | none | A PEM client certificate for mTLS. |
| `--otel-key-file` | none | The PEM private key of the client certificate. |
| `--otel-sampling-ratio` | `0.01` | The fraction of new traces to keep, from 0 to 1. |
| `--otel-export-interval-secs` | `10` | Seconds between two metric exports. |
| `--otel-span-queue` | `2048` | The most finished spans that wait for export. |
| `--log-rate-limit` | `100` | Log lines per second for each place in the code. 0 means no limit. |

The node refuses to start in these cases:

- The sampling ratio is below 0, above 1, or not a number.
- You set `--otel-cert-file` without `--otel-key-file`, or the other way around.
- You set a TLS file, and the endpoint starts with `http://`.
- The endpoint does not start with `http://` or `https://`.
- The export interval or the span queue is 0.

## TLS and mTLS to the collector

An endpoint that starts with `https://` turns on TLS. The node checks the collector certificate
against one set of trust roots:

- With `--otel-ca-file`, the node trusts only that CA bundle.
- Without it, the node uses the trust roots of the build. The `roots-system` feature uses the
  system trust store. The `roots-webpki` feature uses the Mozilla roots that are compiled in.

For mTLS, also pass `--otel-cert-file` and `--otel-key-file`.

The TLS backend feature selects the cryptography. Every backend supports TLS to the collector. The
node installs the provider of its TLS backend as the process-wide `rustls` default before it starts
telemetry. A `tls-openssl` build uses that provider for the collector connection.

## Sampling

Sampling is parent-based. A command that arrives with a trace context follows the decision of its
parent. A new trace is kept with the probability `--otel-sampling-ratio`, which is 1% by default.

The node does not keep every error trace. To keep all of them, set `--otel-sampling-ratio 1` and
sample by tail in the collector.

## Traces

Each command gets a span named `command`. It has these child spans:

- `route` covers the entry checks and the choice of the owner. The entry checks are the lookup,
  the arguments, the authentication, and the scopes.
- `store` covers the work on the store.
- `forward` covers one forward to another node.

The `command` span carries `db.system=redis` and `db.operation.name`, for example `GET`. A span
never carries a key or a value. A command that returns an error sets the span status to error.

Between nodes, the trace context travels in W3C Trace Context form (`traceparent` and
`tracestate`) in each frame. The node that sends a forward puts the context of its `forward` span
in the frame. The node that receives it opens a `command` span with that context as its parent.
That span also carries `discus.forwarded=true`. So one trace ID covers the command on every node.

A build without `otel` creates no spans, and neither does a node without `--otel`. Such a node
forwards the trace context that it receives without a change, so a trace stays connected across
that node. A node drops a malformed `traceparent` and its `tracestate`, and never forwards them.

## Metrics

The node exports metrics every 10 seconds. A metric attribute is always one of these five:
`command`, `status`, `node`, `peer`, and `state`. A metric never carries a key, a value, a client
ID, a connection ID, or an error message.

The `command` attribute is the command name from the command table. An unknown command records
`unknown`. A command that Discus rejects on purpose records `unsupported`.

Latency histograms use these bucket bounds in milliseconds: 0.1, 0.25, 0.5, 1, 2, 5, 10, 25, 50,
and 100.

| Metric | Type | Attributes | Meaning |
| --- | --- | --- | --- |
| `discus.commands` | counter | `command`, `status` | Commands, with `status` `ok` or `error`. |
| `discus.command.duration` | histogram (ms) | `command` | Command latency. |
| `discus.keyspace.hits` | counter | none | Reads that found a live key. |
| `discus.keyspace.misses` | counter | none | Reads that found no live key. |
| `discus.evictions` | counter | none | Keys evicted under memory pressure. |
| `discus.memory.used` | gauge (bytes) | none | Memory that the store uses. |
| `discus.memory.limit` | gauge (bytes) | none | The memory budget, `--max-memory`. |
| `discus.keys` | gauge | none | Stored keys. |
| `discus.auth.attempts` | counter | `status` | Results of `AUTH` and `HELLO ... AUTH`, `success` or `failure`. |
| `discus.telemetry.spans.dropped` | counter | none | Spans dropped because the queue was full. |
| `discus.telemetry.spans.export_failures` | counter | none | Failed span exports. |
| `discus.telemetry.metrics.export_failures` | counter | none | Failed metric exports. |
| `discus.logs.suppressed` | counter | none | Log lines dropped by the rate limit. |
| `discus.ring.epoch` | gauge | none | The generation of the current epoch. |
| `discus.members` | gauge | `state` | Members of the current epoch in each state. |
| `discus.forwards` | counter | `command`, `status` | Forwards to other nodes, with `status` `ok`, `timeout`, or `error`. |
| `discus.forward.hops` | histogram | none | The hop count of each forward, 1 or 2. |
| `discus.peer.errors` | counter | `peer`, `status` | Requests to a peer without a reply in time, with `status` `timeout`. |
| `discus.quic.failures` | counter | `peer`, `status` | Failed handshakes (`handshake`) and lost connections (`reset`). |
| `discus.transport.frames.rejected` | counter | none | Node-to-node frames that failed the verifier. |
| `discus.transport.unsupported` | counter | none | Requests of a message type that this build does not know. |
| `discus.transport.handshake.failures` | counter | none | Failed QUIC handshakes and `HELLO` exchanges. |
| `discus.transport.requests.in` | counter | none | Node-to-node requests that this node served. |
| `discus.transport.requests.out` | counter | none | Node-to-node requests that this node sent. |
| `discus.replication.queue.depth` | gauge | `peer` | Keys that wait in the replication queue of a peer. |
| `discus.replication.lag` | gauge (ms) | `peer` | The time that the last replication batch to a peer took. |
| `discus.hints.recorded` | counter | `peer` | Hints that the primary recorded for a peer that it cannot reach. |
| `discus.hints.replayed` | counter | `peer` | Hints that a peer acknowledged after a replay. |
| `discus.hints.evicted` | counter | `peer` | Hints lost to a bound before a replay. |
| `discus.gossip.rtt` | histogram (ms) | `peer` | The round-trip time of gossip requests. |
| `discus.tombstones` | gauge | none | Stored tombstones (delete markers). |

The `peer` attribute is the node ID of the peer in hex. A transport event without a known peer is
not counted.

## Resource attributes

Every metric and span carries these resource attributes:

| Attribute | Value |
| --- | --- |
| `service.name` | `discus` |
| `service.instance.id` | The node ID: the value of `--node-id`, else a new ID at each process start. |
| `discus.ring.epoch` | The epoch at startup. The `discus.ring.epoch` gauge has the live value. |
| `k8s.namespace.name` | From the `KUBERNETES_NAMESPACE` environment variable. |
| `k8s.pod.name` | From the `POD_NAME` environment variable. |
| `k8s.pod.uid` | From the `POD_UID` environment variable. |
| `k8s.node.name` | From the `NODE_NAME` environment variable. |

The node skips a Kubernetes attribute when its variable is not set. Set the variables with the
Kubernetes Downward API:

```yaml
env:
  - name: KUBERNETES_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: POD_UID
    valueFrom:
      fieldRef:
        fieldPath: metadata.uid
  - name: NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
```

The standard `OTEL_RESOURCE_ATTRIBUTES` variable adds more attributes.

## Logs

The node writes one JSON object per line to stdout. `RUST_LOG` sets the level, and the default is
`info`. OTLP log export is not part of this release.

Each line has these fields:

| Field | Meaning |
| --- | --- |
| `timestamp` | The time in UTC, in RFC 3339 form. |
| `level` | `ERROR`, `WARN`, `INFO`, `DEBUG`, or `TRACE`. |
| `target` | The module that wrote the line. |
| `fields` | The message and the values of the line. |
| `trace_id` | The 32-digit trace ID. Only when the line comes from inside a span. |
| `span_id` | The 16-digit span ID. Only when the line comes from inside a span. |

With `--otel`, the trace ID in a log line is the same as the trace ID in the exported trace. Use it
to find the trace of a log line.

### Rate limit

Each place in the code that writes log lines can write at most `--log-rate-limit` lines per second.
The default is 100. The node drops the lines over the limit and counts them in
`discus.logs.suppressed`. A noisy place in the code does not stop other places from logging. Set
`--log-rate-limit 0` to turn the limit off.
