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

To turn on export with the Helm chart, set the `otel` values:

```sh
helm upgrade <release> oci://ghcr.io/ronaldphilipsen/charts/discus \
  --reuse-values \
  --version <version> \
  --set otel.enabled=true \
  --set otel.endpoint=http://otel-collector.observability:4317
```

For TLS to the collector, use an `https://` endpoint. To trust a private CA, set
`otel.tls.secretName` to a Secret with the key `ca.crt`. For mTLS, also set
`otel.tls.mutual=true`. The Secret must then also have the keys `tls.crt` and `tls.key`.

The chart also includes a Grafana dashboard. To install it with the Grafana Operator, set
`grafanaDashboard.enabled=true` and set `grafanaDashboard.instanceSelector`. The dashboard needs a
metrics store that keeps the OTLP metric names with their dots, for example VictoriaMetrics.

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
- `raft` covers a counter command on its Raft leader: the quorum read or the log write.
- `counter.sync` covers a leader-follower write that waits for its follower.

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
| `discus.client.redirects` | counter | none | `-MOVED` replies that the redirect listener sent. |
| `discus.client.misrouted` | counter | `reason` | Commands that a redirect connection forwarded instead of a `-MOVED` reply. The `reason` is `epoch`, `down`, or `no_address`. |
| `discus.client.topology_requests` | counter | `command` | `CLUSTER` topology requests, with `command` `slots`, `shards`, `nodes`, or `info`. |
| `discus.peer.errors` | counter | `peer`, `status`, `channel` | Requests to a peer without a reply in time, with `status` `timeout`. |
| `discus.quic.failures` | counter | `peer`, `status`, `channel` | Failed handshakes (`handshake`) and lost connections (`reset`). |
| `discus.transport.frames.rejected` | counter | `channel` | Node-to-node frames that failed the verifier. |
| `discus.transport.unsupported` | counter | `channel` | Requests of a message type that this build does not know. |
| `discus.transport.handshake.failures` | counter | `channel` | Failed QUIC handshakes and `HELLO` exchanges. |
| `discus.transport.requests.in` | counter | `channel` | Node-to-node requests that this node served. |
| `discus.transport.requests.out` | counter | `channel` | Node-to-node requests that this node sent. |
| `discus.replication.queue.depth` | gauge | `peer` | Keys that wait in the replication queue of a peer. |
| `discus.replication.lag` | gauge (ms) | `peer` | The time that the last replication batch to a peer took. |
| `discus.hints.recorded` | counter | `peer` | Hints that the primary recorded for a peer that it cannot reach. |
| `discus.hints.replayed` | counter | `peer` | Hints that a peer acknowledged after a replay. |
| `discus.hints.evicted` | counter | `peer` | Hints lost to a bound before a replay. |
| `discus.gossip.rtt` | histogram (ms) | `peer` | The round-trip time of gossip requests. |
| `discus.local_health.lag` | gauge (ms) | none | The lag of the data runtime: how late its 10 ms timer fired, or how long it has not fired. |
| `discus.local_health.score` | gauge (ms) | none | The smoothed lag forecast that sets the multiplier. |
| `discus.local_health.multiplier` | gauge | none | The factor, from 1 to 9, that stretches the probe and suspicion timeouts. |
| `discus.local_health.evidence_dropped` | counter | none | Data-path evidence dropped while the local health was at or above the evidence cutoff (multiplier 1.5). |
| `discus.tombstones` | gauge | none | Stored tombstones (delete markers). |
| `discus.primary_read.delete_unacked` | counter | none | Deleted keys under `--primary-read-prefix` with no secondary acknowledgment in `--replication-timeout-ms`. The delete still returned success. Exported as `discus_primary_read_delete_unacked_total`. |
| `discus.counter.roles` | gauge | `state` | Counter groups by the role of this node: `leader`, `follower`, `voter`, `learner`, or `idle`. |
| `discus.counter.memory` | gauge (bytes) | none | Counter state that this node holds. |
| `discus.counter.mode.switches` | counter | `state` | Mode switches of a counter group by the new mode, `raft` or `leader_follower`. |
| `discus.counter.failovers` | counter | none | Leader-follower groups that this node took over from another leader. |
| `discus.raft.single_voter_groups` | gauge | none | Raft groups that this node leads and that have one voter. If it stays above zero, raise an alert. The next loss then loses the counters of the group. |

The `channel` attribute names the transport endpoint that a metric comes from. A gossip node has
two endpoints, and each reports on its own: `gossip` is the gossip-only endpoint on
`--cluster-listen`, and `rpc` is the RPC-only endpoint on `--rpc-listen`. A static cluster has one
endpoint for both, and it reports as `cluster`. The transport counters have one data point per
channel. They are not summed. A peer error from a gossip suspicion or a gossip transport event
has the channel `gossip`.

The `peer` attribute is the node ID of the peer in hex. A transport event without a known peer is
not counted. The `state` attribute holds a member state, a counter role, or a counter mode. A
metric never carries a counter group number.

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

## CPU profiles

Discus can push CPU profiles to a Pyroscope server. The profiler runs inside the node. It samples
the stacks of all threads and uploads a pprof profile every 10 seconds.

Profiling needs the `pyroscope` build feature. The default build and the published images do not
have it, because the sampling costs CPU. To build an image with it, run this command in the
repository root:

```sh
docker build --build-arg FEATURES=pyroscope -t discus:pyroscope .
```

Start the node with `--pyroscope-url`:

```sh
discus --pyroscope-url http://pyroscope.observability:4040
```

If the binary lacks the `pyroscope` feature, `--pyroscope-url` stops the start. The node prints this
message and exits with code 2:

```text
discus: profiling is enabled (--pyroscope-url) but this binary was built without the "pyroscope" feature
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--pyroscope-url` | none | The Pyroscope server. Profiling is off without it. |
| `--pyroscope-service-name` | `discus` | The `service_name` label of the profiles. |
| `--pyroscope-sample-rate` | `100` | CPU samples per second, from 1 to 1000. |

Each profile has the label `node_id`. In Kubernetes, it also has the labels `namespace`, `pod` and
`node` from the variables of the Helm chart. To turn on profiling with the chart, add the flag to
`extraArgs` and use an image with the feature:

```sh
helm upgrade <release> oci://ghcr.io/ronaldphilipsen/charts/discus \
  --reuse-values \
  --version <version> \
  --set image.repository=<registry>/discus \
  --set image.tag=pyroscope \
  --set-string 'extraArgs[0]=--pyroscope-url=http://pyroscope.observability:4040'
```

The benchmark script `scripts/bench-k8s.py` has the option `--pyroscope-url`, which adds the same
flag. Each result line has `start_ms` and `end_ms`. Use them to select the time range of one run in
Pyroscope.
