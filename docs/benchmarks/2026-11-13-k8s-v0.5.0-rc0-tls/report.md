# Benchmark: Discus 0.5.0-rc0 against Valkey Cluster and Valkey Sentinel, with TLS

Date: 2026-11-13. Cluster: `home-ops`, node `k8s-neo-alpha`. Discus `0.5.0-rc0-pyroscope`,
Valkey 9.

This run follows the [benchmark methodology](../methodology.md). Every connection uses TLS on both
sides. Each client presents a certificate, and the server requires it. Valkey also encrypts its
replication, its cluster bus, and Sentinel. Discus always encrypts the traffic between its nodes.
The [TLS section of the methodology](../methodology.md#tls) lists the settings.

Earlier reports ran Valkey without TLS. Discus encrypted its replication, but Valkey did not, so
those reports were not fair to Discus. Their numbers cannot be compared with the numbers here.

In forward mode, memtier connects to port 6379 of every pod. The nodes forward the commands that
they do not own. In cluster mode, memtier reads the slot map from port 6380 and sends each command
to the primary of its key.

## Main results

- Discus completed all 72 runs. No GET missed, and memtier got 0 MOVED and 0 ASK replies in the
  cluster-mode runs.
- On 1 node, Discus did more operations per second than Valkey in all four workloads.
- In cluster mode on 3 and 6 nodes, Discus did more GET and mixed operations per second than
  Valkey Cluster. Valkey Cluster did more SET operations per second.
- With 16 requests in flight, Discus in cluster mode did 1,000,600 operations per second on
  3 nodes, and Valkey Cluster did 445,000. On 6 nodes, Discus did 1,667,300, and Valkey Cluster
  did 735,900.
- Valkey Sentinel sends every request on the primary to one node, so it did the fewest operations
  in every workload on 3 and 6 nodes.
- Forward mode does not scale past 3 nodes. On 6 nodes, cluster mode did 1.9 to 3.3 times the
  forward-mode rate.
- Discus 0.5.0-rc0 fixes a hang in cluster mode with TLS. Before the fix, memtier stopped in the
  mixed workloads on 3 nodes and in SET and the mixed workloads on 6 nodes. In this run, every
  cluster-mode run completed.

## Throughput

Operations per second, median of 3 runs of 30 seconds, rounded to hundreds.

### Throughput, 1 node

| Workload | Discus, forward | Discus, cluster | Valkey |
| --- | ---: | ---: | ---: |
| GET | 73,700 | 78,600 | 67,700 |
| SET | 71,200 | 70,900 | 66,500 |
| Mixed (1 write, 9 reads) | 76,000 | 75,500 | 67,000 |
| Mixed, 16 in flight | 466,100 | 484,400 | 410,300 |

### Throughput, 3 nodes

| Workload | Discus, forward | Discus, cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 204,700 | 197,600 | 174,400 | 66,800 |
| SET | 84,400 | 133,300 | 157,400 | 60,400 |
| Mixed (1 write, 9 reads) | 159,400 | 180,200 | 167,500 | 64,600 |
| Mixed, 16 in flight | 670,100 | 1,000,600 | 445,000 | 375,100 |

### Throughput, 6 nodes

| Workload | Discus, forward | Discus, cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 189,000 | 351,900 | 313,300 | 61,400 |
| SET | 117,600 | 233,900 | 279,700 | 52,000 |
| Mixed (1 write, 9 reads) | 163,000 | 311,100 | 303,500 | 59,400 |
| Mixed, 16 in flight | 510,400 | 1,667,300 | 735,900 | 337,200 |

The Sentinel columns use the runs with all connections on the primary. With reads on all nodes,
Valkey Sentinel did 189,300 GET/s on 3 nodes and 291,500 on 6 nodes.

The three runs of each Discus workload were within 7% of their median.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.24 | 1.88 | 1.25 | 1.90 | 1.38 | 2.65 |
| SET | 1.33 | 1.99 | 1.33 | 2.03 | 1.40 | 2.72 |
| Mixed (1 write, 9 reads) | 1.24 | 1.91 | 1.25 | 1.95 | 1.38 | 2.67 |
| Mixed, 16 in flight | 3.23 | 4.29 | 3.23 | 4.42 | 3.50 | 5.89 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.38 | 2.42 | 1.42 | 2.29 | 1.55 | 3.02 | 4.29 | 6.11 |
| SET | 4.03 | 6.94 | 2.08 | 3.63 | 1.76 | 2.53 | 4.67 | 6.33 |
| Mixed (1 write, 9 reads) | 1.50 | 7.49 | 1.56 | 2.45 | 1.62 | 3.10 | 4.35 | 6.46 |
| Mixed, 16 in flight | 7.74 | 11.84 | 4.51 | 6.75 | 10.30 | 16.38 | 11.52 | 19.58 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 3.07 | 7.97 | 1.59 | 2.77 | 1.76 | 3.20 | 9.09 | 12.73 |
| SET | 5.25 | 9.41 | 2.38 | 4.19 | 1.94 | 3.57 | 10.69 | 14.46 |
| Mixed (1 write, 9 reads) | 3.90 | 9.09 | 1.80 | 2.96 | 1.77 | 3.29 | 9.41 | 12.80 |
| Mixed, 16 in flight | 16.13 | 50.69 | 5.41 | 8.26 | 11.84 | 19.58 | 25.73 | 42.49 |

The Sentinel primary takes all 288 or 576 connections, so its latency is high on 3 and 6 nodes.

## CPU per operation

Microseconds of server CPU for each operation: the `usage_seconds` of all server pods together,
divided by the number of operations. The value is the median of the 3 runs.

| Setup | SET | Mixed, 16 in flight |
| --- | ---: | ---: |
| Discus, cluster, 3 nodes | 12.4 | 2.2 |
| Valkey Cluster, 3 nodes | 11.8 | 3.5 |
| Discus, cluster, 6 nodes | 15.1 | 2.4 |
| Valkey Cluster, 6 nodes | 15.1 | 5.2 |

A SET cost about the same CPU in both systems. In the pipelined mix, a Valkey Cluster operation
cost more CPU than a Discus operation. The busiest Valkey Cluster pod used 0.76 of its 1 CPU in
the pipelined runs on 3 nodes. These counters leave out the kernel time for the network (softirq),
so they do not show whether a pod reached its CPU limit.

On another day, without TLS, Valkey Cluster did 1,723,200 pipelined operations per second on
3 nodes. See the [1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md). With TLS, it did 445,000.
This run did not look for the cause of that difference.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- The Valkey runs and the Discus runs come from two sessions on the same day. Valkey ran from
  14:55 to 16:06, and Discus ran from 20:35 to 21:25. Load from other pods on the host can differ
  between the sessions.
- The client used at most 6.5 of its 8 CPU. The kernel throttled no client run.
- The kernel throttled the Discus server pods in a few periods only. The most was 23 periods in
  the 3 pipelined forward-mode runs on 6 nodes.
- The Discus image has the Pyroscope profiler, and profiling was on in every Discus run.

## Files in this directory

- `results.jsonl` has one JSON line per Discus 0.5.0-rc0 run. `run-2.log` is the log of the
  script for these runs.
- `results-tls.jsonl` has the Valkey runs with TLS. It also has runs of Discus 0.4.9 with TLS,
  in which cluster mode on 3 and 6 nodes hung. `results-plain.jsonl` has runs of Discus 0.4.9
  without TLS. `run-1.log` is the log of the script for both files. This report uses only the
  Valkey runs from this session.

To run the Discus setups again, run this command from the repository root:

```bash
S=discus-1-0.5.0-rc0-pyro,discus-cluster-1-0.5.0-rc0-pyro
S=$S,discus-3-0.5.0-rc0-pyro,discus-cluster-3-0.5.0-rc0-pyro
S=$S,discus-6-0.5.0-rc0-pyro,discus-cluster-6-0.5.0-rc0-pyro
python3 scripts/bench-k8s.py --systems "$S" --out results.jsonl
```
