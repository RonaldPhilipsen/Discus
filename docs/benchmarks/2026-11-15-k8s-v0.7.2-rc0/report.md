# Benchmark: Discus 0.7.2-rc0 against Valkey Cluster and Valkey Sentinel

Date: 2026-11-15. Cluster: `home-ops`, node `k8s-neo-alpha`. Discus `0.7.2-rc0-pyroscope`,
Valkey 9.

This run follows the [benchmark methodology](../methodology.md). Every connection uses TLS on both
sides.

Discus 0.7.2-rc0 has two changes on top of 0.7.1:

- A replica decodes the entry keys of an inbound replication batch as slices of the received
  frame. Before, it made one allocation and one copy for each key.
- The send loop records the metric `discus.replication.batch.entries` for each batch.

The purpose of this run is to measure the gain of the first change. The last section compares
this run with the [0.7.1 run](../2026-11-15-k8s-v0.7.1/report.md) of the same day.

In forward mode, memtier connects to port 6379 of every pod. The nodes forward the commands that
they do not own. In cluster mode, memtier reads the slot map from port 6380 and sends each command
to the primary of its key.

The Valkey numbers come from the
[TLS run of 2026-11-13](../2026-11-13-k8s-v0.5.0-rc0-tls/report.md). This run did not run Valkey
again.

## Main results

- Discus completed all 72 runs. No GET missed, and memtier got 0 MOVED and 0 ASK replies in the
  cluster-mode runs.
- The key change gives no gain that this benchmark can measure. The SET throughput and the CPU
  profile of SET are the same as for 0.7.1, within the difference between runs.
- On 1 node, Discus did more GET, mixed, and pipelined operations per second than Valkey. In
  forward mode, Discus did 65,000 SET operations per second, and Valkey did 66,500.
- In cluster mode on 3 and 6 nodes, Discus did more GET and pipelined operations per second than
  Valkey Cluster and Valkey Sentinel. Valkey Cluster did more SET operations per second.
- In cluster mode on 3 nodes, Discus did 129,600 SET operations per second. Valkey Cluster did
  157,400, and Valkey Sentinel did 60,400. On 6 nodes, Discus did 226,700, Valkey Cluster did
  279,700, and Valkey Sentinel did 52,000.

## Throughput

Operations per second, median of 3 runs of 30 seconds, rounded to hundreds.

### Throughput, 1 node

| Workload | Discus, forward | Discus, cluster | Valkey |
| --- | ---: | ---: | ---: |
| GET | 72,600 | 77,400 | 67,700 |
| SET | 65,000 | 69,500 | 66,500 |
| Mixed (1 write, 9 reads) | 72,700 | 72,900 | 67,000 |
| Mixed, 16 in flight | 449,300 | 462,400 | 410,300 |

### Throughput, 3 nodes

| Workload | Discus, forward | Discus, cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 203,000 | 193,100 | 174,400 | 66,800 |
| SET | 84,800 | 129,600 | 157,400 | 60,400 |
| Mixed (1 write, 9 reads) | 159,500 | 176,600 | 167,500 | 64,600 |
| Mixed, 16 in flight | 652,800 | 987,000 | 445,000 | 375,100 |

### Throughput, 6 nodes

| Workload | Discus, forward | Discus, cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 186,600 | 344,600 | 313,300 | 61,400 |
| SET | 114,300 | 226,700 | 279,700 | 52,000 |
| Mixed (1 write, 9 reads) | 161,100 | 302,000 | 303,500 | 59,400 |
| Mixed, 16 in flight | 495,100 | 1,631,500 | 735,900 | 337,200 |

The Sentinel columns use the runs with all connections on the primary. With reads on all nodes,
Valkey Sentinel did 189,300 GET/s on 3 nodes and 291,500 on 6 nodes.

The three runs of each Discus workload were within 9% of their median, with one exception. The
pipelined runs on 1 node in cluster mode were within 15%.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.29 | 2.24 | 1.22 | 2.10 | 1.38 | 2.65 |
| SET | 1.41 | 2.37 | 1.35 | 2.30 | 1.40 | 2.72 |
| Mixed (1 write, 9 reads) | 1.25 | 2.13 | 1.25 | 2.08 | 1.38 | 2.67 |
| Mixed, 16 in flight | 3.36 | 4.67 | 3.28 | 4.67 | 3.50 | 5.89 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.38 | 2.64 | 1.43 | 2.51 | 1.55 | 3.02 | 4.29 | 6.11 |
| SET | 4.03 | 7.04 | 2.10 | 3.74 | 1.76 | 2.53 | 4.67 | 6.33 |
| Mixed (1 write, 9 reads) | 1.49 | 7.49 | 1.57 | 2.70 | 1.62 | 3.10 | 4.35 | 6.46 |
| Mixed, 16 in flight | 7.93 | 12.41 | 4.58 | 6.91 | 10.30 | 16.38 | 11.52 | 19.58 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.94 | 8.13 | 1.61 | 2.81 | 1.76 | 3.20 | 9.09 | 12.73 |
| SET | 5.41 | 9.86 | 2.42 | 4.25 | 1.94 | 3.57 | 10.69 | 14.46 |
| Mixed (1 write, 9 reads) | 3.92 | 9.09 | 1.85 | 3.12 | 1.77 | 3.29 | 9.41 | 12.80 |
| Mixed, 16 in flight | 16.89 | 51.71 | 5.60 | 8.45 | 11.84 | 19.58 | 25.73 | 42.49 |

The Sentinel primary takes all 288 or 576 connections, so its latency is high on 3 and 6 nodes.

## CPU per operation

Microseconds of server CPU for each operation: the `usage_seconds` of all server pods together,
divided by the number of operations. The value is the median of the 3 runs.

| Setup | SET | Mixed, 16 in flight |
| --- | ---: | ---: |
| Discus, cluster, 3 nodes | 12.8 | 1.9 |
| Valkey Cluster, 3 nodes | 11.8 | 3.5 |
| Discus, cluster, 6 nodes | 14.8 | 2.3 |
| Valkey Cluster, 6 nodes | 15.1 | 5.2 |

These counters leave out the kernel time for the network (softirq). The methodology explains why
this favors Valkey.

## The effect of the key change

This section compares 0.7.2-rc0 with 0.7.1, because the purpose of the release candidate is to
measure one change. The other sections compare Discus only with Valkey.

### Throughput of SET

SET operations per second in cluster mode, median of 3 runs. The last column is the largest
distance of one run from its median, over both builds.

| Nodes | 0.7.1 | 0.7.2-rc0 | Difference | Largest distance from the median |
| --- | ---: | ---: | ---: | ---: |
| 1 | 70,200 | 69,500 | -1.0% | 3.6% |
| 3 | 131,300 | 129,600 | -1.3% | 5.4% |
| 6 | 224,000 | 226,700 | +1.2% | 2.1% |

Each difference is smaller than the difference between two runs of one build. On 1 node there is
no replication, so the change cannot have an effect there.

### CPU profile of SET

The change removes key allocations inside `decode_frame` on the replicas. The table shows the
share of the CPU of all pods in the Pyroscope profile of each SET run in cluster mode. Each cell
has the three runs.

| Nodes | Part | 0.7.1 | 0.7.2-rc0 |
| --- | --- | --- | --- |
| 3 | `decode_frame` | 2.8%, 2.4%, 3.1% | 3.3%, 3.1%, 2.5% |
| 3 | Allocator inside `decode_frame` | 1.0%, 0.9%, 1.0% | 1.0%, 1.0%, 0.9% |
| 6 | `decode_frame` | 2.7%, 4.2%, 3.7% | 3.7%, 3.6%, 3.4% |
| 6 | Allocator inside `decode_frame` | 1.0%, 1.7%, 1.6% | 1.3%, 1.1%, 1.3% |

The share of `decode_frame` did not go down, and the allocator share inside it did not go down.
The values of the two builds overlap.

The benchmark keys have about 10 bytes. An allocation of that size takes the fast path of the
allocator. The copy of the 256-byte value, which the change keeps, is the larger cost.

The change has one possible cost. The keys hold the read buffer of the stream until the batch is
applied, so the reader can need a new buffer more often. The profile shows no such cost:
`BytesMut::reserve_inner` takes 0.1% to 0.2% of the CPU in both builds.

### Conclusion

The key change has no gain and no cost that this benchmark can measure. It adds a rule that the
compiler does not enforce: a receiver that keeps a decoded key must copy it. Without a gain, the
change is not worth that rule. For this reason, the change did not merge. The tag `v0.7.2-rc0`
keeps the code.

A workload with long keys and small values can show a larger effect. This benchmark does not
measure such a workload.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- The Valkey numbers are two days older than the Discus numbers. Load from other pods on the host
  can differ between the sessions.
- The 0.7.1 runs are from 13:50 to 14:49, and the 0.7.2-rc0 runs are from 17:45 to 18:35. Load
  from other pods on the host can differ between the two sessions.
- 0.7.2-rc0 also records the new batch metric. The comparison with 0.7.1 measures both changes
  together.
- The profile shares come from windows of about 27 seconds. The CPU for each operation from the
  profile differs by up to 30% between runs of one build, so the comparison uses shares.
- The clients present a certificate from a separate client CA. The Valkey runs of 2026-11-13 used
  one CA for nodes and clients. A client connects once, so this does not change the throughput.
- The client used at most 6.0 of its 8 CPU, with one exception. In the second GET run on 6 nodes
  in cluster mode, the counter shows 7.2 CPU. The kernel throttled no client run.
- The kernel throttled the Discus server pods in a few periods only. The most was 19 periods in
  the 3 pipelined forward-mode runs on 6 nodes. No SET run in cluster mode was throttled.

## Files in this directory

- `results.jsonl` has one JSON line per Discus 0.7.2-rc0 run. The fields `start_ms` and `end_ms`
  select the run in Pyroscope.

To run the Discus setups again, run this command from the repository root:

```bash
S=discus-1-0.7.2-rc0-pyro,discus-cluster-1-0.7.2-rc0-pyro
S=$S,discus-3-0.7.2-rc0-pyro,discus-cluster-3-0.7.2-rc0-pyro
S=$S,discus-6-0.7.2-rc0-pyro,discus-cluster-6-0.7.2-rc0-pyro
python3 scripts/bench-k8s.py --systems "$S" --out results.jsonl
```
