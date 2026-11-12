# Benchmark: Discus 0.5.0-rc.2 in forward mode and cluster mode

Date: 2026-11-11. Cluster: `home-ops`. Discus 0.5.0-rc.2, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). The runs took place from 17:46 to
18:36. The Valkey numbers come from the [2026-11-10 report](../2026-11-10-k8s-v0.3.0/report.md).
In Discus 0.5.0-rc.2, a replica applies a replicated entry with one hash-map lookup under the
write lock.

Forward mode and cluster mode have the same meaning as in the
[0.5.0-rc.1 report](../2026-11-11-k8s-v0.5.0-rc.1/report.md). In forward mode, memtier connects to
port 6379 of every pod, and the nodes forward the commands that they do not own. In cluster mode,
memtier reads the slot map from port 6380 and sends each command to the primary of its key.

## Main results

- Discus completed all 72 runs in both modes. No GET missed, and every node reported 1,000,000
  keys after the prefill and after the runs.
- memtier reported 0 MOVED and 0 ASK replies per second in all 36 cluster-mode runs.
- In cluster mode, Discus GET is at the level of Valkey Cluster: 97% with 3 nodes and 96% with 6
  nodes. The target for 6 nodes is 80%. Discus met it.
- In cluster mode, Discus SET did 69% of Valkey Cluster with 3 nodes and 60% with 6 nodes. The
  target is 70%. Discus did not meet it.
- Forward mode does not scale past 3 nodes. Cluster mode doubles the 6-node rate of every workload.
- In cluster mode, Discus did 147% to 459% of the Valkey Sentinel rate with 3 and 6 nodes.

## Throughput

Operations per second, median of 3 runs of 30 seconds. A share is the cluster-mode rate divided by
the Valkey rate.

### Throughput, 1 node

| Workload | Forward | Cluster | Valkey | Share |
| --- | ---: | ---: | ---: | ---: |
| GET | 83,600 | 83,000 | 87,500 | 95% |
| SET | 72,600 | 73,900 | 85,200 | 87% |
| Mixed (1 write, 9 reads) | 79,900 | 81,800 | 86,700 | 94% |
| Mixed, 16 in flight | 408,600 | 432,000 | 669,500 | 65% |

### Throughput, 3 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel | Share of Cluster | Share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 220,700 | 218,900 | 226,700 | 87,000 | 97% | 252% |
| SET | 82,900 | 143,600 | 207,700 | 80,600 | 69% | 178% |
| Mixed (1 write, 9 reads) | 168,200 | 196,400 | 217,900 | 85,100 | 90% | 231% |
| Mixed, 16 in flight | 620,300 | 942,600 | 1,774,200 | 641,800 | 53% | 147% |

### Throughput, 6 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel | Share of Cluster | Share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 199,300 | 398,500 | 413,300 | 86,800 | 96% | 459% |
| SET | 116,800 | 229,000 | 381,200 | 77,900 | 60% | 294% |
| Mixed (1 write, 9 reads) | 168,500 | 329,400 | 403,200 | 83,400 | 82% | 395% |
| Mixed, 16 in flight | 482,200 | 1,635,400 | 3,126,600 | 603,300 | 52% | 271% |

With reads on all nodes, Valkey Sentinel did 243,600 GET/s with 3 nodes and 353,800 with 6 nodes.

The range of the three runs was within 7% of the median for SET and GET, with two exceptions. The
3-node cluster-mode SET runs did 144,000, 143,600, and 126,600, so 61% to 69% of Valkey Cluster.
The 1-node forward-mode GET runs had a range of 10%, from one low run of 75,300.

### Spec targets

| Target | Result | Met |
| --- | ---: | --- |
| Cluster-mode SET with 3 nodes, at least 70% of Valkey Cluster | 69% | No |
| Cluster-mode SET with 6 nodes, at least 70% of Valkey Cluster | 60% | No |
| Cluster-mode GET with 6 nodes, at least 80% of Valkey Cluster | 96% | Yes |

The spec calls these values targets, not gates.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.17 | 1.86 | 1.14 | 1.73 | 1.07 | 2.10 |
| SET | 1.30 | 2.10 | 1.28 | 1.98 | 1.09 | 2.13 |
| Mixed (1 write, 9 reads) | 1.20 | 1.93 | 1.17 | 1.81 | 1.08 | 2.11 |
| Mixed, 16 in flight | 3.68 | 5.21 | 3.52 | 4.58 | 2.25 | 3.13 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.28 | 2.22 | 1.29 | 2.02 | 1.22 | 2.37 | 3.25 | 4.83 |
| SET | 4.09 | 7.01 | 1.95 | 3.44 | 1.34 | 2.02 | 3.52 | 4.61 |
| Mixed (1 write, 9 reads) | 1.41 | 7.07 | 1.44 | 2.32 | 1.27 | 2.43 | 3.31 | 4.58 |
| Mixed, 16 in flight | 8.32 | 12.73 | 4.64 | 6.72 | 2.51 | 4.42 | 7.07 | 8.89 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.78 | 7.42 | 1.40 | 2.27 | 1.36 | 2.42 | 6.46 | 8.83 |
| SET | 5.31 | 9.47 | 2.37 | 4.06 | 1.45 | 2.65 | 7.23 | 9.66 |
| Mixed (1 write, 9 reads) | 3.76 | 8.38 | 1.70 | 2.81 | 1.36 | 2.45 | 6.75 | 9.21 |
| Mixed, 16 in flight | 17.28 | 50.69 | 5.44 | 7.97 | 3.02 | 4.80 | 15.04 | 18.43 |

The Sentinel primary takes all 288 or 576 connections, so its latency is high with 3 and 6 nodes.

## CPU per operation

Microseconds of server CPU for each operation: the `usage_seconds` of all server pods together,
divided by the number of operations.

| Workload | Nodes | Forward | Cluster | Valkey or Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 1 | 5.0 | 6.1 | 6.1 | |
| SET | 1 | 6.8 | 6.6 | 5.9 | |
| GET | 3 | 5.4 | 8.7 | 5.7 | 4.7 |
| SET | 3 | 23.1 | 11.3 | 10.0 | 8.6 |
| Mixed, 16 in flight | 3 | 3.4 | 2.0 | 1.2 | 0.8 |
| GET | 6 | 16.9 | 6.4 | 7.5 | 5.5 |
| SET | 6 | 31.8 | 15.6 | 9.4 | 12.8 |
| Mixed, 16 in flight | 6 | 9.2 | 2.4 | 1.5 | 1.3 |

In forward mode, a SET costs 2 to 3 times the CPU of a Valkey Cluster SET, because most writes take
an extra hop. Cluster mode removes that hop. In cluster mode with 6 nodes, a SET still costs about
1.7 times the CPU of a Valkey Cluster SET.

## Why SET stays behind Valkey Cluster

No server pod and no client pod was throttled. In cluster mode, the server pods used about 0.5 to
0.6 of their 1 CPU during SET. Each connection waits for one reply before it sends the next
request, so the latency sets the rate. In cluster mode, the SET p50 was 0.6 ms to 0.9 ms above
Valkey Cluster.

A SET does not wait for the replicas. The primary replies after it applies the write, and a separate
task sends the write to the replicas. That task and the inbound batches from other primaries run on
the same worker thread as the client connections. In a local profile of cluster-mode SET,
replication took about half of the node CPU. A client reply that waits behind this work gets a
higher latency. To close the SET gap, a change must make that work cheaper or move it out of the way
of the client connections.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- The client CPU went to 6.1 of 8 CPU in one cluster-mode SET run with 6 nodes. That run was the
  fastest of the three, so the client did not set the rate.
- This run did not repeat the scale-out test of the 0.5.0-rc.1 report.

## Files in this directory

`results-discus.jsonl` has one JSON line per Discus run, in forward mode and in cluster mode. The
Valkey results are in the [2026-11-10 directory](../2026-11-10-k8s-v0.3.0/). To run the Discus part
again, with the `home-ops` context and Helm available, run this command from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-11-k8s-v0.5.0-rc.2/results-discus.jsonl \
  --systems discus-cluster-1-0.5.0-rc.2,discus-cluster-3-0.5.0-rc.2,discus-cluster-6-0.5.0-rc.2,discus-1-0.5.0-rc.2,discus-3-0.5.0-rc.2,discus-6-0.5.0-rc.2
```
