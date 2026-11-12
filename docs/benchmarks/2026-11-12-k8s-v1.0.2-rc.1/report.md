# Benchmark: Discus 1.0.2-rc.1 in forward mode and cluster mode

Date: 2026-11-12. Cluster: `home-ops`. Discus 1.0.2-rc.1, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). The runs took place from 09:02 to
09:52. The Valkey numbers come from the [2026-11-10 report](../2026-11-10-k8s-v0.3.0/report.md).

This build makes the path of each command cheaper. A read clones no key, and the owner list of a key
needs no allocation. The counter check allocates nothing. Discus orders a pipeline on key hashes.
When no repair can apply, a command skips the repair lookup. The replication sender puts a burst of
writes into one batch. The write set records a write at a lower cost. A replica observes the
hybrid logical clock (HLC) once for each replicated batch.

In forward mode, memtier connects to port 6379 of every pod. The nodes forward the commands that
they do not own. In cluster mode, memtier reads the slot map from port 6380 and sends each command
to the primary of its key. In this chart, port 6380 gives a DNS name for each pod, in the form
`<ip-with-dashes>.discus-peers.discus-bench.svc.cluster.local`. The client pod resolved these
names, so the run used the default chart values.

## Main results

- Discus completed all 72 runs in both modes. No GET missed, and memtier reported 0 MOVED and 0
  ASK replies in all 36 cluster-mode runs.
- In cluster mode, Discus GET did 94% of Valkey Cluster with 3 nodes and 96% with 6 nodes.
- In cluster mode, Discus SET did 67% of Valkey Cluster with 3 nodes and 65% with 6 nodes.
- In cluster mode, Discus did 164% to 459% of the Valkey Sentinel rate with 3 and 6 nodes.
- With 1 node, Discus did 93% to 96% of Valkey without pipelining, and 74% with 16 requests in
  flight.
- Forward mode does not scale past 3 nodes. With 6 nodes, cluster mode does 2 to 3.4 times the
  forward-mode rate.
- In cluster mode with 3 nodes, the pipelined mixed load made two replicas drop about a quarter of
  their keys. The section [Copies per key](#copies-per-key) gives the details.

## Throughput

Operations per second, median of 3 runs of 30 seconds. A share is the Discus rate divided by the
Valkey rate.

### Throughput, 1 node

| Workload | Forward | Cluster | Valkey | Forward share | Cluster share |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 82,000 | 84,300 | 87,500 | 94% | 96% |
| SET | 75,300 | 79,200 | 85,200 | 88% | 93% |
| Mixed (1 write, 9 reads) | 81,200 | 82,300 | 86,700 | 94% | 95% |
| Mixed, 16 in flight | 479,500 | 497,300 | 669,500 | 72% | 74% |

### Throughput, 3 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 221,500 | 214,100 | 226,700 | 87,000 |
| SET | 89,700 | 138,400 | 207,700 | 80,600 |
| Mixed (1 write, 9 reads) | 172,800 | 196,200 | 217,900 | 85,100 |
| Mixed, 16 in flight | 697,100 | 1,050,900 | 1,774,200 | 641,800 |

| Workload | Forward share of Cluster | Cluster share of Cluster | Forward share of Sentinel | Cluster share of Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 98% | 94% | 255% | 246% |
| SET | 43% | 67% | 111% | 172% |
| Mixed (1 write, 9 reads) | 79% | 90% | 203% | 230% |
| Mixed, 16 in flight | 39% | 59% | 109% | 164% |

### Throughput, 6 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 202,800 | 398,100 | 413,300 | 86,800 |
| SET | 126,700 | 247,200 | 381,200 | 77,900 |
| Mixed (1 write, 9 reads) | 172,700 | 331,700 | 403,200 | 83,400 |
| Mixed, 16 in flight | 521,400 | 1,790,900 | 3,126,600 | 603,300 |

| Workload | Forward share of Cluster | Cluster share of Cluster | Forward share of Sentinel | Cluster share of Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 49% | 96% | 234% | 459% |
| SET | 33% | 65% | 163% | 317% |
| Mixed (1 write, 9 reads) | 43% | 82% | 207% | 398% |
| Mixed, 16 in flight | 17% | 57% | 86% | 297% |

With reads on all nodes, Valkey Sentinel did 243,600 GET/s with 3 nodes and 353,800 with 6 nodes.
The tables use the Sentinel runs with all connections on the primary.

The three runs of each Discus workload were within 6% of their median.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.16 | 1.74 | 1.14 | 1.67 | 1.07 | 2.10 |
| SET | 1.26 | 1.99 | 1.24 | 1.86 | 1.09 | 2.13 |
| Mixed (1 write, 9 reads) | 1.18 | 1.87 | 1.15 | 1.81 | 1.08 | 2.11 |
| Mixed, 16 in flight | 3.18 | 4.16 | 3.07 | 4.08 | 2.25 | 3.13 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.27 | 2.24 | 1.29 | 2.04 | 1.22 | 2.37 | 3.25 | 4.83 |
| SET | 3.81 | 6.66 | 1.95 | 3.54 | 1.34 | 2.02 | 3.52 | 4.61 |
| Mixed (1 write, 9 reads) | 1.39 | 6.88 | 1.44 | 2.29 | 1.27 | 2.43 | 3.31 | 4.58 |
| Mixed, 16 in flight | 7.49 | 11.71 | 4.29 | 6.30 | 2.51 | 4.42 | 7.07 | 8.89 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.88 | 7.36 | 1.41 | 2.29 | 1.36 | 2.42 | 6.46 | 8.83 |
| SET | 4.96 | 8.70 | 2.25 | 3.89 | 1.45 | 2.65 | 7.23 | 9.66 |
| Mixed (1 write, 9 reads) | 3.68 | 8.16 | 1.70 | 2.77 | 1.36 | 2.45 | 6.75 | 9.21 |
| Mixed, 16 in flight | 16.13 | 49.92 | 5.05 | 7.52 | 3.02 | 4.80 | 15.04 | 18.43 |

The Sentinel primary takes all 288 or 576 connections, so its latency is high with 3 and 6 nodes.

## CPU per operation

Microseconds of server CPU for each operation: the `usage_seconds` of all server pods together,
divided by the number of operations. The value is the median of the 3 runs.

| Workload | Nodes | Forward | Cluster | Valkey or Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 1 | 5.6 | 5.6 | 6.1 | |
| SET | 1 | 6.6 | 5.2 | 5.9 | |
| Mixed (1 write, 9 reads) | 1 | 5.7 | 5.8 | 5.6 | |
| Mixed, 16 in flight | 1 | 1.2 | 1.2 | 0.8 | |
| GET | 3 | 6.7 | 7.8 | 5.7 | 4.7 |
| SET | 3 | 19.9 | 12.7 | 10.0 | 8.6 |
| Mixed (1 write, 9 reads) | 3 | 10.2 | 9.2 | 7.5 | 7.2 |
| Mixed, 16 in flight | 3 | 2.3 | 1.9 | 1.2 | 0.8 |
| GET | 6 | 17.4 | 7.0 | 7.5 | 5.5 |
| SET | 6 | 30.2 | 12.6 | 9.4 | 12.8 |
| Mixed (1 write, 9 reads) | 6 | 21.5 | 10.3 | 8.1 | 6.4 |
| Mixed, 16 in flight | 6 | 8.4 | 2.3 | 1.5 | 1.3 |

In forward mode, most writes take an extra hop, so a SET costs 2 to 3 times the CPU of a Valkey
Cluster SET. Cluster mode removes that hop. In cluster mode, a SET costs about 1.3 times the CPU of
a Valkey Cluster SET with 3 and 6 nodes.

## Copies per key

| Setup | Nodes | Copies of each key |
| --- | ---: | ---: |
| Discus | 1 | 1 |
| Discus | 3 and 6 | 3 |
| Valkey | 1 | 1 |
| Valkey Cluster with 2 replicas for each primary | 3 and 6 | 3 |
| Valkey Sentinel | 3 | 3 |
| Valkey Sentinel | 6 | 6 |

The script checks `DBSIZE` after the prefill and after all runs of a setup. Discus `DBSIZE` is the
sum of the keys of all nodes divided by 3. Five Discus setups reported 1,000,000 keys at both
checks. In cluster mode with 3 nodes, `DBSIZE` was 999,806 after the runs, so 582 copies were
missing.

A second run of that setup read the local key count of each node after each workload. This run is
not part of the results. It used the same deploy, prefill, and workload settings as the script.
One node kept 1,000,000 keys at all times. After each pipelined mixed run, the other two nodes held
these counts:

| Run | Node 2 | Node 3 |
| --- | ---: | ---: |
| 1 | 696,368 | 728,880 |
| 2 | 716,336 | 720,432 |
| 3 | 697,904 | 727,600 |

The counts stayed low during the next GET run. The next SET run wrote the keys again and brought
the counts back to about 995,500. No pod restarted, and no node evicted a key. Forward mode with 3
nodes and both 6-node setups kept every copy during the runs.

The [cluster guide](../../cluster.md) describes this path. When the replication queue of a peer
overflows, the primary moves the oldest keys to hints. When the hints reach their bound, the
primary loses the oldest hints. The replica then drops its copies of the key space of each lost
hint. After a client writes the key again or reads it from the replica, the copy comes back. In
cluster mode, reads go to the primary, so the dropped copies stay missing. During these runs, many
keys had 1 copy instead of 3. The GET results do not show it, because the primary held every key.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- The Valkey numbers come from 2026-11-10. The two sessions can differ in the load from other pods
  on the host.
- The client used at most 5.3 of its 8 CPU in a Discus run. The kernel throttled no client run, so
  the client did not set the rate.
- The kernel throttled no Discus server pod, except for 4 periods in one cluster-mode GET run with
  3 nodes.
- Without pipelining, the Discus server pods did not reach their 1 CPU. The latency sets the rate
  in these runs.
- The script does not log the local key count of each node. The second run of the 3-node cluster
  setup gave those counts.

## Files in this directory

`results-discus.jsonl` has one JSON line per Discus run, in forward mode and in cluster mode. The
Valkey results are in the [2026-11-10 directory](../2026-11-10-k8s-v0.3.0/). To run the Discus part
again, with the `home-ops` context and Helm available, run this command from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-12-k8s-v1.0.2-rc.1/results-discus.jsonl \
  --systems discus-cluster-1-1.0.2-rc.1,discus-cluster-3-1.0.2-rc.1,discus-cluster-6-1.0.2-rc.1,discus-1-1.0.2-rc.1,discus-3-1.0.2-rc.1,discus-6-1.0.2-rc.1
```
