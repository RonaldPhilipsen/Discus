# Benchmark: Discus 1.1.1 against Valkey Cluster and Valkey Sentinel

Date: 2026-11-12. Cluster: `home-ops`. Discus 1.1.1, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). The runs took place from 11:29 to
13:01. The same session ran Discus and every Valkey setup, so all numbers come from one session.

In forward mode, memtier connects to port 6379 of every pod. The nodes forward the commands that
they do not own. In cluster mode, memtier reads the slot map from port 6380 and sends each command
to the primary of its key.

## Main results

- Discus completed all 72 runs in both modes. No GET missed, and memtier reported 0 MOVED and 0
  ASK replies in all 36 cluster-mode runs.
- Every Discus setup kept all 3 copies of each key. Each node held 1,000,000 keys after the runs.
- In cluster mode, Discus GET did 94% of Valkey Cluster with 3 nodes and 95% with 6 nodes.
- In cluster mode, Discus SET did 67% of Valkey Cluster with 3 nodes and 64% with 6 nodes.
- With 16 requests in flight, Discus in cluster mode did 61% of Valkey Cluster with 3 nodes and
  60% with 6 nodes.
- In cluster mode, Discus did 166% to 468% of the Valkey Sentinel rate with 3 and 6 nodes.
- With 1 node, Discus did 89% to 96% of Valkey without pipelining, and 73% with 16 requests in
  flight.
- Forward mode does not scale past 3 nodes. With 6 nodes, cluster mode does 1.8 to 3.4 times the
  forward-mode rate.

## Throughput

Operations per second, median of 3 runs of 30 seconds, rounded to hundreds. A share is the Discus
rate divided by the Valkey rate.

### Throughput, 1 node

| Workload | Forward | Cluster | Valkey | Forward share | Cluster share |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 81,600 | 83,500 | 86,700 | 94% | 96% |
| SET | 75,200 | 76,800 | 84,200 | 89% | 91% |
| Mixed (1 write, 9 reads) | 81,700 | 82,700 | 86,900 | 94% | 95% |
| Mixed, 16 in flight | 489,800 | 490,900 | 670,500 | 73% | 73% |

### Throughput, 3 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 218,900 | 214,500 | 228,100 | 85,700 |
| SET | 89,600 | 142,000 | 210,500 | 80,500 |
| Mixed (1 write, 9 reads) | 174,300 | 202,100 | 219,300 | 83,300 |
| Mixed, 16 in flight | 695,300 | 1,049,600 | 1,723,200 | 633,700 |

### Throughput, 6 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 205,000 | 397,600 | 419,900 | 85,000 |
| SET | 123,900 | 240,700 | 374,900 | 77,300 |
| Mixed (1 write, 9 reads) | 178,400 | 322,000 | 397,700 | 82,100 |
| Mixed, 16 in flight | 522,800 | 1,800,800 | 3,026,100 | 589,700 |

With reads on all nodes, Valkey Sentinel did 239,500 GET/s with 3 nodes and 349,000 with 6 nodes.
The tables use the Sentinel runs with all connections on the primary.

The three runs of each Discus workload were within 7% of their median.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.16 | 1.73 | 1.14 | 1.67 | 1.07 | 2.10 |
| SET | 1.26 | 1.97 | 1.24 | 1.85 | 1.11 | 2.17 |
| Mixed (1 write, 9 reads) | 1.17 | 1.80 | 1.14 | 1.79 | 1.07 | 2.08 |
| Mixed, 16 in flight | 3.12 | 4.09 | 3.10 | 4.01 | 2.25 | 3.17 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.27 | 2.29 | 1.30 | 2.02 | 1.21 | 2.38 | 3.28 | 4.64 |
| SET | 3.79 | 6.46 | 1.95 | 3.47 | 1.33 | 1.96 | 3.52 | 4.64 |
| Mixed (1 write, 9 reads) | 1.36 | 6.82 | 1.43 | 2.27 | 1.26 | 2.45 | 3.39 | 4.74 |
| Mixed, 16 in flight | 7.42 | 11.39 | 4.29 | 6.37 | 2.53 | 4.45 | 7.17 | 8.96 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.72 | 7.17 | 1.41 | 2.29 | 1.38 | 2.48 | 6.59 | 9.15 |
| SET | 4.96 | 8.77 | 2.33 | 3.95 | 1.46 | 2.67 | 7.29 | 9.66 |
| Mixed (1 write, 9 reads) | 3.55 | 7.87 | 1.69 | 2.77 | 1.38 | 2.48 | 6.88 | 9.28 |
| Mixed, 16 in flight | 15.55 | 48.38 | 5.02 | 7.58 | 3.06 | 4.83 | 15.36 | 19.20 |

The Sentinel primary takes all 288 or 576 connections, so its latency is high with 3 and 6 nodes.

## CPU per operation

Microseconds of server CPU for each operation: the `usage_seconds` of all server pods together,
divided by the number of operations. The value is the median of the 3 runs.

In forward mode, most writes take an extra hop, so a SET costs 2.4 to 2.7 times the CPU of a Valkey
Cluster SET. Cluster mode removes that hop. In cluster mode, a SET costs 1.3 to 1.5 times the CPU
of a Valkey Cluster SET with 3 and 6 nodes.

## Where Discus can improve

The largest gaps to Valkey Cluster are SET and the pipelined mix in cluster mode. To find the
cause, a local run profiled a cluster of 3 Discus 1.1.1 nodes in cluster mode, with one core for
each node. The profile counts user-space CPU only. The shares below are parts of the CPU of one
node. They come from one machine, so they show where the time goes, not the Kubernetes rates.

| Cost | SET | Mixed, 16 in flight | GET |
| --- | ---: | ---: | ---: |
| Write set of each connection (`WriteSet::record`) | 13% | 5% | 0% |
| Queue of each written key for the replicas (`Replicator::written`) | 12% | 4% | 0% |
| Read lock of the epoch history in `Node::owners` | 5% | 11% | 8% |
| Command lookup with SipHash and a UTF-8 scan | 1% | 3% | 2% |
| New `hard_stop.cancelled()` future at each wait of a connection | 5% | 1% | 11% |

- The write set takes a part of a node-wide budget of 1% of the memory. With 288 connections, the
  budget is full. Then each SET evicts the oldest key of the set, frees it, and allocates a new
  entry. With the whole memory as the budget, the write set cost fell from 13% to 5%.
- Each write looks up its owners a second time to queue the key. It then takes the peer lock and
  clones a shared pointer for each replica. Each queued key allocates a list node.
- The epoch read lock costs two atomic operations for each command, and a write takes it twice.
  Almost all of its time is in those two instructions, not in the owner lookup.
- The GET cost of the stop futures includes the work of `tokio::sync::Notify`. Each new future
  registers with the token and leaves it again.

The send of the replication batches, their receive, and QUIC together took about 12% of the CPU
of a SET. These parts are not the main cost now.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- The client used at most 5.6 of its 8 CPU. The kernel throttled no client run, so the client did
  not set the rate.
- The kernel throttled no Discus server pod, except for 13 periods in the cluster-mode GET runs
  with 3 nodes.
- Without pipelining, the Discus server pods did not reach their 1 CPU. The latency sets the rate
  in these runs.
- The profile in [Where Discus can improve](#where-discus-can-improve) comes from a local machine,
  not from the Kubernetes pods.

## Files in this directory

`results.jsonl` has one JSON line per run of every setup. `run.log` is the log of the script. To
run the benchmark again, with the `home-ops` context and Helm available, run this command from the
repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-12-k8s-v1.1.1/results.jsonl \
  --systems discus-cluster-1-1.1.1,discus-cluster-3-1.1.1,discus-cluster-6-1.1.1,discus-1-1.1.1,discus-3-1.1.1,discus-6-1.1.1,valkey-1,valkey-cluster-replica2-3,valkey-cluster-replica2-6,valkey-sentinel-3,valkey-sentinel-6
```
