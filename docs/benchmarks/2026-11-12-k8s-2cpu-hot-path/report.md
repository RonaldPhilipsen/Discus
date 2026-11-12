# Benchmark: Discus perf/hot-path against Valkey Cluster and Valkey Sentinel, 2 cores per pod

Date: 2026-11-12. Cluster: `home-ops`. Discus image `hot-path-3bc4fea-pyroscope` (branch
`perf/hot-path`, commit 3bc4fea), with profiling off. Valkey 9 with `--io-threads 2`.

This run follows the [benchmark methodology](../methodology.md). The one change is that every
server pod has a CPU limit of 2 cores, not 1. The runs took place from 19:42 to 21:13. The same
session ran Discus and every Valkey setup, so all numbers come from one session.

In forward mode, memtier connects to port 6379 of every pod. The nodes forward the commands that
they do not own. In cluster mode, memtier reads the slot map from port 6380 and sends each command
to the primary of its key.

## Main results

- Discus completed all 72 runs in both modes. No GET missed, and memtier reported 0 MOVED and 0
  ASK replies in all 36 cluster-mode runs.
- Every Discus setup kept all 3 copies of each key. Each node held 1,000,000 keys after the runs.
- In cluster mode with 3 nodes, Discus did 252,400 SET/s and Valkey Cluster did 190,200. Discus
  GET did 379,700/s and Valkey Cluster GET did 208,800.
- In cluster mode with 6 nodes, Discus did 356,500 SET/s and Valkey Cluster did 326,200. Discus
  GET did 365,100/s and Valkey Cluster GET did 347,400.
- With 16 requests in flight, Discus in cluster mode did 1,826,400 ops/s with 3 nodes and
  Valkey Cluster did 1,929,500. With 6 nodes, Discus did 2,747,400 and Valkey Cluster did
  2,516,100.
- With 1 node, Discus did about 150,000 to 157,000 ops/s without pipelining. Valkey did about
  81,000 to 85,000.
- Valkey with `--io-threads 2` and 2 cores was not faster than Valkey with 1 core in the earlier
  run. With 6 nodes it was slower. See [Valkey with io-threads 2](#valkey-with-io-threads-2).
- In cluster mode, Discus used less server CPU per operation than Valkey Cluster in every
  workload.

## Throughput

Operations per second, median of 3 runs of 30 seconds, rounded to hundreds.

### Throughput, 1 node

| Workload | Forward | Cluster | Valkey |
| --- | ---: | ---: | ---: |
| GET | 157,100 | 155,900 | 85,100 |
| SET | 150,500 | 149,600 | 81,500 |
| Mixed (1 write, 9 reads) | 154,300 | 154,700 | 84,900 |
| Mixed, 16 in flight | 960,800 | 970,700 | 740,700 |

### Throughput, 3 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 337,900 | 379,700 | 208,800 | 82,900 |
| SET | 141,600 | 252,400 | 190,200 | 75,600 |
| Mixed (1 write, 9 reads) | 288,200 | 351,100 | 201,600 | 81,400 |
| Mixed, 16 in flight | 1,177,300 | 1,826,400 | 1,929,500 | 735,300 |

### Throughput, 6 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 265,300 | 365,100 | 347,400 | 82,100 |
| SET | 186,400 | 356,500 | 326,200 | 70,700 |
| Mixed (1 write, 9 reads) | 243,600 | 355,100 | 338,600 | 80,200 |
| Mixed, 16 in flight | 838,000 | 2,747,400 | 2,516,100 | 675,000 |

With reads on all nodes, Valkey Sentinel did 225,400 GET/s with 3 nodes and 302,100 with 6 nodes.
The tables use the Sentinel runs with all connections on the primary.

The three runs of each Discus workload were within 8% of their median.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.61 | 1.01 | 0.61 | 1.01 | 1.10 | 1.46 |
| SET | 0.66 | 1.10 | 0.66 | 1.09 | 1.16 | 1.50 |
| Mixed (1 write, 9 reads) | 0.62 | 1.04 | 0.61 | 1.02 | 1.11 | 1.45 |
| Mixed, 16 in flight | 1.60 | 2.54 | 1.59 | 2.53 | 1.97 | 2.61 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Valkey Sentinel p50 | Valkey Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.80 | 1.45 | 0.75 | 1.32 | 1.38 | 1.81 | 3.41 | 4.48 |
| SET | 2.22 | 4.80 | 1.14 | 2.42 | 1.50 | 1.93 | 3.73 | 4.93 |
| Mixed (1 write, 9 reads) | 0.90 | 3.36 | 0.83 | 1.55 | 1.42 | 1.85 | 3.47 | 4.61 |
| Mixed, 16 in flight | 4.22 | 7.52 | 2.45 | 4.54 | 2.45 | 3.26 | 6.17 | 8.10 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Valkey Sentinel p50 | Valkey Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.06 | 4.89 | 1.48 | 2.75 | 1.62 | 2.80 | 6.88 | 9.02 |
| SET | 3.23 | 6.33 | 1.58 | 2.85 | 1.77 | 2.59 | 7.90 | 10.69 |
| Mixed (1 write, 9 reads) | 2.32 | 5.57 | 1.55 | 2.96 | 1.72 | 2.54 | 7.07 | 9.41 |
| Mixed, 16 in flight | 10.43 | 37.38 | 3.29 | 6.08 | 3.68 | 6.33 | 13.44 | 17.15 |

## CPU per operation

Cores is the CPU that all server pods of the setup used together, from the cgroup counters,
median of the 3 runs. µs/op is that CPU time divided by the number of operations. No server pod
was throttled for more than 3 periods of 100 ms in any run.

### Server CPU, 1 node

| Workload | Forward cores | Forward µs/op | Cluster cores | Cluster µs/op | Valkey cores | Valkey µs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.96 | 6.3 | 0.93 | 6.0 | 1.34 | 15.8 |
| SET | 1.00 | 6.6 | 0.95 | 6.3 | 1.61 | 19.7 |
| Mixed (1 write, 9 reads) | 0.80 | 5.2 | 1.17 | 7.7 | 1.55 | 18.5 |
| Mixed, 16 in flight | 1.17 | 1.2 | 1.10 | 1.1 | 1.27 | 1.7 |

### Server CPU, 3 nodes

| Workload | Forward cores | Forward µs/op | Cluster cores | Cluster µs/op | Valkey Cluster cores | Valkey Cluster µs/op | Valkey Sentinel cores | Valkey Sentinel µs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.91 | 8.7 | 4.03 | 10.6 | 3.82 | 17.8 | 1.37 | 16.4 |
| SET | 3.56 | 25.5 | 3.53 | 14.0 | 5.66 | 30.2 | 3.22 | 42.5 |
| Mixed (1 write, 9 reads) | 3.41 | 11.8 | 3.08 | 9.1 | 5.04 | 25.4 | 1.47 | 18.2 |
| Mixed, 16 in flight | 3.72 | 3.1 | 3.08 | 1.7 | 5.10 | 2.6 | 1.67 | 2.4 |

### Server CPU, 6 nodes

| Workload | Forward cores | Forward µs/op | Cluster cores | Cluster µs/op | Valkey Cluster cores | Valkey Cluster µs/op | Valkey Sentinel cores | Valkey Sentinel µs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 6.77 | 25.5 | 4.55 | 12.1 | 7.20 | 20.7 | 1.41 | 16.9 |
| SET | 7.76 | 41.6 | 7.20 | 20.8 | 11.12 | 33.4 | 4.47 | 63.8 |
| Mixed (1 write, 9 reads) | 6.83 | 28.2 | 6.30 | 17.9 | 9.93 | 29.1 | 1.63 | 20.6 |
| Mixed, 16 in flight | 8.90 | 10.6 | 7.87 | 2.9 | 10.49 | 4.2 | 1.64 | 2.4 |

## Valkey with io-threads 2

This table compares the Valkey setups of this run with the same setups in the
[1-core run](../2026-11-12-k8s-v1.1.1/report.md). That run used 1 core per pod and no `--io-threads`
flag. The I/O threads used 2 to 3 times more CPU. Throughput did not rise, except with 16 requests
in flight. With 6 nodes, GET, SET and the pipelined mix were 13% to 17% slower.

| Setup | Workload | io-threads 1, 1 core | io-threads 2, 2 cores | Cores used, 1 core | Cores used, 2 cores |
| --- | --- | ---: | ---: | ---: | ---: |
| Valkey, 1 node | GET | 86,700 | 85,100 | 0.36 | 1.34 |
| Valkey, 1 node | SET | 84,200 | 81,500 | 0.57 | 1.61 |
| Valkey, 1 node | Mixed, 16 in flight | 670,500 | 740,700 | 0.48 | 1.27 |
| Valkey Cluster, 3 nodes | GET | 228,100 | 208,800 | 1.55 | 3.82 |
| Valkey Cluster, 3 nodes | SET | 210,500 | 190,200 | 1.84 | 5.66 |
| Valkey Cluster, 3 nodes | Mixed, 16 in flight | 1,723,200 | 1,929,500 | 2.08 | 5.10 |
| Valkey Cluster, 6 nodes | GET | 419,900 | 347,400 | 2.61 | 7.20 |
| Valkey Cluster, 6 nodes | SET | 374,900 | 326,200 | 4.15 | 11.12 |
| Valkey Cluster, 6 nodes | Mixed, 16 in flight | 3,026,100 | 2,516,100 | 4.11 | 10.49 |

The Valkey numbers in this report are therefore not the best Valkey numbers on this cluster. With 6
nodes, the 1-core Valkey Cluster run did 374,900 SET/s and 419,900 GET/s, which is more than Discus
did in this run.

## What the results show

The Discus pods are bound by latency, not by CPU. memtier keeps a fixed number of connections per
node, so throughput is about the number of connections divided by the p50 latency. With 2 cores,
a node can apply a write on one core while the replication work runs on another. In cluster mode
with 3 nodes, SET p50 was 1.14 ms against 1.50 ms for Valkey Cluster, and the pods used about
1.2 cores each.

Forward mode adds a network hop to most commands. With 6 nodes, forward mode had a SET p50 of
3.23 ms against 1.58 ms in cluster mode. With 16 requests in flight, its p99 was 37.38 ms.

## Limits of this benchmark

- The 6-node runs ask for 12 server cores on one Kubernetes node that other workloads also use.
  Discus in cluster mode did fewer GET/s with 6 nodes (365,100) than with 3 nodes (379,700).
  Valkey Cluster with 6 nodes was slower than in the 1-core run. The 6-node numbers can be limited
  by the host, not by Discus or Valkey.
- The memtier client used up to 6.8 of its 8 cores in one run, and about 2 to 5 cores in most
  runs.
- The Discus image is a branch build, not a release.
- Valkey ran with `--io-threads 2`. A Valkey run without I/O threads and with 2 cores is not part
  of this report.

## Files in this directory

- `results.jsonl`: one line per run, with memtier results, `kubectl top` output and the cgroup CPU
  counters of each pod.

The run used this command:

```sh
SYSTEMS=discus-cluster-1-hot-path-pyro,discus-cluster-3-hot-path-pyro
SYSTEMS=$SYSTEMS,discus-cluster-6-hot-path-pyro,discus-1-hot-path-pyro
SYSTEMS=$SYSTEMS,discus-3-hot-path-pyro,discus-6-hot-path-pyro,valkey-1
SYSTEMS=$SYSTEMS,valkey-cluster-replica2-3,valkey-cluster-replica2-6
SYSTEMS=$SYSTEMS,valkey-sentinel-3,valkey-sentinel-6
BENCH_CPUS=2 BENCH_VALKEY_IO_THREADS=2 DISCUS_CHART=$PWD/charts/discus \
  python3 scripts/bench-k8s.py --systems "$SYSTEMS" --out results.jsonl
```
