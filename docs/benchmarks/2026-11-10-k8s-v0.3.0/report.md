# Benchmark: Discus 0.3.0-rc.2 with 1, 3, and 6 nodes

Date: 2026-11-10. Cluster: `home-ops`. Discus 0.3.0-rc.2, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). Every node was one pod with
1 CPU and 2 GiB. Every setup got 96 client connections per node. All runs took place in one
session, from 22:12 to 23:28.

Discus 0.3.0-rc.2 comes from pull request #47, commit `4ec02de`. That change gives membership,
gossip, and the admin server their own thread and UDP port. It also adds a local health score to
the failure detector. It raises the forward timeout defaults to 500 ms for reads and 2 s for
writes.

## Main results

- Discus 0.3.0-rc.2 completed every run at 1, 3, and 6 nodes, including all pipelined runs. No
  GET missed, and every node reported 1,000,000 keys after the runs.
- With 1 node, Discus did 91% to 94% of the Valkey rate without pipelining. With 16 commands in
  flight per connection, it did 64% of the Valkey rate.
- With 3 nodes, Discus did 98% of the Valkey Cluster GET rate. It did 41% of the SET rate, 79% of
  the mixed rate, and 35% of the pipelined rate.
- With 6 nodes, Discus did 49% of the Valkey Cluster GET rate. It did 31% of the SET rate, 41% of
  the mixed rate, and 15% of the pipelined rate.
- The Discus GET rate went down from 222,900 to 202,000 ops/s between 3 and 6 nodes. Valkey
  Cluster went up from 226,700 to 413,300 ops/s.
- Valkey Sentinel stayed between 77,900 and 87,000 ops/s on the primary at 3 and 6 nodes. Only
  reads on all nodes grew with the node count.

A likely cause of the Discus GET result is forwarding. With 3 nodes and 3 copies, every node has a
copy of every key, so no GET goes to another node. With 6 nodes, a node has a copy of half of the
keys. The other half of the GETs go to another node, which adds a network round trip. This run did
not count the forwards, so this cause is not confirmed.

## Results

Operations per second. Each value is the median of 3 runs of 30 seconds. The range of the three
runs was within 6% of the median for every setup and workload.

### Throughput, 1 node

| Workload | Discus | Valkey |
| --- | ---: | ---: |
| GET | 82,400 | 87,500 |
| SET | 77,400 | 85,200 |
| Mixed (1 write, 9 reads) | 81,000 | 86,700 |
| Mixed, 16 in flight | 425,500 | 669,500 |

### Throughput, 3 nodes

| Workload | Discus | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: |
| GET | 222,900 | 226,700 | 87,000 |
| GET, reads on all 3 nodes | | | 243,600 |
| SET | 85,000 | 207,700 | 80,600 |
| Mixed (1 write, 9 reads) | 171,400 | 217,900 | 85,100 |
| Mixed, 16 in flight | 618,400 | 1,774,200 | 641,800 |

### Throughput, 6 nodes

| Workload | Discus | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: |
| GET | 202,000 | 413,300 | 86,800 |
| GET, reads on all 6 nodes | | | 353,800 |
| SET | 118,400 | 381,200 | 77,900 |
| Mixed (1 write, 9 reads) | 166,800 | 403,200 | 83,400 |
| Mixed, 16 in flight | 478,400 | 3,126,600 | 603,300 |

## Latency

Latency in milliseconds, median of the 3 runs. With more than one memtier process, each value is
the highest percentile of the processes.

### Latency, 1 node

| Workload | Discus p50 | Discus p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: |
| GET | 1.15 | 1.70 | 1.07 | 2.10 |
| SET | 1.27 | 1.93 | 1.09 | 2.13 |
| Mixed (1 write, 9 reads) | 1.18 | 1.85 | 1.08 | 2.11 |
| Mixed, 16 in flight | 3.58 | 4.61 | 2.25 | 3.13 |

### Latency, 3 nodes

| Workload | Discus p50 | Discus p99 | Valkey Cluster p50 | Valkey Cluster p99 | Valkey Sentinel p50 | Valkey Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.27 | 2.22 | 1.22 | 2.37 | 3.25 | 4.83 |
| GET, reads on all 3 nodes | | | | | 1.14 | 2.25 |
| SET | 4.01 | 6.85 | 1.34 | 2.02 | 3.52 | 4.61 |
| Mixed (1 write, 9 reads) | 1.39 | 6.94 | 1.27 | 2.43 | 3.31 | 4.58 |
| Mixed, 16 in flight | 8.38 | 12.67 | 2.51 | 4.42 | 7.07 | 8.89 |

### Latency, 6 nodes

| Workload | Discus p50 | Discus p99 | Valkey Cluster p50 | Valkey Cluster p99 | Valkey Sentinel p50 | Valkey Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.43 | 7.42 | 1.36 | 2.42 | 6.46 | 8.83 |
| GET, reads on all 6 nodes | | | | | 1.54 | 2.62 |
| SET | 5.25 | 9.21 | 1.45 | 2.65 | 7.23 | 9.66 |
| Mixed (1 write, 9 reads) | 3.71 | 8.45 | 1.36 | 2.45 | 6.75 | 9.21 |
| Mixed, 16 in flight | 17.66 | 49.92 | 3.02 | 4.80 | 15.04 | 18.43 |

The Sentinel primary takes the connections of all nodes, 288 with 3 nodes and 576 with 6 nodes.
It is one process, so more connections give more latency, not more throughput.

## CPU use

CPU use in cores, as the mean over the server pods, median of the 3 runs. The values come from
`usage_seconds` in the `cfs` field, divided by the run duration.

| Workload | Discus, 1 node | Valkey, 1 node | Discus, 3 nodes | Valkey Cluster, 3 nodes | Discus, 6 nodes | Valkey Cluster, 6 nodes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.49 | 0.55 | 0.47 | 0.41 | 0.55 | 0.52 |
| SET | 0.55 | 0.50 | 0.56 | 0.70 | 0.64 | 0.60 |
| Mixed (1 write, 9 reads) | 0.58 | 0.48 | 0.48 | 0.54 | 0.61 | 0.54 |
| Mixed, 16 in flight | 0.57 | 0.51 | 0.73 | 0.69 | 0.66 | 0.75 |

No server pod and no client pod was throttled in any run. No server pod used its full CPU. Without
pipelining, the rate follows from the connections and the latency. For example, 96 connections at
1.07 ms give about 90,000 ops/s. These runs measure latency more than capacity. The pipelined runs
come closer to capacity.

The client used at most 4.9 of its 8 CPU, in the pipelined Valkey Cluster run with 6 nodes. The
client did not set the result of any run.

## Copies of each key

| Nodes | Discus | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: |
| 1 | 1 | | |
| 3 | 3 | 3 | 3 |
| 6 | 3 | 3 | 6 |

Valkey Cluster had 2 replicas for each primary. Each Cluster pod ran one primary and two replicas
of other primaries, and the three processes shared the 1 CPU of the pod. The single Valkey node
had no replicas. In Sentinel, only the primary takes writes.

## Limits of this benchmark

- A forward timeout is still evidence against the peer. If a heavier load or a smaller CPU limit
  pushes forwards past 500 ms, nodes can mark busy but healthy peers as Suspect.
- The CPU values come from the cAdvisor counters of the kubelet. The kubelet updates them at
  intervals, so a value can be off by a part of the run.
- The Discus client connects to each pod IP, not through the ClusterIP Service. A real client
  connects through the Service and gets a less even spread.
- All pods and the client ran on one host, `k8s-neo-alpha`, with 32 cores. The network round trip
  does not cross a physical network.

## Files in this directory

| File | Content |
| --- | --- |
| `report.md` | This report. |
| `results-matrix.jsonl` | Valkey with 1 node, Discus with 1 and 3 nodes, Valkey Cluster with 3 nodes, and Valkey Sentinel with 3 and 6 nodes. |
| `results-discus-6.jsonl` | Discus with 6 nodes. |
| `results-valkey-cluster-6.jsonl` | Valkey Cluster with 6 nodes. |

Each file has one JSON line per run, with the CFS counters of each pod in `cfs`. The lines with a
`verify` field hold the key count and the replication state of each node before and after the
runs.

To run the benchmark again, with the `home-ops` context and Helm available, run this command from
the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-10-k8s-v0.3.0/results.jsonl \
  --systems valkey-1,discus-1-0.3.0-rc.2,discus-3-0.3.0-rc.2,valkey-cluster-replica2-3,valkey-sentinel-3,discus-6-0.3.0-rc.2,valkey-cluster-replica2-6,valkey-sentinel-6
```
