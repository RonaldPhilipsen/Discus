# Benchmark: Discus 0.4.1-rc.1 with 1, 3, and 6 nodes

Date: 2026-11-11. Cluster: `home-ops`. Discus 0.4.1-rc.1, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). Every node was one pod with
1 CPU and 2 GiB. Every setup got 96 client connections per node. The Discus runs took place from
08:19 to 08:44.

Discus 0.4.1-rc.1 comes from pull request #49, commit `cb738e4`. That change makes a forwarded
command cheaper. The placement hash runs in one pass. If the write set of a connection evicts a key,
the new key takes over its memory budget. The frame encoder reuses its builders. The wire format and
the placement do not change.

The Valkey numbers come from the [2026-11-10 report](../2026-11-10-k8s-v0.3.0/report.md). This
run did not repeat them.

## Main results

- Discus 0.4.1-rc.1 completed every run at 1, 3, and 6 nodes, including all pipelined runs. No
  GET missed, and every node reported 1,000,000 keys after the runs.
- With 1 node, Discus did 88% to 94% of the Valkey rate without pipelining. With 16 commands in
  flight per connection, it did 64% of the Valkey rate.
- With 3 nodes, Discus did 98% of the Valkey Cluster GET rate. It did 42% of the SET rate, 79% of
  the mixed rate, and 36% of the pipelined rate.
- With 6 nodes, Discus did 48% of the Valkey Cluster GET rate. It did 32% of the SET rate, 42% of
  the mixed rate, and 16% of the pipelined rate.
- SET stays behind because most writes need a forward to the primary of the key. The section
  [Why SET stays behind](#why-set-stays-behind) explains this.

## Results

Operations per second. Each value is the median of 3 runs of 30 seconds. The range of the three
Discus runs was within 3% of the median for every setup and workload.

### Throughput, 1 node

| Workload | Discus | Valkey |
| --- | ---: | ---: |
| GET | 82,300 | 87,500 |
| SET | 74,600 | 85,200 |
| Mixed (1 write, 9 reads) | 81,200 | 86,700 |
| Mixed, 16 in flight | 427,800 | 669,500 |

### Throughput, 3 nodes

| Workload | Discus | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: |
| GET | 221,500 | 226,700 | 87,000 |
| GET, reads on all 3 nodes | | | 243,600 |
| SET | 86,800 | 207,700 | 80,600 |
| Mixed (1 write, 9 reads) | 172,100 | 217,900 | 85,100 |
| Mixed, 16 in flight | 635,500 | 1,774,200 | 641,800 |

### Throughput, 6 nodes

| Workload | Discus | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: |
| GET | 198,500 | 413,300 | 86,800 |
| GET, reads on all 6 nodes | | | 353,800 |
| SET | 120,700 | 381,200 | 77,900 |
| Mixed (1 write, 9 reads) | 171,100 | 403,200 | 83,400 |
| Mixed, 16 in flight | 496,900 | 3,126,600 | 603,300 |

### Discus as a share of Valkey

| Workload | 1 node, Valkey | 3 nodes, Valkey Cluster | 6 nodes, Valkey Cluster |
| --- | ---: | ---: | ---: |
| GET | 94% | 98% | 48% |
| SET | 88% | 42% | 32% |
| Mixed (1 write, 9 reads) | 94% | 79% | 42% |
| Mixed, 16 in flight | 64% | 36% | 16% |

## Latency

Latency in milliseconds, median of the 3 runs. With more than one memtier process, each value is
the highest percentile of the processes.

### Latency, 1 node

| Workload | Discus p50 | Discus p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: |
| GET | 1.15 | 1.74 | 1.07 | 2.10 |
| SET | 1.28 | 1.99 | 1.09 | 2.13 |
| Mixed (1 write, 9 reads) | 1.17 | 1.81 | 1.08 | 2.11 |
| Mixed, 16 in flight | 3.55 | 4.64 | 2.25 | 3.13 |

### Latency, 3 nodes

| Workload | Discus p50 | Discus p99 | Valkey Cluster p50 | Valkey Cluster p99 | Valkey Sentinel p50 | Valkey Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.27 | 2.24 | 1.22 | 2.37 | 3.25 | 4.83 |
| GET, reads on all 3 nodes | | | | | 1.14 | 2.25 |
| SET | 4.03 | 6.78 | 1.34 | 2.02 | 3.52 | 4.61 |
| Mixed (1 write, 9 reads) | 1.39 | 6.91 | 1.27 | 2.43 | 3.31 | 4.58 |
| Mixed, 16 in flight | 8.26 | 12.41 | 2.51 | 4.42 | 7.07 | 8.89 |

### Latency, 6 nodes

| Workload | Discus p50 | Discus p99 | Valkey Cluster p50 | Valkey Cluster p99 | Valkey Sentinel p50 | Valkey Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.48 | 7.39 | 1.36 | 2.42 | 6.46 | 8.83 |
| GET, reads on all 6 nodes | | | | | 1.54 | 2.62 |
| SET | 5.18 | 9.02 | 1.45 | 2.65 | 7.23 | 9.66 |
| Mixed (1 write, 9 reads) | 3.63 | 8.16 | 1.36 | 2.45 | 6.75 | 9.21 |
| Mixed, 16 in flight | 17.02 | 49.66 | 3.02 | 4.80 | 15.04 | 18.43 |

The Sentinel primary takes the connections of all nodes, 288 with 3 nodes and 576 with 6 nodes.
It is one process, so more connections give more latency, not more throughput.

## CPU use

CPU use in cores, as the mean over the server pods, median of the 3 runs. The values come from
`usage_seconds` in the `cfs` field, divided by the run duration.

| Workload | Discus, 1 node | Valkey, 1 node | Discus, 3 nodes | Valkey Cluster, 3 nodes | Discus, 6 nodes | Valkey Cluster, 6 nodes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.37 | 0.55 | 0.49 | 0.41 | 0.56 | 0.52 |
| SET | 0.48 | 0.50 | 0.66 | 0.70 | 0.65 | 0.60 |
| Mixed (1 write, 9 reads) | 0.48 | 0.48 | 0.60 | 0.54 | 0.56 | 0.54 |
| Mixed, 16 in flight | 0.70 | 0.51 | 0.68 | 0.69 | 0.73 | 0.75 |

No server pod and no client pod was throttled in any Discus run. The client used at most 3.3 of
its 8 CPU. Without pipelining, the rate follows from the connections and the latency. These runs
measure latency more than capacity.

## Why SET stays behind

A write runs on the primary of its key. A client connects to any node, so that node forwards the
SET when it is not the primary. With 3 nodes that is 2 of every 3 SETs, and with 6 nodes it is 5
of every 6. A read goes to any node with a copy. With 3 nodes and 3 copies, every node has every
key, so no GET needs a forward.

Valkey Cluster has no forwards in this benchmark. Memtier in Cluster mode knows the slot of each
key and sends the command to the right primary. In the local measurement, one forward cost about
4.5 µs of user CPU, and a local GET cost 0.67 µs. Replication to two secondaries added about
2.2 µs per SET.

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

- The Discus and Valkey numbers come from two sessions on two days. Load from other pods on the
  host can differ between sessions.
- A forward timeout is still evidence against the peer. If a heavier load or a smaller CPU limit
  pushes forwards past 500 ms, nodes can mark busy but healthy peers as Suspect.
- The CPU values come from the cAdvisor counters of the kubelet. The kubelet updates them at
  intervals, so a value can be off by a part of the run. One sample showed 1.05 cores on a pod
  with a limit of 1 CPU.
- The Discus client connects to each pod IP, not through the ClusterIP Service. A real client
  connects through the Service and gets a less even spread.
- All pods and the client ran on one host, `k8s-neo-alpha`, with 32 cores. The network round trip
  does not cross a physical network.

## Files in this directory

| File | Content |
| --- | --- |
| `report.md` | This report. |
| `results-discus.jsonl` | Discus 0.4.1-rc.1 with 1, 3, and 6 nodes. |

The file has one JSON line per run, with the CFS counters of each pod in `cfs`. The lines with a
`verify` field hold the key count of each node before and after the runs. The Valkey results are
in the [2026-11-10 directory](../2026-11-10-k8s-v0.3.0/).

To run the Discus part again, with the `home-ops` context and Helm available, run this command
from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-11-k8s-v0.4.1-rc.1/results-discus.jsonl \
  --systems discus-1-0.4.1-rc.1,discus-3-0.4.1-rc.1,discus-6-0.4.1-rc.1
```
