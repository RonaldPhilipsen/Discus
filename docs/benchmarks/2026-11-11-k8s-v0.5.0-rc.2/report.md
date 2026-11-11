# Benchmark: Discus 0.5.0-rc.2 in forward mode and cluster mode

Date: 2026-11-11. Cluster: `home-ops`. Discus 0.5.0-rc.2, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). It uses the same setups as the
[0.5.0-rc.1 report](../2026-11-11-k8s-v0.5.0-rc.1/report.md). Every node was one pod with 1 CPU
and 2 GiB. Every setup got 96 client connections per node. The runs took place from 17:46 to
18:36.

Discus 0.5.0-rc.2 has one change from 0.5.0-rc.1. A replica applies a replicated entry with one
hash-map lookup under the write lock. Before, it did a version lookup first and then a take and an
insert. In a local profile under SET load, this function went from 19.0% to 10.3% of the node
CPU. This run tests whether the SET rate goes up, mostly in cluster mode.

The numbers come from three sources:

- **rc.2**: this run, in the file `results-discus.jsonl` of this directory.
- **rc.1**: the [0.5.0-rc.1 report](../2026-11-11-k8s-v0.5.0-rc.1/report.md) and its file
  `results-discus.jsonl`. That run took place on the same day, from 14:50 to 15:39.
- **Valkey**: the [2026-11-10 report](../2026-11-10-k8s-v0.3.0/report.md) and its files
  `results-matrix.jsonl` and `results-valkey-cluster-6.jsonl`. This run did not repeat Valkey.
  With 1 node, Valkey ran without Cluster. With 3 and 6 nodes, Valkey Cluster had 2 replicas for
  each primary.

Forward mode and cluster mode have the same meaning as in the rc.1 report. In forward mode,
memtier connects to port 6379 of every pod, and the nodes forward the commands that they do not
own. In cluster mode, one memtier process reads the slot map from port 6380. It then sends each
command to the primary of its key.

## Main results

- Discus 0.5.0-rc.2 completed all 72 runs at 1, 3, and 6 nodes in both modes. No GET missed. Every
  node reported 1,000,000 keys after the prefill and after the runs.
- memtier reported 0 MOVED and 0 ASK replies per second in all 36 cluster-mode runs.
- The change did not give a SET rate change that this benchmark can see. Each rc.1 to rc.2
  difference is smaller than the range of the three runs, or the ranges of rc.1 and rc.2 overlap.
- In cluster mode, the SET median was 69% of Valkey Cluster with 3 nodes and 60% with 6 nodes.
  The target is 70%. Discus did not meet it.
- In cluster mode with 6 nodes, the GET median was 96% of Valkey Cluster. The target is 80%.
  Discus met it.
- In cluster mode, Discus did 147% to 459% of the Valkey Sentinel rate at 3 and 6 nodes. The
  section on Valkey Cluster and Valkey Sentinel has all four workloads.

## SET

Operations per second, median of 3 runs of 30 seconds. "Range" is the highest run minus the lowest
run, as a share of the median. With 1 node, the Valkey value is of Valkey without Cluster.

| Setup | Nodes | rc.1 | rc.2 | Change | Range rc.1 | Range rc.2 | Valkey | rc.1 share | rc.2 share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Forward | 1 | 73,900 | 72,600 | -1.8% | 2% | 2% | 85,200 | 87% | 85% |
| Cluster | 1 | 75,100 | 73,900 | -1.6% | 3% | 4% | 85,200 | 88% | 87% |
| Forward | 3 | 84,200 | 82,900 | -1.6% | 1% | 2% | 207,700 | 41% | 40% |
| Cluster | 3 | 133,200 | 143,600 | +7.9% | 10% | 12% | 207,700 | 64% | 69% |
| Forward | 6 | 116,600 | 116,800 | +0.2% | 6% | 6% | 381,200 | 31% | 31% |
| Cluster | 6 | 244,900 | 229,000 | -6.5% | 7% | 7% | 381,200 | 64% | 60% |

The three cluster-mode runs gave these rates:

| Setup | rc.1 runs | rc.2 runs |
| --- | --- | --- |
| Cluster, 3 nodes | 145,100, 133,200, 132,400 | 144,000, 143,600, 126,600 |
| Cluster, 6 nodes | 247,700, 244,900, 230,900 | 243,200, 229,000, 226,300 |

With 3 nodes, the best runs of rc.1 and rc.2 are almost the same, and one rc.2 run is below all
rc.1 runs. With 6 nodes, the best rc.2 run is above the lowest rc.1 run, so the ranges overlap.
Thus the +7.9% and the -6.5% are not larger than the noise of three runs.

Latency in milliseconds and server CPU per operation, median of the 3 runs. The CPU value is the
`usage_seconds` of all server pods together, divided by the number of operations. It is in
microseconds.

| Setup | Nodes | p50 rc.1 | p50 rc.2 | p99 rc.1 | p99 rc.2 | CPU/op rc.1 | CPU/op rc.2 | Valkey p50 | Valkey p99 | Valkey CPU/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Forward | 1 | 1.29 | 1.30 | 1.97 | 2.10 | 7.1 | 6.8 | 1.09 | 2.13 | 5.9 |
| Cluster | 1 | 1.27 | 1.28 | 2.01 | 1.98 | 7.0 | 6.6 | 1.09 | 2.13 | 5.9 |
| Forward | 3 | 4.03 | 4.09 | 6.94 | 7.01 | 22.9 | 23.1 | 1.34 | 2.02 | 10.0 |
| Cluster | 3 | 2.04 | 1.95 | 3.73 | 3.44 | 12.1 | 11.3 | 1.34 | 2.02 | 10.0 |
| Forward | 6 | 5.28 | 5.31 | 9.47 | 9.47 | 32.7 | 31.8 | 1.45 | 2.65 | 9.4 |
| Cluster | 6 | 2.37 | 2.37 | 4.09 | 4.06 | 14.4 | 15.6 | 1.45 | 2.65 | 9.4 |

The CPU per SET went down by 3% to 7% in four setups. It went up by 1% in forward mode with 3
nodes and by 8% in cluster mode with 6 nodes. These differences are about the size of the error of
the cAdvisor counters. The local profile predicts a drop of about 9% of the node CPU. This
benchmark cannot confirm or reject that drop.

## GET

The change is on the write path, so GET is a control. The columns have the same meaning as in the
SET tables.

| Setup | Nodes | rc.1 | rc.2 | Change | Range rc.1 | Range rc.2 | Valkey | rc.1 share | rc.2 share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Forward | 1 | 81,400 | 83,600 | +2.8% | 4% | 10% | 87,500 | 93% | 96% |
| Cluster | 1 | 83,200 | 83,000 | -0.3% | 2% | 4% | 87,500 | 95% | 95% |
| Forward | 3 | 220,900 | 220,700 | -0.1% | 1% | 1% | 226,700 | 97% | 97% |
| Cluster | 3 | 219,300 | 218,900 | -0.2% | 4% | 3% | 226,700 | 97% | 97% |
| Forward | 6 | 198,100 | 199,300 | +0.6% | 3% | 3% | 413,300 | 48% | 48% |
| Cluster | 6 | 397,700 | 398,500 | +0.2% | 5% | 7% | 413,300 | 96% | 96% |

| Setup | Nodes | p50 rc.1 | p50 rc.2 | p99 rc.1 | p99 rc.2 | CPU/op rc.1 | CPU/op rc.2 | Valkey p50 | Valkey p99 | Valkey CPU/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Forward | 1 | 1.17 | 1.17 | 1.74 | 1.86 | 7.0 | 5.0 | 1.07 | 2.10 | 6.1 |
| Cluster | 1 | 1.14 | 1.14 | 1.70 | 1.73 | 4.2 | 6.1 | 1.07 | 2.10 | 6.1 |
| Forward | 3 | 1.27 | 1.28 | 2.24 | 2.22 | 5.8 | 5.4 | 1.22 | 2.37 | 5.7 |
| Cluster | 3 | 1.32 | 1.29 | 2.08 | 2.02 | 5.8 | 8.7 | 1.22 | 2.37 | 5.7 |
| Forward | 6 | 2.90 | 2.78 | 7.55 | 7.42 | 17.7 | 16.9 | 1.36 | 2.42 | 7.5 |
| Cluster | 6 | 1.40 | 1.40 | 2.29 | 2.27 | 7.6 | 6.4 | 1.36 | 2.42 | 7.5 |

All GET medians are within 3% of rc.1, so the two sessions had the same conditions. The GET CPU per
operation changed by up to 50% between rc.1 and rc.2 with no code change on that path. Thus the
CPU values of single setups are too noisy to show a change of 10% or less.

## Spec targets

| Target | rc.1 | rc.2 | Met with rc.2 |
| --- | ---: | ---: | --- |
| Cluster-mode SET with 3 nodes, at least 70% of Valkey Cluster | 64% | 69% | No |
| Cluster-mode SET with 6 nodes, at least 70% of Valkey Cluster | 64% | 60% | No |
| Cluster-mode GET with 6 nodes, at least 80% of Valkey Cluster | 96% | 96% | Yes |

The spec calls these values targets, not gates. With 3 nodes, the best rc.2 run did 69%, and the
best rc.1 run did 70%. Three runs cannot separate 64% from 69%.

## Comparison with Valkey Cluster and Valkey Sentinel

This section compares rc.2 with Valkey on all four workloads at 1, 3, and 6 nodes. The Valkey
numbers come from the 2026-11-10 runs, as in the rest of this report. Each value is the median of
3 runs of 30 seconds.

The setups:

| Setup | Layout |
| --- | --- |
| Discus forward | memtier connects to port 6379 of every pod. A node forwards a command that it does not own. |
| Discus cluster | One memtier process in cluster mode reads the slot map from port 6380. It sends each command to the primary of its key. |
| Valkey | One Valkey node, without Cluster. Only at 1 node. |
| Valkey Cluster | Valkey Cluster with 2 replicas for each primary. memtier runs in cluster mode. |
| Valkey Sentinel | 1 primary and 2 or 5 replicas. memtier sends all commands to the primary. |

### Summary of the comparison

- With 1 node, Discus did 85% to 96% of the Valkey rate without pipelining. With 16 commands in
  flight per connection, it did 61% to 65%.
- In cluster mode, Discus GET is at the level of Valkey Cluster: 97% at 3 nodes and 96% at 6 nodes.
- In cluster mode, Discus SET is the largest gap: 69% of Valkey Cluster at 3 nodes and 60% at 6
  nodes. The SET p50 is 0.6 ms to 0.9 ms higher than in Valkey Cluster.
- In cluster mode, the mix of 1 write and 9 reads did 90% of Valkey Cluster at 3 nodes and 82% at 6
  nodes. With pipelining, Discus did about half the Valkey Cluster rate.
- Forward mode does not scale past 3 nodes. At 6 nodes, it did 31% to 48% of Valkey Cluster without
  pipelining and 15% with pipelining. Cluster mode doubles the rate at 6 nodes for every workload.
- Discus did more than Valkey Sentinel in every setup except one: forward mode with pipelining did
  97% of Sentinel at 3 nodes and 80% at 6 nodes. Sentinel sends all writes to one primary, so its
  rate does not go up with more nodes.

### Throughput

Operations per second. A share is the Discus rate divided by the Valkey rate in the same row.

#### Throughput at 1 node

| Workload | Discus forward | Discus cluster | Valkey | Forward share | Cluster share |
| --- | ---: | ---: | ---: | ---: | ---: |
| `get` | 83,600 | 83,000 | 87,500 | 96% | 95% |
| `set` | 72,600 | 73,900 | 85,200 | 85% | 87% |
| `mixed-1w9r` | 79,900 | 81,800 | 86,700 | 92% | 94% |
| `mixed-1w9r-p16` | 408,600 | 432,000 | 669,500 | 61% | 65% |

#### Throughput at 3 nodes

| Workload | Discus forward | Discus cluster | Valkey Cluster | Valkey Sentinel | Cluster share of Valkey Cluster | Cluster share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 220,700 | 218,900 | 226,700 | 87,000 | 97% | 252% |
| `set` | 82,900 | 143,600 | 207,700 | 80,600 | 69% | 178% |
| `mixed-1w9r` | 168,200 | 196,400 | 217,900 | 85,100 | 90% | 231% |
| `mixed-1w9r-p16` | 620,300 | 942,600 | 1,774,200 | 641,800 | 53% | 147% |

In forward mode, `get` did 97% of Valkey Cluster and `set` did 40%. The two mixes did 77% and 35%.

#### Throughput at 6 nodes

| Workload | Discus forward | Discus cluster | Valkey Cluster | Valkey Sentinel | Cluster share of Valkey Cluster | Cluster share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 199,300 | 398,500 | 413,300 | 86,800 | 96% | 459% |
| `set` | 116,800 | 229,000 | 381,200 | 77,900 | 60% | 294% |
| `mixed-1w9r` | 168,500 | 329,400 | 403,200 | 83,400 | 82% | 395% |
| `mixed-1w9r-p16` | 482,200 | 1,635,400 | 3,126,600 | 603,300 | 52% | 271% |

In forward mode, `get` did 48% of Valkey Cluster and `set` did 31%. The two mixes did 42% and 15%.

Valkey Sentinel can also spread reads over all its nodes. With reads on all nodes, Sentinel did
243,600 GET per second at 3 nodes and 353,800 at 6 nodes. Discus cluster mode did 90% and 113% of
those values.

### Latency

p50 and p99 in milliseconds, median of the 3 runs.

#### Latency at 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 1.17 | 1.86 | 1.14 | 1.73 | 1.07 | 2.10 |
| `set` | 1.30 | 2.10 | 1.28 | 1.98 | 1.09 | 2.13 |
| `mixed-1w9r` | 1.20 | 1.93 | 1.17 | 1.81 | 1.08 | 2.11 |
| `mixed-1w9r-p16` | 3.68 | 5.21 | 3.52 | 4.58 | 2.25 | 3.13 |

#### Latency at 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 1.28 | 2.22 | 1.29 | 2.02 | 1.22 | 2.37 | 3.25 | 4.83 |
| `set` | 4.09 | 7.01 | 1.95 | 3.44 | 1.34 | 2.02 | 3.52 | 4.61 |
| `mixed-1w9r` | 1.41 | 7.07 | 1.44 | 2.32 | 1.27 | 2.43 | 3.31 | 4.58 |
| `mixed-1w9r-p16` | 8.32 | 12.73 | 4.64 | 6.72 | 2.51 | 4.42 | 7.07 | 8.89 |

#### Latency at 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 2.78 | 7.42 | 1.40 | 2.27 | 1.36 | 2.42 | 6.46 | 8.83 |
| `set` | 5.31 | 9.47 | 2.37 | 4.06 | 1.45 | 2.65 | 7.23 | 9.66 |
| `mixed-1w9r` | 3.76 | 8.38 | 1.70 | 2.81 | 1.36 | 2.45 | 6.75 | 9.21 |
| `mixed-1w9r-p16` | 17.28 | 50.69 | 5.44 | 7.97 | 3.02 | 4.80 | 15.04 | 18.43 |

Sentinel has a high latency at 3 and 6 nodes because all 288 or 576 connections go to one primary.

### Server CPU per operation

Microseconds of server CPU for each operation. The value is the `usage_seconds` of all server pods
together, divided by the number of operations. The cAdvisor counters are noisy: between rc.1 and
rc.2, the GET value of one setup changed by up to 50% with no code change. Use these values only
for differences of about 2 times or more.

| Workload | Nodes | Discus forward | Discus cluster | Valkey or Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: |
| `get` | 1 | 5.0 | 6.1 | 6.1 | |
| `set` | 1 | 6.8 | 6.6 | 5.9 | |
| `get` | 3 | 5.4 | 8.7 | 5.7 | 4.7 |
| `set` | 3 | 23.1 | 11.3 | 10.0 | 8.6 |
| `mixed-1w9r-p16` | 3 | 3.4 | 2.0 | 1.2 | 0.8 |
| `get` | 6 | 16.9 | 6.4 | 7.5 | 5.5 |
| `set` | 6 | 31.8 | 15.6 | 9.4 | 12.8 |
| `mixed-1w9r-p16` | 6 | 9.2 | 2.4 | 1.5 | 1.3 |

In forward mode, a SET costs 2 to 3 times the CPU of a Valkey Cluster SET, because most writes take
an extra hop. Cluster mode removes that hop. In cluster mode at 6 nodes, a SET still costs about
1.7 times the CPU of a Valkey Cluster SET. With pipelining, Discus uses about 1.6 times the CPU of
Valkey Cluster for each operation.

### Copies of each key

Discus keeps 3 copies of each key at 3 and 6 nodes. Valkey Cluster keeps 3 copies: one primary and 2
replicas. Valkey Sentinel keeps a copy on every node: 3 copies at 3 nodes and 6 copies at 6 nodes.
So Discus and Valkey Cluster store the same number of copies.

In Valkey Cluster, each pod runs 3 processes: one primary and the replicas of 2 other primaries.
These 3 processes share the 1 CPU of the pod. A Discus pod runs one process that holds both primary
and replica data.

## Why the SET rate did not change

No server pod and no client pod was throttled in any run. In cluster mode, the server pods used
about 0.5 to 0.6 of their 1 CPU during SET. Each connection waits for one reply before it sends the
next request. Thus the latency sets the rate, not the CPU. The change cuts CPU time, but not the
latency. In cluster mode with 3 and 6 nodes, the SET p50 stayed 0.6 ms to 0.9 ms above Valkey
Cluster. To close the SET gap, a change must cut the time of one write, for example in the
replication round trip.

## Limits of this benchmark

- The rc.1 and rc.2 runs took place in two sessions on the same day. The Valkey runs took place a
  day earlier. Load from other pods on the host can differ between sessions.
- The CPU values come from the cAdvisor counters of the kubelet. The kubelet updates them at
  intervals, so a value can be off by a part of the run.
- The client CPU went to 6.1 of 8 CPU in one cluster-mode SET run with 6 nodes. That run was the
  fastest of the three, so the client did not set the rate.
- In forward mode with 1 node, one GET run did 75,300. The other two runs did 83,600 and 83,800.
  The low run does not change the median.
- This run did not repeat the scale-out test of the rc.1 report.
- The CPU limit does not count kernel network work. This favors Valkey, which replicates over kernel
  TCP. Discus runs QUIC in user space, and that work counts against its 1 CPU.
- All pods and the client ran on one host, `k8s-neo-alpha`, with 32 cores. The network round trip
  does not cross a physical network.

## Files in this directory

| File | Content |
| --- | --- |
| `report.md` | This report. |
| `results-discus.jsonl` | Discus 0.5.0-rc.2 with 1, 3, and 6 nodes, in forward mode and in cluster mode. |

The file has one JSON line per run, with the same fields as the rc.1 file. To run the benchmark
again, with the `home-ops` context and Helm available, run this command from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-11-k8s-v0.5.0-rc.2/results-discus.jsonl \
  --systems discus-cluster-1-0.5.0-rc.2,discus-cluster-3-0.5.0-rc.2,discus-cluster-6-0.5.0-rc.2,discus-1-0.5.0-rc.2,discus-3-0.5.0-rc.2,discus-6-0.5.0-rc.2
```
