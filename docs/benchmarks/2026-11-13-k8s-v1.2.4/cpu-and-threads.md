# Benchmark: CPU limit and worker threads in Discus

Date: 2026-11-12. Cluster: `home-ops`, node `k8s-neo-alpha`. Valkey 9.

In the [1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md), Discus in cluster mode did about 60%
of the Valkey Cluster SET rate. This report tests whether more CPU or more threads change that
result. All runs follow the [benchmark methodology](../methodology.md), with the changes that
each section names.

This report is part of the [SET throughput study](report.md). The study page lists the other
reports.

| Run | Time | Discus image | What changes |
| --- | --- | --- | --- |
| 1.1.1 report | 11:29–13:01 | 1.1.1 | Nothing. The reference run, 1 core per pod. |
| perf/hot-path | 17:32–17:56 | `hot-path-3bc4fea-pyroscope` | 3 nodes, 1 core, the new image |
| 2 cores | 19:42–21:13 | `hot-path-3bc4fea-pyroscope` | 2 cores per pod, Valkey `--io-threads 2` |
| 2 workers | 21:33–22:22 | `hot-path-3bc4fea-pyroscope` | 1 core per pod, `TOKIO_WORKER_THREADS=2` |

The `hot-path-3bc4fea-pyroscope` image is the `perf/hot-path` branch at commit 3bc4fea. It
contains the `pyroscope` feature, but profiling is off in every run of this report. The
"perf/hot-path" run is the "Profiling off" run of the profiling cost report.

## Terms

The reports of this study use these terms:

- Cluster mode: memtier reads the slot map from port 6380 and sends each command to the primary
  of its key.
- Forward mode: memtier connects to port 6379 of every pod. The nodes forward the commands that
  they do not own.
- Closed-loop load: each connection waits for a reply before it sends the next request. The
  throughput is then about the number of connections divided by the latency.
- Worker thread: a tokio thread that runs the Discus tasks. By default, Discus starts one worker
  thread for each core of its CPU limit.
- CPU limit: a time budget. With 1 core, a pod can use 100 ms of CPU in each 100 ms period, on
  any core.
- Throttled period: a period in which the pod used all of its budget and had to wait for the
  next period.
- p50 and p99: the latency that 50% and 99% of the requests stay under, in milliseconds.
- Cores used: the CPU time of all server pods in the cgroup counters, divided by the run time.
- Softirq time: the CPU time that the kernel uses to send and receive network packets. The
  cgroup counters do not include it.

## Main results

- With 2 cores per pod, Discus in cluster mode did 252,400 SET/s and 379,700 GET/s on 3 nodes.
  Valkey Cluster did 190,200 and 208,800 in the same run.
- Discus got 50% to 100% faster with the second core in most workloads. Valkey did not get
  faster with 2 cores and `--io-threads 2`.
- Two worker threads in 1 core gave most of the 2-core throughput. The pods were then throttled
  in up to 87% of the periods, and p99 rose as high as 68 ms. This is not a good default.
- With 1 core and 1 worker thread, the cgroup counters showed 0.4 to 0.6 cores per pod and no
  throttled periods. A local test found that these counters leave out the softirq time. With
  the softirq time, a node with 1 worker thread used 91% to 100% of its core.
- The `perf/hot-path` code changes cut the user CPU per command in local profiles. They did not
  change the Kubernetes throughput with 1 core.
- Replication causes the SET gap. With 1 copy of each key, Discus matched Valkey Cluster. The
  [replication cost report](replication-cost.md) has the details.

## The CPU limit: 1 core against 2 cores

The 1-core numbers come from the 1.1.1 report. The 2-core numbers come from the
[2-core report](https://github.com/RonaldPhilipsen/Discus/blob/5ce029c/docs/benchmarks/2026-11-12-k8s-2cpu-hot-path/report.md).
The two runs use different Discus images. With 1 core and 3 nodes, the `perf/hot-path` image was
within 4% of 1.1.1 in every workload. The gain thus comes from the second core. The numbers are in
[Result tables](#result-tables) at the end of this report.

With the second core, Discus got 50% to 100% faster in most workloads. With 1 node, it did almost
twice the rate: 149,600 SET/s against 76,800. Valkey stayed flat or got slower. Only the
pipelined mix got faster for Valkey, but not for Valkey Cluster with 6 nodes.

Valkey ran with `--io-threads 2` in the 2-core run. Each Valkey Cluster pod runs three processes:
one primary and two replicas of other primaries. Each process starts one I/O thread of its own,
so a pod runs three main threads and three I/O threads within 2 cores. The main thread of each
process still runs every command. The I/O threads used 2.5 to 3 times more CPU, but they did not
lower the p50 latency.

The latency shows why Discus gained. With 1 node, the Discus GET p50 fell from 1.14 ms to
0.61 ms. With a closed-loop load, half the latency gives about twice the throughput. The Valkey
p50 did not change.

With 6 nodes, Discus in cluster mode did fewer GET/s with 2 cores than with 1 core: 365,100
against 397,600. Valkey Cluster with 6 nodes also got slower in every workload. The 6-node runs
with 2 cores ask for 12 server cores on a Kubernetes node that other workloads also use. The
host can thus limit the 6-node numbers.

## Two worker threads in 1 core

This run sets `TOKIO_WORKER_THREADS=2` through the chart value `extraEnv`. The CPU limit stays at
1 core. The 1-worker numbers come from the 1.1.1 report, in which no period was throttled.

With 1 and 3 nodes in cluster mode, the throughput came close to the 2-core numbers. For
example, Discus did 382,800 GET/s on 3 nodes with 2 workers, and 379,700 with 2 cores. In
forward mode with 6 nodes, the second worker gave no gain.

The pods used all of their budget, and the kernel throttled them in most periods. A throttled
pod holds every request until the next period starts. In the SET and pipelined workloads, p99
rose to between 16 and 68 ms in most setups. With 2 real cores, p99 stayed between 1 and 7.5 ms,
except in forward mode with 6 nodes.

### Why one worker thread is slow

With 1 worker thread, the cgroup counters showed 0.4 to 0.6 cores per pod. The pods thus looked
limited by latency, not by CPU. A local test on 2026-11-13 showed that the counters leave out
the softirq time. This result is not yet confirmed on the Kubernetes node.

The local test ran 3 nodes with a quota of 1 core and 1 worker thread each. The load was SET
with 3 copies, from 96 connections per node. The counters in `/proc/stat` showed each worker
CPU 91% to 100% busy:

- User time: about 24%.
- System time: about 43%.
- Softirq time: 30% to 33%.

For the same node, the cgroup counters showed 0.67 cores. A node with 1 worker thread is thus
probably limited by CPU. The second worker gives the pod more CPU time: about 1.33 cores for
each node, with the softirq time.

The same test moved the replication work to a thread of its own. This did not help. At normal
priority, the separate thread lost 21% of the throughput, because the two threads woke each
other up. A thread that can run on a second CPU hit the quota in every period.

## The perf/hot-path changes

The `perf/hot-path` branch removes costs per command that a CPU profile showed. In a local run
with 3 nodes and 1 core per node, the share of the CPU for each cost changed as follows:

- The epoch read lock in `Node::owners`: from 5–11% to under 1%.
- Node frees and allocations in `WriteSet::record`: from 13% to about 4% on SET.
- The stop future of each connection: from 13% to about 1% on GET.
- The command lookup: half of its earlier cost.
- `Replicator::written`: about 11% on SET, almost unchanged.

The local SET throughput rose by about 6%, from 463,000–479,000 to 498,000–505,000 ops/s. The
local benchmark settles at one of two throughput levels. This comparison uses only runs at the
same level.

On Kubernetes with 1 core and 3 nodes, the branch did not change the throughput. Discus in
cluster mode, in operations per second:

| Workload | Discus 1.1.1 | Discus perf/hot-path | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 214,500 | 218,500 | 228,100 | 85,700 |
| SET | 142,000 | 144,500 | 210,500 | 80,500 |
| Mixed (1 write, 9 reads) | 202,100 | 194,700 | 219,300 | 83,300 |
| Mixed, 16 in flight | 1,049,600 | 1,068,800 | 1,723,200 | 633,700 |

The branch cuts user CPU. In the local test above, the user time was about a quarter of the CPU
of a node. Most of the CPU went to system calls and to network work in the kernel.

## What to do next

1. Measure the softirq time on node `k8s-neo-alpha` during a run, with `/proc/stat`. This
   confirms or rejects the local result.
2. Cut the kernel work per SET: fewer network packets and fewer system calls.
3. Run Valkey with 2 cores and no I/O threads. Then the 2-core comparison uses the best Valkey
   numbers.
4. If the host has less other load, run the 6-node setups again.

## Limits of this benchmark

- The runs use two Discus images: 1.1.1 and the `perf/hot-path` build.
- The runs took place at different times of the day. Other workloads on the host can change the
  results between runs.
- The memtier client used up to 6.8 of its 8 cores in one run.
- The cgroup counters leave out the softirq time, for Discus and for Valkey. The "cores used"
  numbers are thus lower than the real CPU use of both systems.

## Files in this directory

- `results-1cpu-2-workers.jsonl`: 1, 3 and 6 nodes, 1 core, 2 worker threads.

The [replication cost report](replication-cost.md) and the
[profiling cost report](profiling-cost.md) describe the other result files in this directory. The
"perf/hot-path" run comes from `results-1cpu-profiling-off.jsonl`. The other results are in the
[1.1.1 report](../2026-11-12-k8s-v1.1.1/) and the
[2-core report](https://github.com/RonaldPhilipsen/Discus/tree/5ce029c/docs/benchmarks/2026-11-12-k8s-2cpu-hot-path).
Each line has the memtier results and the cgroup CPU counters of each pod.
[`scripts/bench-k8s-cpu.py`](../../../scripts/bench-k8s-cpu.py) prints the medians, the CPU per
operation and the throttled periods from these files.

The 2-workers run used this command:

```sh
SYSTEMS=discus-cluster-1-hot-path-pyro,discus-cluster-3-hot-path-pyro
SYSTEMS=$SYSTEMS,discus-cluster-6-hot-path-pyro,discus-1-hot-path-pyro
SYSTEMS=$SYSTEMS,discus-3-hot-path-pyro,discus-6-hot-path-pyro
BENCH_CPUS=1 DISCUS_CHART=$PWD/charts/discus \
  DISCUS_HELM_SET_STRING='extraEnv[0].name=TOKIO_WORKER_THREADS,extraEnv[0].value=2' \
  python3 scripts/bench-k8s.py --systems "$SYSTEMS" --out results-1cpu-2-workers.jsonl
```

## Result tables

The throughput is in operations per second, as the median of 3 runs of 30 seconds, rounded to
hundreds. p50 and p99 are in milliseconds. A latency cell gives p50 / p99.

### Throughput, 1 core and 2 cores

With 1 node:

| Workload | Discus cluster, 1 core | Discus cluster, 2 cores | Discus forward, 1 core | Discus forward, 2 cores | Valkey, 1 core | Valkey, 2 cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 83,500 | 155,900 | 81,600 | 157,100 | 86,700 | 85,100 |
| SET | 76,800 | 149,600 | 75,200 | 150,500 | 84,200 | 81,500 |
| Mixed (1 write, 9 reads) | 82,700 | 154,700 | 81,700 | 154,300 | 86,900 | 84,900 |
| Mixed, 16 in flight | 490,900 | 970,700 | 489,800 | 960,800 | 670,500 | 740,700 |

With 3 nodes:

| Workload | Discus cluster, 1 core | Discus cluster, 2 cores | Discus forward, 1 core | Discus forward, 2 cores | Valkey Cluster, 1 core | Valkey Cluster, 2 cores | Valkey Sentinel, 1 core | Valkey Sentinel, 2 cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 214,500 | 379,700 | 218,900 | 337,900 | 228,100 | 208,800 | 85,700 | 82,900 |
| SET | 142,000 | 252,400 | 89,600 | 141,600 | 210,500 | 190,200 | 80,500 | 75,600 |
| Mixed (1 write, 9 reads) | 202,100 | 351,100 | 174,300 | 288,200 | 219,300 | 201,600 | 83,300 | 81,400 |
| Mixed, 16 in flight | 1,049,600 | 1,826,400 | 695,300 | 1,177,300 | 1,723,200 | 1,929,500 | 633,700 | 735,300 |

With 6 nodes:

| Workload | Discus cluster, 1 core | Discus cluster, 2 cores | Discus forward, 1 core | Discus forward, 2 cores | Valkey Cluster, 1 core | Valkey Cluster, 2 cores | Valkey Sentinel, 1 core | Valkey Sentinel, 2 cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 397,600 | 365,100 | 205,000 | 265,300 | 419,900 | 347,400 | 85,000 | 82,100 |
| SET | 240,700 | 356,500 | 123,900 | 186,400 | 374,900 | 326,200 | 77,300 | 70,700 |
| Mixed (1 write, 9 reads) | 322,000 | 355,100 | 178,400 | 243,600 | 397,700 | 338,600 | 82,100 | 80,200 |
| Mixed, 16 in flight | 1,800,800 | 2,747,400 | 522,800 | 838,000 | 3,026,100 | 2,516,100 | 589,700 | 675,000 |

### Latency in cluster mode, 1 core and 2 cores

With 1 node:

| Workload | Discus, 1 core | Discus, 2 cores | Valkey, 1 core | Valkey, 2 cores |
| --- | ---: | ---: | ---: | ---: |
| GET | 1.14 / 1.67 | 0.61 / 1.01 | 1.07 / 2.10 | 1.10 / 1.46 |
| SET | 1.24 / 1.85 | 0.66 / 1.09 | 1.11 / 2.17 | 1.16 / 1.50 |
| Mixed (1 write, 9 reads) | 1.14 / 1.79 | 0.61 / 1.02 | 1.07 / 2.08 | 1.11 / 1.45 |
| Mixed, 16 in flight | 3.10 / 4.01 | 1.59 / 2.53 | 2.25 / 3.17 | 1.97 / 2.61 |

With 3 nodes:

| Workload | Discus, 1 core | Discus, 2 cores | Valkey Cluster, 1 core | Valkey Cluster, 2 cores |
| --- | ---: | ---: | ---: | ---: |
| GET | 1.30 / 2.02 | 0.75 / 1.32 | 1.21 / 2.38 | 1.38 / 1.81 |
| SET | 1.95 / 3.47 | 1.14 / 2.42 | 1.33 / 1.96 | 1.50 / 1.93 |
| Mixed (1 write, 9 reads) | 1.43 / 2.27 | 0.83 / 1.55 | 1.26 / 2.45 | 1.42 / 1.85 |
| Mixed, 16 in flight | 4.29 / 6.37 | 2.45 / 4.54 | 2.53 / 4.45 | 2.45 / 3.26 |

With 6 nodes:

| Workload | Discus, 1 core | Discus, 2 cores | Valkey Cluster, 1 core | Valkey Cluster, 2 cores |
| --- | ---: | ---: | ---: | ---: |
| GET | 1.41 / 2.29 | 1.48 / 2.75 | 1.38 / 2.48 | 1.62 / 2.80 |
| SET | 2.33 / 3.95 | 1.58 / 2.85 | 1.46 / 2.67 | 1.77 / 2.59 |
| Mixed (1 write, 9 reads) | 1.69 / 2.77 | 1.55 / 2.96 | 1.38 / 2.48 | 1.72 / 2.54 |
| Mixed, 16 in flight | 5.02 / 7.58 | 3.29 / 6.08 | 3.06 / 4.83 | 3.68 / 6.33 |

### Throughput and p99 with 2 worker threads

The p99 columns are in milliseconds. The last column is the share of throttled periods with 2
workers.

Cluster mode, 1 node:

| Workload | 1 worker, 1 core | 2 workers, 1 core | 2 cores | p99, 1 worker | p99, 2 workers | p99, 2 cores | Throttled periods, 2 workers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 83,500 | 152,200 | 155,900 | 1.67 | 1.05 | 1.01 | 31% |
| SET | 76,800 | 132,100 | 149,600 | 1.85 | 1.35 | 1.09 | 82% |
| Mixed (1 write, 9 reads) | 82,700 | 156,900 | 154,700 | 1.79 | 1.13 | 1.02 | 47% |
| Mixed, 16 in flight | 490,900 | 754,000 | 970,700 | 4.01 | 17.66 | 2.53 | 78% |

Cluster mode, 3 nodes:

| Workload | 1 worker, 1 core | 2 workers, 1 core | 2 cores | p99, 1 worker | p99, 2 workers | p99, 2 cores | Throttled periods, 2 workers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 214,500 | 382,800 | 379,700 | 2.02 | 1.58 | 1.32 | 68% |
| SET | 142,000 | 198,700 | 252,400 | 3.47 | 16.64 | 2.42 | 82% |
| Mixed (1 write, 9 reads) | 202,100 | 327,400 | 351,100 | 2.27 | 2.65 | 1.55 | 87% |
| Mixed, 16 in flight | 1,049,600 | 1,285,600 | 1,826,400 | 6.37 | 33.02 | 4.54 | 86% |

Cluster mode, 6 nodes:

| Workload | 1 worker, 1 core | 2 workers, 1 core | 2 cores | p99, 1 worker | p99, 2 workers | p99, 2 cores | Throttled periods, 2 workers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 397,600 | 378,400 | 365,100 | 2.29 | 2.70 | 2.75 | 7% |
| SET | 240,700 | 324,800 | 356,500 | 3.95 | 17.79 | 2.85 | 77% |
| Mixed (1 write, 9 reads) | 322,000 | 376,600 | 355,100 | 2.77 | 9.47 | 2.96 | 86% |
| Mixed, 16 in flight | 1,800,800 | 2,160,200 | 2,747,400 | 7.58 | 34.05 | 6.08 | 87% |

Forward mode, 1 node:

| Workload | 1 worker, 1 core | 2 workers, 1 core | 2 cores | p99, 1 worker | p99, 2 workers | p99, 2 cores | Throttled periods, 2 workers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 81,600 | 152,100 | 157,100 | 1.73 | 1.05 | 1.01 | 30% |
| SET | 75,200 | 136,400 | 150,500 | 1.97 | 1.72 | 1.10 | 73% |
| Mixed (1 write, 9 reads) | 81,700 | 155,200 | 154,300 | 1.80 | 1.11 | 1.04 | 48% |
| Mixed, 16 in flight | 489,800 | 746,100 | 960,800 | 4.09 | 19.71 | 2.54 | 77% |

Forward mode, 3 nodes:

| Workload | 1 worker, 1 core | 2 workers, 1 core | 2 cores | p99, 1 worker | p99, 2 workers | p99, 2 cores | Throttled periods, 2 workers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 218,900 | 331,100 | 337,900 | 2.29 | 1.63 | 1.45 | 63% |
| SET | 89,600 | 96,500 | 141,600 | 6.46 | 25.47 | 4.80 | 80% |
| Mixed (1 write, 9 reads) | 174,300 | 255,100 | 288,200 | 6.82 | 9.47 | 3.36 | 85% |
| Mixed, 16 in flight | 695,300 | 745,200 | 1,177,300 | 11.39 | 40.70 | 7.52 | 85% |

Forward mode, 6 nodes:

| Workload | 1 worker, 1 core | 2 workers, 1 core | 2 cores | p99, 1 worker | p99, 2 workers | p99, 2 cores | Throttled periods, 2 workers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 205,000 | 212,300 | 265,300 | 7.17 | 22.91 | 4.89 | 85% |
| SET | 123,900 | 119,200 | 186,400 | 8.77 | 37.12 | 6.33 | 85% |
| Mixed (1 write, 9 reads) | 178,400 | 184,800 | 243,600 | 7.87 | 27.77 | 5.57 | 84% |
| Mixed, 16 in flight | 522,800 | 477,600 | 838,000 | 48.38 | 68.09 | 37.38 | 84% |
