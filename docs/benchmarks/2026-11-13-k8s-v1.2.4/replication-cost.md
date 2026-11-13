# Benchmark: replication cost in Discus

Date: 2026-11-12. Cluster: `home-ops`, node `k8s-neo-alpha`. Valkey 9.

In the [1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md), Discus in cluster mode did about 60%
of the Valkey Cluster SET rate. This report tests whether replication causes that gap. It
compares Discus in cluster mode with Valkey Cluster, with 1, 2 and 3 copies of each key. All runs
follow the [benchmark methodology](../methodology.md), with 1 core per pod and 1 worker thread.

This report is part of the [SET throughput study](report.md). The study page lists the other
reports.

| Run | Time | Discus image | What changes |
| --- | --- | --- | --- |
| 1.1.1 report | 11:29–13:01 | 1.1.1 | Valkey Cluster with 3 copies, 1 core per pod |
| perf/hot-path | 17:32–17:56 | `hot-path-3bc4fea-pyroscope` | Discus, 3 nodes, 3 copies (the default) |
| One copy | 18:31–18:38 | `hot-path-3bc4fea-pyroscope` | Discus, 3 nodes, `--replication-factor=1` |
| Copy count | 22:31–23:54 | `hot-path-3bc4fea-pyroscope` | 3 and 6 nodes, 1 to 3 copies in Discus and Valkey Cluster |

The `hot-path-3bc4fea-pyroscope` image is the `perf/hot-path` branch at commit 3bc4fea. It
contains the `pyroscope` feature, but profiling is off in every run. The "perf/hot-path" run is
the "Profiling off" run of the [profiling cost report](profiling-cost.md).

## Copies of a key

A copy is one stored instance of a key. The primary of the key keeps the first copy. Each other
copy is on a replica.

- In Discus, `--replication-factor` sets the number of copies: the primary plus its secondaries.
  The default is 3.
- In Valkey Cluster, the number of copies is the primary plus 0, 1 or 2 replicas. Each replica is
  a separate Valkey process in a pod of another primary.
- With 1 copy, only the primary keeps the key. If that node fails, its keys are lost. This setup
  shows the cost of replication. It is not a setup for production.

CPU per operation is the CPU time of all server pods in the cgroup counters, divided by the number
of operations, in microseconds. The cgroup counters leave out the softirq time, so the real CPU per
operation is higher. The [CPU limit report](cpu-and-threads.md#why-one-worker-thread-is-slow)
describes this.

## Main results

- Replication causes the SET gap between Discus and Valkey Cluster. With 1 copy of each key,
  Discus in cluster mode did 210,100 SET/s on 3 nodes. Valkey Cluster did 210,500. With 3
  copies, Discus did 144,500.
- With the same number of copies, Discus matched Valkey Cluster at 1 copy. The first extra copy
  cost Discus 20% to 27% of the SET rate. Valkey Cluster lost at most 5% from 1 to 3 copies.
- Each extra copy costs Discus more than twice the CPU that it costs Valkey Cluster.
- Reads did not change with the copy count in either system.
- With 16 requests in flight, Discus was slower than Valkey Cluster at every copy count.

## One copy against three copies

This run compares Discus in cluster mode with 3 copies (the default) and with 1 copy
(`--replication-factor=1`). It uses 3 nodes with 1 core each. The Valkey Cluster numbers have 3
copies and come from the 1.1.1 report. The numbers are in
[One copy and three copies, 3 nodes](#one-copy-and-three-copies-3-nodes) at the end of this
report.

With 1 copy, Discus matched Valkey Cluster in SET, GET and the mix without pipelining. The 3
copies add about 0.6 ms to the SET p50. The CPU per SET grows from 5.1 to 12.9 microseconds.
Most of that CPU is in system calls, not in the replication code of Discus. Valkey Cluster also
keeps 3 copies, but its replication adds almost no latency.

A primary sends one batch at a time to each replica. It waits for the reply before it sends the
next batch. With this load, the batches are small, so a SET pays for many small QUIC packets.

## Same number of copies as Valkey Cluster

This run gives Discus and Valkey Cluster the same number of copies of each key: 1, 2 or 3. Both
systems run with 1 core per pod, on 3 and 6 nodes. The Valkey numbers for 3 copies come from the
1.1.1 report. All other numbers come from this run. The numbers are in the
[result tables](#result-tables) at the end of this report.

With 1 copy, Discus was within 7% of Valkey Cluster in SET, GET and the mix without pipelining.
With 6 nodes, the two were equal. Reads did not change with the copy count in either system.

The first extra copy costs Discus the most. From 1 to 2 copies, the SET rate fell 20% with 3
nodes and 27% with 6 nodes. The SET p50 rose by about 0.35 to 0.5 ms. The third copy cost
another 11% to 14%. Valkey Cluster lost at most 5% from 1 to 3 copies.

Both systems send the reply to a write before the replicas have the write. The extra latency in
Discus thus does not come from a wait for a replica. It comes from the replication work in the
same pod: the send loops, the QUIC packets, and the batches from other primaries. With 1 worker
thread, this work runs on the same thread as the client connections.

Replication costs CPU in both systems. On 3 nodes, the CPU per SET in Discus rose from 6.6 to
14.4 microseconds from 1 to 3 copies. In Valkey Cluster, it rose from 5.7 to 8.7 microseconds.
The two extra copies thus cost Discus more than twice the CPU that they cost Valkey Cluster.

The cgroup counters showed no throttled periods. But these counters leave out the softirq time.
A local test found that a node with 1 worker thread is probably limited by CPU. In the same test,
replication on a thread of its own did not help. The
[CPU limit report](cpu-and-threads.md#why-one-worker-thread-is-slow) has the details.

With pipelining (16 in flight), Discus was slower than Valkey Cluster at every copy count. The
gap was 22% with 1 copy and 38% to 39% with 3 copies.

Valkey Sentinel keeps one copy on each node and sends every write to one primary. At 1 core, it
did 80,500 SET/s with 3 nodes and 77,300 with 6 nodes. These numbers come from the
[1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md).

## What to do next

1. Measure the keys per replication batch and the QUIC packets per SET.
2. Test a short wait before each send, or more than one batch in flight per replica. Both
   changes put more keys in each packet.
3. Run the copy-count runs with 2 cores per pod. If the pod has CPU to spare, the runs show the
   replication cost alone.

## Limits of this benchmark

- The runs took place at different times of the day. Other workloads on the host can change the
  results between runs.
- The cgroup counters leave out the softirq time. The CPU per operation is thus lower than the
  real CPU use of both systems.

## Files in this directory

- `results-1cpu-rf1.jsonl`: 3 nodes, cluster mode, 1 copy of each key.
- `results-1cpu-copies-1.jsonl`, `results-1cpu-copies-2.jsonl`, `results-1cpu-copies-3.jsonl`:
  Discus in cluster mode, 3 and 6 nodes, 1, 2 or 3 copies of each key.
- `results-1cpu-valkey-copies-1-2.jsonl`: Valkey Cluster, 3 and 6 nodes, 0 or 1 replicas.

The "3 copies" column of the first table comes from `results-1cpu-profiling-off.jsonl`. The Valkey
Cluster numbers with 3 copies come from the [1.1.1 report](../2026-11-12-k8s-v1.1.1/). Each line has
the memtier results and the cgroup CPU counters of each pod.
[`scripts/bench-k8s-cpu.py`](../../../scripts/bench-k8s-cpu.py) prints the medians, the CPU per
operation and the throttled periods from these files.

The copy-count run used this command, once for each `--replication-factor` value:

```sh
BENCH_CPUS=1 DISCUS_CHART=$PWD/charts/discus \
  DISCUS_HELM_SET='extraArgs={--replication-factor=2}' python3 scripts/bench-k8s.py \
  --systems discus-cluster-3-hot-path-pyro,discus-cluster-6-hot-path-pyro \
  --out results-1cpu-copies-2.jsonl
SYSTEMS=valkey-cluster-noreplica-3,valkey-cluster-noreplica-6
SYSTEMS=$SYSTEMS,valkey-cluster-replica-3,valkey-cluster-replica-6
BENCH_CPUS=1 python3 scripts/bench-k8s.py --systems "$SYSTEMS" \
  --out results-1cpu-valkey-copies-1-2.jsonl
```

## Result tables

The throughput is in operations per second, as the median of 3 runs of 30 seconds, rounded to
hundreds. p50 and p99 are in milliseconds. CPU per op is in microseconds, and lower is better.

### One copy and three copies, 3 nodes

| Workload | Discus, 3 copies | Discus, 1 copy | Valkey Cluster | p50, 3 copies | p50, 1 copy | p50, Valkey Cluster | CPU per op, 3 copies | CPU per op, 1 copy | CPU per op, Valkey Cluster |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 218,500 | 222,700 | 228,100 | 1.28 | 1.26 | 1.21 | 7.0 | 5.7 | 6.8 |
| SET | 144,500 | 210,100 | 210,500 | 1.92 | 1.35 | 1.33 | 12.9 | 5.1 | 8.7 |
| Mixed (1 write, 9 reads) | 194,700 | 219,300 | 219,300 | 1.42 | 1.28 | 1.26 | 8.2 | 6.4 | 7.4 |
| Mixed, 16 in flight | 1,068,800 | 1,509,300 | 1,723,200 | 4.22 | 3.02 | 2.53 | 2.0 | 1.3 | 1.2 |

### Same number of copies, 3 nodes

| Workload | Copies | Discus | Valkey Cluster | p50, Discus | p50, Valkey | p99, Discus | p99, Valkey | CPU per op, Discus | CPU per op, Valkey |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SET | 1 | 207,600 | 222,400 | 1.35 | 1.25 | 1.82 | 2.46 | 6.6 | 5.7 |
| SET | 2 | 167,200 | 213,300 | 1.70 | 1.30 | 2.56 | 2.40 | 9.4 | 8.1 |
| SET | 3 | 143,700 | 210,500 | 1.95 | 1.33 | 3.42 | 1.96 | 14.4 | 8.7 |
| GET | 1 | 228,400 | 226,700 | 1.27 | 1.21 | 1.74 | 2.38 | 5.3 | 5.7 |
| GET | 2 | 226,300 | 228,200 | 1.27 | 1.20 | 1.81 | 2.37 | 7.5 | 5.8 |
| GET | 3 | 220,000 | 228,100 | 1.30 | 1.21 | 2.08 | 2.38 | 6.8 | 6.8 |
| Mixed (1 write, 9 reads) | 1 | 219,900 | 226,700 | 1.29 | 1.22 | 1.75 | 2.38 | 5.1 | 5.6 |
| Mixed (1 write, 9 reads) | 2 | 201,800 | 229,300 | 1.40 | 1.25 | 1.99 | 2.43 | 6.9 | 6.1 |
| Mixed (1 write, 9 reads) | 3 | 202,400 | 219,300 | 1.44 | 1.26 | 2.27 | 2.45 | 6.9 | 7.4 |
| Mixed, 16 in flight | 1 | 1,483,000 | 1,905,800 | 3.04 | 2.30 | 3.95 | 4.42 | 1.3 | 1.0 |
| Mixed, 16 in flight | 2 | 1,185,100 | 1,797,500 | 3.84 | 2.38 | 5.28 | 4.45 | 1.4 | 1.0 |
| Mixed, 16 in flight | 3 | 1,065,000 | 1,723,200 | 4.22 | 2.53 | 6.33 | 4.45 | 1.8 | 1.2 |

### Same number of copies, 6 nodes

| Workload | Copies | Discus | Valkey Cluster | p50, Discus | p50, Valkey | p99, Discus | p99, Valkey | CPU per op, Discus | CPU per op, Valkey |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SET | 1 | 390,700 | 388,700 | 1.43 | 1.39 | 2.30 | 2.50 | 7.0 | 7.3 |
| SET | 2 | 284,500 | 381,800 | 1.92 | 1.46 | 3.13 | 2.59 | 12.1 | 8.6 |
| SET | 3 | 245,100 | 374,900 | 2.29 | 1.46 | 3.90 | 2.67 | 13.7 | 11.1 |
| GET | 1 | 408,300 | 409,300 | 1.39 | 1.38 | 2.22 | 2.45 | 6.5 | 6.8 |
| GET | 2 | 405,000 | 409,600 | 1.39 | 1.38 | 2.24 | 2.45 | 6.6 | 6.5 |
| GET | 3 | 413,400 | 419,900 | 1.40 | 1.38 | 2.29 | 2.48 | 6.5 | 6.2 |
| Mixed (1 write, 9 reads) | 1 | 408,800 | 406,300 | 1.39 | 1.38 | 2.21 | 2.46 | 7.0 | 6.8 |
| Mixed (1 write, 9 reads) | 2 | 332,400 | 399,000 | 1.65 | 1.39 | 2.61 | 2.50 | 9.3 | 7.0 |
| Mixed (1 write, 9 reads) | 3 | 334,400 | 397,700 | 1.68 | 1.38 | 2.78 | 2.48 | 10.4 | 8.1 |
| Mixed, 16 in flight | 1 | 2,628,700 | 3,168,800 | 3.39 | 2.93 | 5.15 | 4.99 | 1.4 | 1.1 |
| Mixed, 16 in flight | 2 | 2,064,100 | 3,021,500 | 4.38 | 3.01 | 6.27 | 4.89 | 1.8 | 1.3 |
| Mixed, 16 in flight | 3 | 1,843,500 | 3,026,100 | 5.09 | 3.06 | 7.58 | 4.83 | 2.1 | 1.4 |
