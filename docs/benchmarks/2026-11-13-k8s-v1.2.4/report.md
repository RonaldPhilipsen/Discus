# Benchmark study: SET throughput of Discus against Valkey

Dates: 2026-11-12 and 2026-11-13. Cluster: `home-ops`, node `k8s-neo-alpha`. Valkey 9.

In the [1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md), Discus in cluster mode did about 60%
of the Valkey Cluster SET rate. GET was almost equal. This study looks for the cause of the SET
gap. It brings together five reports. Each report has its own setup, results and commands.

| Report | Question | Discus image |
| --- | --- | --- |
| [CPU limit and worker threads](cpu-and-threads.md) | Do more CPU or more threads close the gap? | `hot-path-3bc4fea-pyroscope` |
| [Replication cost](replication-cost.md) | Does the number of copies of a key cause the gap? | `hot-path-3bc4fea-pyroscope` |
| [Profiling cost](profiling-cost.md) | How much does Pyroscope profiling cost? | `hot-path-3bc4fea-pyroscope` |
| [Event interval](event-interval.md) | Does the tokio event interval change the throughput? | `event-interval-0de9075-pyroscope` |
| [SET profile of 1.2.4](set-profile.md) | Is the gap still there in 1.2.4, and where does the CPU time go? | `1.2.4-pyroscope` |

The [CPU limit report](cpu-and-threads.md#terms) defines the terms of the study. All runs follow
the [benchmark methodology](../methodology.md). The Valkey numbers come from the 1.1.1 report,
except where a report ran Valkey again with a different setup.

## Main results

- The SET gap continues in 1.2.4. In cluster mode on 3 nodes with 1 core, Discus did 146,000
  SET/s. Valkey Cluster did 210,500, and Valkey Sentinel did 80,500.
- Replication causes the gap. With 1 copy of each key, Discus matched Valkey Cluster. Each
  extra copy cost Discus more than twice the CPU time that it cost Valkey Cluster.
- A SET does not wait for the replicas. The replication work runs on the same worker thread as
  the client connections, so it adds CPU time in front of the client replies.
- A node with 1 worker thread is limited by CPU. The cgroup counters show 0.4 to 0.6 cores per
  pod, but they leave out the softirq time. A local test found 91% to 100% use of the core.
- With 2 cores per pod, Discus did 252,400 SET/s on 3 nodes. Valkey Cluster did 190,200 with
  2 cores and `--io-threads 2`.
- Changes that do not add CPU or remove replication work did not move the SET rate. These were
  the `perf/hot-path` changes, the event interval, and replication on a thread of its own.

## SET in each setup

3 nodes, cluster mode, 1 core per pod and 1 worker thread, unless the setup says otherwise.
Operations per second, median of 3 runs, rounded to hundreds. p50 and p99 are in milliseconds.

| Setup | Report | SET/s | p50 | p99 |
| --- | --- | ---: | ---: | ---: |
| Discus 1.2.4, 3 copies (the default) | [SET profile](set-profile.md) | 146,000 | 1.92 | 3.38 |
| Discus, 1 copy | [Replication cost](replication-cost.md) | 207,600 | 1.35 | 1.82 |
| Discus, 2 copies | [Replication cost](replication-cost.md) | 167,200 | 1.70 | 2.56 |
| Discus, 3 copies | [Replication cost](replication-cost.md) | 143,700 | 1.95 | 3.42 |
| Discus, 2 cores and 2 worker threads | [CPU limit](cpu-and-threads.md) | 252,400 | 1.14 | 2.42 |
| Discus, 1 core and 2 worker threads | [CPU limit](cpu-and-threads.md) | 198,700 | 1.10 | 16.64 |
| Discus, event interval 61 (the default) | [Event interval](event-interval.md) | 149,600 | 1.93 | 3.46 |
| Discus on musl, profiling on | [Profiling cost](profiling-cost.md) | 114,800 | 2.02 | 6.78 |
| Valkey Cluster, 1 copy | [Replication cost](replication-cost.md) | 222,400 | 1.25 | 2.46 |
| Valkey Cluster, 3 copies | [1.1.1](../2026-11-12-k8s-v1.1.1/report.md) | 210,500 | 1.33 | 1.96 |
| Valkey Cluster, 3 copies, 2 cores | [CPU limit](cpu-and-threads.md) | 190,200 | 1.50 | 1.93 |
| Valkey Sentinel | [1.1.1](../2026-11-12-k8s-v1.1.1/report.md) | 80,500 | 3.52 | 4.64 |

The reports give GET, the mixed workloads, forward mode, and 1 and 6 nodes. In those workloads,
GET stays close to Valkey Cluster, and the pipelined mix shows a gap of the same kind as SET.

## Why SET is slower

Each SET on a primary queues the key for each replica. A send loop then builds batches, and
QUIC sends them to the peers. Each node also applies the batches of the other primaries. With 1
worker thread, all of this work shares the thread with the client connections.

The CPU time per SET shows the cost. In the cgroup counters, it rose from 6.6 to 14.4 µs from 1
to 3 copies. In Valkey Cluster, it rose from 5.7 to 8.7 µs. The Pyroscope profile of 1.2.4 gives
11.2 µs per SET and 7.6 µs per GET:

| Part | µs per SET |
| --- | ---: |
| Reply to the client, the same as GET | 5.0 |
| Read from the client | 0.95 |
| QUIC connections, of which UDP send is 0.43 | 0.95 |
| SET command and write bookkeeping on the primary | 0.83 |
| Replication batches from peers | 0.50 |
| Replication batches to peers | 0.35 |
| TTL sweep | 0.18 |

Replication adds about 3.5 µs per SET. The SET rate is about 1.5 times lower than the GET rate,
and SET uses about 1.5 times more CPU per operation. The core is thus the limit, and replication
work takes a part of it.

Valkey Cluster runs each replica as a separate process. Its primary process does not apply the
writes of other primaries, and the kernel can run the replica processes on other cores.

## What did not help

- The `perf/hot-path` changes cut the CPU share of single costs in local profiles by up to 12
  percentage points. On Kubernetes, SET stayed at 144,500. User time is only about a quarter of the
  CPU of a node. System calls and kernel network work take the rest.
- Event intervals from 7 to 255 stayed within 4% of the default. The value 1 raised p99 to
  11.5 ms in SET.
- Two worker threads in 1 core gave 198,700 SET/s, but the kernel throttled the pods in 82% of
  the periods. p99 rose to 16.6 ms.
- A local test put replication on a thread of its own. At normal priority, it lost 21% of the
  throughput, because the threads woke each other up. A thread that can run on a second CPU hit
  the quota in every period.

## Profiling

On musl, Pyroscope profiling cost about 20% of the throughput. An unwinder call in the signal
handler scanned all of `.eh_frame` for most samples. The
[static glibc report](https://github.com/RonaldPhilipsen/Discus/blob/e9affe8/docs/benchmarks/2026-11-13-k8s-static-glibc/report.md)
removed this cost. Release 1.2.4 builds on static glibc, and the profiling image costs at most 2%.
The 1.2.4 runs thus had profiling on. The earlier runs of this study had profiling off.

## What to do next

1. Cut the replication work per SET on the primary: fewer QUIC packets and system calls per
   write, and less write bookkeeping. Put more keys in each batch, for example with a short wait
   before each send or more than one batch in flight per replica.
2. Measure the keys per replication batch and the QUIC packets per SET.
3. Measure the softirq time of each Discus pod on the Kubernetes node. The node-wide counters
   also include the memtier pod.
4. Give pods more CPU with a matching number of worker threads, for users who need more SET
   throughput now.
5. Run the copy-count runs with 2 cores per pod. If the pod has CPU to spare, the runs show the
   replication cost alone.
6. Run Valkey with 2 cores and no I/O threads, so that the 2-core comparison uses the best
   Valkey numbers.

## Limits of this study

- Most runs use 3 nodes with 1 core. The CPU limit and replication reports also cover 1 and 6
  nodes.
- The runs use three Discus images, at different times of two days. Other workloads on the host
  can change the results between runs.
- The cgroup counters and the profiles leave out the softirq time, for Discus and for Valkey.

## Files in this directory

| File | Report |
| --- | --- |
| `results-1.2.4.jsonl` | [SET profile of 1.2.4](set-profile.md) |
| `results-1cpu-2-workers.jsonl` | [CPU limit and worker threads](cpu-and-threads.md) |
| `results-1cpu-rf1.jsonl`, `results-1cpu-copies-1.jsonl` to `results-1cpu-copies-3.jsonl`, `results-1cpu-valkey-copies-1-2.jsonl` | [Replication cost](replication-cost.md) |
| `results-1cpu-profiling-off.jsonl`, `results-1cpu-profiling-on.jsonl` | [Profiling cost](profiling-cost.md) |
| `results-1cpu-event-interval-1.jsonl` to `results-1cpu-event-interval-255.jsonl` | [Event interval](event-interval.md) |

The 1-core and 2-core runs of the CPU limit report also use files of the
[1.1.1 report](../2026-11-12-k8s-v1.1.1/) and the
[2-core report](https://github.com/RonaldPhilipsen/Discus/tree/5ce029c/docs/benchmarks/2026-11-12-k8s-2cpu-hot-path).
