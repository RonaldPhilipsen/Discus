# Benchmark: Discus 1.2.4 against Valkey Cluster and Valkey Sentinel

Date: 2026-11-13, 12:47–13:03 UTC. Cluster: `home-ops`, node `k8s-neo-alpha`. 3 nodes with 1 core
and 2 GiB each. Valkey 9.

The test measures the SET throughput of Discus 1.2.4 and finds where the CPU time of a SET goes.
The runs follow the [benchmark methodology](../methodology.md). Profiling was on in every run.
This report is part of the [SET throughput study](report.md).

## Setup

| Item | Value |
| --- | --- |
| Discus image | `1.2.4-pyroscope`: commit e9affe8 (tag v1.2.4), built with `--build-arg FEATURES=pyroscope` |
| Chart | The repository chart at commit e9affe8 |
| Rounds | 3. Each round runs cluster mode, then forward mode. |
| Load | memtier, 96 connections per node, 256 B values, 1,000,000 keys, 30 s per run |
| Replication factor | 3 (the default) |

Cluster mode means that memtier reads the slot map and sends each command to the primary of its
key. Forward mode means that memtier connects to every pod, and the nodes forward the commands
that they do not own.

The Valkey numbers come from the [1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md). Those runs
used the same node and the same load on 2026-11-12.

## Throughput

Operations per second, median of 3 runs, rounded to hundreds.

| Workload | Discus forward | Discus cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 224,000 | 213,600 | 228,100 | 85,700 |
| SET | 90,300 | 146,000 | 210,500 | 80,500 |
| Mixed (1 write, 9 reads) | 167,000 | 196,300 | 219,300 | 83,300 |
| Mixed, 16 in flight | 707,600 | 1,080,700 | 1,723,200 | 633,700 |

The three cluster-mode SET runs did 146,300, 146,000 and 144,800 operations per second. The
three forward-mode SET runs did 92,300, 90,300 and 90,200.

## Latency

p50 and p99 in milliseconds, median of 3 runs.

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.26 | 2.21 | 1.31 | 2.06 | 1.21 | 2.38 | 3.28 | 4.64 |
| SET | 3.76 | 6.43 | 1.92 | 3.38 | 1.33 | 1.96 | 3.52 | 4.64 |
| Mixed (1 write, 9 reads) | 1.38 | 6.88 | 1.44 | 2.27 | 1.26 | 2.45 | 3.39 | 4.74 |
| Mixed, 16 in flight | 7.33 | 11.26 | 4.19 | 6.24 | 2.53 | 4.45 | 7.17 | 8.96 |

## Where the CPU time of a SET goes

The table shows the CPU time of the three Discus pods per operation, from the Pyroscope profile
of round 1 in cluster mode. The SET window is 12:48:45–12:49:08 UTC, and the GET window is
12:48:10–12:48:33 UTC. A function counts with the time of the functions that it calls.

| Part | SET, µs per op | GET, µs per op |
| --- | ---: | ---: |
| Reply to the client (`__libc_send`) | 5.02 | 5.24 |
| Read from the client (`__libc_recv`) | 0.95 | 0.91 |
| SET command on the primary (`cluster::local`) | 0.83 | |
| QUIC connections (`quinn` `ConnectionDriver`) | 0.95 | |
| UDP send in QUIC (`__libc_sendmsg`, part of the line above) | 0.43 | |
| Replication batches from peers (`NodeService::call`) | 0.50 | |
| Replication batches to peers (`replication::fill`) | 0.35 | |
| TTL sweep (`Store::sweep_expired`) | 0.18 | 0.03 |
| All CPU time | 11.18 | 7.63 |

Rounds 2 and 3 give the same picture. The client reply takes 41% to 45% of the CPU time in the
SET runs and 66% to 69% in the GET runs. The profile totals depend on how many 10-second uploads
fall in a window, so each value can be about 10% off.

A reply to the client costs about 5 µs for both workloads. In SET, the parts of replication add
about 3.5 µs per operation on top of a GET: the QUIC connections, the batches to and from the
peers, and the write bookkeeping of the SET command. The SET command does not wait for the
replicas before it replies, so this cost adds CPU time, not waiting time.

In this window, the pods used 1.64 cores in all for SET and 1.70 cores for GET. The profiles and
the cgroup counters do not include softirq time. The SET rate is about 1.5 times lower than the
GET rate, and SET uses about 1.5 times more CPU per operation. The
[earlier CPU report](cpu-and-threads.md) found a similar result. With
the softirq time, a node with 1 worker thread used almost all of its core.

## Conclusion

In cluster mode, Discus 1.2.4 does 146,000 SET/s, and Valkey Cluster does 210,500. The gap
continues. The data points to the CPU time of replication on each node: about 3.5 µs per SET.
For Discus to reach the Valkey Cluster SET rate, that cost must drop to almost zero.

## Limits of this benchmark

- The runs cover 3 nodes with 1 core only.
- The breakdown comes from one 23-second window per workload.
- The node-wide softirq counters include the memtier pod, so the test does not give the softirq
  time of each Discus pod.

## Files in this directory

- `results-1.2.4.jsonl`: one line for each run, with the memtier results and the cgroup CPU
  counters of each pod.

You run these commands from the repository root. The first command builds the image from a clean
copy of commit e9affe8 and pushes it.

```sh
git archive e9affe8 | tar -x -C /path/to/src && cd /path/to/src
docker build --build-arg FEATURES=pyroscope -t ghcr.io/ronaldphilipsen/discus:1.2.4-pyroscope .
docker push ghcr.io/ronaldphilipsen/discus:1.2.4-pyroscope
```

```sh
export BENCH_CPUS=1 DISCUS_CHART=$PWD/charts/discus
python3 scripts/bench-k8s.py --reps 3 --systems discus-cluster-3-1.2.4-pyro,discus-3-1.2.4-pyro \
  --out docs/benchmarks/2026-11-13-k8s-v1.2.4/results-1.2.4.jsonl
```
