# Benchmark: Discus 0.4.1-rc.1 with 1, 3, and 6 nodes

Date: 2026-11-11. Cluster: `home-ops`. Discus 0.4.1-rc.1, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). The Discus runs took place from
08:19 to 08:44. Discus 0.4.1-rc.1 comes from pull request #49, commit `cb738e4`. It makes a
forwarded command cheaper. The wire format and the placement do not change. The Valkey numbers
come from the [2026-11-10 report](../2026-11-10-k8s-v0.3.0/report.md).

## Main results

- Discus completed every run, including all pipelined runs. No GET missed, and every node reported
  1,000,000 keys after the runs.
- With 1 node, Discus did 88% to 94% of the Valkey rate without pipelining and 64% with
  pipelining.
- With 3 nodes, Discus did 98% of the Valkey Cluster GET rate, but only 42% of the SET rate.
- With 6 nodes, Discus did 48% of the Valkey Cluster GET rate and 32% of the SET rate.
- Discus did more than Valkey Sentinel in every workload except the pipelined one.

## Throughput

Operations per second, median of 3 runs of 30 seconds. The range of the three runs was within 3%
of the median for every setup. A share is the Discus rate divided by the Valkey rate.

### Throughput, 1 node

| Workload | Discus | Valkey | Share |
| --- | ---: | ---: | ---: |
| GET | 82,300 | 87,500 | 94% |
| SET | 74,600 | 85,200 | 88% |
| Mixed (1 write, 9 reads) | 81,200 | 86,700 | 94% |
| Mixed, 16 in flight | 427,800 | 669,500 | 64% |

### Throughput, 3 nodes

| Workload | Discus | Valkey Cluster | Valkey Sentinel | Share of Cluster | Share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 221,500 | 226,700 | 87,000 | 98% | 255% |
| SET | 86,800 | 207,700 | 80,600 | 42% | 108% |
| Mixed (1 write, 9 reads) | 172,100 | 217,900 | 85,100 | 79% | 202% |
| Mixed, 16 in flight | 635,500 | 1,774,200 | 641,800 | 36% | 99% |

### Throughput, 6 nodes

| Workload | Discus | Valkey Cluster | Valkey Sentinel | Share of Cluster | Share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 198,500 | 413,300 | 86,800 | 48% | 229% |
| SET | 120,700 | 381,200 | 77,900 | 32% | 155% |
| Mixed (1 write, 9 reads) | 171,100 | 403,200 | 83,400 | 42% | 205% |
| Mixed, 16 in flight | 496,900 | 3,126,600 | 603,300 | 16% | 82% |

With reads on all nodes, Valkey Sentinel did 243,600 GET/s with 3 nodes and 353,800 with 6 nodes.

## Latency

p50 and p99 in milliseconds, median of the 3 runs. With more than one memtier process, each value
is the highest percentile of the processes.

### Latency, 1 node

| Workload | Discus p50 | Discus p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: |
| GET | 1.15 | 1.74 | 1.07 | 2.10 |
| SET | 1.28 | 1.99 | 1.09 | 2.13 |
| Mixed (1 write, 9 reads) | 1.17 | 1.81 | 1.08 | 2.11 |
| Mixed, 16 in flight | 3.55 | 4.64 | 2.25 | 3.13 |

### Latency, 3 nodes

| Workload | Discus p50 | Discus p99 | Cluster p50 | Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.27 | 2.24 | 1.22 | 2.37 | 3.25 | 4.83 |
| GET, reads on all 3 nodes | | | | | 1.14 | 2.25 |
| SET | 4.03 | 6.78 | 1.34 | 2.02 | 3.52 | 4.61 |
| Mixed (1 write, 9 reads) | 1.39 | 6.91 | 1.27 | 2.43 | 3.31 | 4.58 |
| Mixed, 16 in flight | 8.26 | 12.41 | 2.51 | 4.42 | 7.07 | 8.89 |

### Latency, 6 nodes

| Workload | Discus p50 | Discus p99 | Cluster p50 | Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.48 | 7.39 | 1.36 | 2.42 | 6.46 | 8.83 |
| GET, reads on all 6 nodes | | | | | 1.54 | 2.62 |
| SET | 5.18 | 9.02 | 1.45 | 2.65 | 7.23 | 9.66 |
| Mixed (1 write, 9 reads) | 3.63 | 8.16 | 1.36 | 2.45 | 6.75 | 9.21 |
| Mixed, 16 in flight | 17.02 | 49.66 | 3.02 | 4.80 | 15.04 | 18.43 |

The Sentinel primary takes all 288 or 576 connections. It is one process, so more connections
give more latency, not more throughput.

## CPU use

CPU use in cores, as the mean over the server pods, median of the 3 runs.

| Workload | Discus, 1 | Valkey, 1 | Discus, 3 | Cluster, 3 | Discus, 6 | Cluster, 6 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.37 | 0.55 | 0.49 | 0.41 | 0.56 | 0.52 |
| SET | 0.48 | 0.50 | 0.66 | 0.70 | 0.65 | 0.60 |
| Mixed (1 write, 9 reads) | 0.48 | 0.48 | 0.60 | 0.54 | 0.56 | 0.54 |
| Mixed, 16 in flight | 0.70 | 0.51 | 0.68 | 0.69 | 0.73 | 0.75 |

No server pod and no client pod was throttled. The client used at most 3.3 of its 8 CPU.

## Why SET stays behind

A write runs on the primary of its key. A client connects to any node, so that node forwards the
SET when it is not the primary. With 3 nodes that is 2 of every 3 SETs, and with 6 nodes it is 5
of every 6. A read goes to any node with a copy, so with 3 nodes no GET needs a forward.

Valkey Cluster has no forwards in this benchmark, because memtier in Cluster mode sends each
command to the right primary. In a local measurement, one forward cost about 4.5 µs of user CPU,
and a local GET cost 0.67 µs.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has this limit: a forward timeout is still evidence against the peer. If a heavier load
pushes forwards past 500 ms, nodes can mark busy but healthy peers as Suspect.

## Files in this directory

`results-discus.jsonl` has one JSON line per Discus run, with the CFS counters of each pod in
`cfs`. The lines with a `verify` field hold the key count of each node before and after the runs.
The Valkey results are in the [2026-11-10 directory](../2026-11-10-k8s-v0.3.0/).

To run the Discus part again, with the `home-ops` context and Helm available, run this command
from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-11-k8s-v0.4.1-rc.1/results-discus.jsonl \
  --systems discus-1-0.4.1-rc.1,discus-3-0.4.1-rc.1,discus-6-0.4.1-rc.1
```
