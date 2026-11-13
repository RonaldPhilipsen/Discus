# Benchmark: Discus 1.2.3 on musl and on static glibc

Date: 2026-11-13. Cluster: `home-ops`, node `k8s-neo-alpha` (AMD Ryzen 9 7945HX). Valkey 9.

This report compares two builds of the same Discus code on Kubernetes. The release image links
musl. The test image links glibc statically. Both images are scratch images. The report also
measures the cost of Pyroscope profiling in the static glibc build. The profiling-cost report
found that profiling costs about 20% of the throughput on musl. That report is
`docs/benchmarks/2026-11-12-k8s-cpu-and-threads/profiling-cost.md`, which is not merged yet.

The runs follow the [benchmark methodology](../methodology.md), with 3 nodes only. Each pod has
1 core. The runs took place from 11:22 to 12:31.

## Main results

- glibc and musl were within 2% of each other in 7 of 8 workloads. This difference is within
  the spread of the runs.
- With 16 requests in flight in cluster mode, glibc did 1,093,000 operations per second and musl
  did 1,047,400. This difference is 4%.
- With profiling on, glibc lost at most 2% of the throughput, which is within the spread of the
  runs. On musl, the profiling-cost report measured 17% to 23%.
- With profiling on, the p99 of glibc stayed within 0.2 ms of the p99 without profiling.
- The CPU time per operation did not show a clear difference between musl and glibc. The runs
  vary more than the 9% to 11% gap of the
  [local test](../2026-11-13-local-glibc-static/report.md).
- All runs kept 1,000,000 keys on each node, and no GET missed.

## Setups

| Setup | Discus image | Build |
| --- | --- | --- |
| musl | `1.2.3` | The release image, `x86_64-unknown-linux-musl` |
| glibc | `static-glibc-1b486a1` | `x86_64-unknown-linux-gnu` with `-C target-feature=+crt-static` |
| glibc, profiling on | `static-glibc-1b486a1-pyroscope` | As glibc, with the `pyroscope` feature, profiles pushed to Pyroscope |

All three images come from commit 1b486a1, which is the tag v1.2.3. The `glibc` image has the
same Cargo features as the release image. The file `Dockerfile.k8s-glibc` builds both glibc
images. It is the release `Dockerfile` with the glibc target and the static flag.

The test ran 3 rounds. Each round runs musl, then glibc, then glibc with profiling on. Each setup
runs in cluster mode and in forward mode. Each run of a workload takes 30 seconds.

## Throughput

Operations per second, median of 3 runs, rounded to hundreds. The Valkey columns come from the
[1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md), 3 nodes with 1 core. Those runs took place
on the same node on 2026-11-12, in a different session.

### Throughput, cluster mode

| Workload | musl | glibc | glibc, profiling on | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 224,400 | 227,600 | 230,700 | 228,100 | 85,700 |
| SET | 149,400 | 149,700 | 147,100 | 210,500 | 80,500 |
| Mixed (1 write, 9 reads) | 199,400 | 196,100 | 196,900 | 219,300 | 83,300 |
| Mixed, 16 in flight | 1,047,400 | 1,093,000 | 1,088,300 | 1,723,200 | 633,700 |

### Throughput, forward mode

| Workload | musl | glibc | glibc, profiling on | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 228,200 | 231,600 | 230,200 | 228,100 | 85,700 |
| SET | 92,200 | 93,700 | 91,800 | 210,500 | 80,500 |
| Mixed (1 write, 9 reads) | 176,900 | 176,700 | 174,000 | 219,300 | 83,300 |
| Mixed, 16 in flight | 709,800 | 711,700 | 713,900 | 1,723,200 | 633,700 |

Most runs were within 5% of their median. One glibc run in forward mode with 16 in flight did
630,200, 11% below the median. The other two runs of that setup did 724,800 and 711,700. The log
shows no error for that run.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, cluster mode

| Workload | musl p50 | musl p99 | glibc p50 | glibc p99 | profiling on p50 | profiling on p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.25 | 1.89 | 1.23 | 1.86 | 1.25 | 1.88 | 1.21 | 2.38 | 3.28 | 4.64 |
| SET | 1.86 | 3.36 | 1.87 | 3.36 | 1.90 | 3.39 | 1.33 | 1.96 | 3.52 | 4.64 |
| Mixed (1 write, 9 reads) | 1.41 | 2.30 | 1.40 | 2.22 | 1.43 | 2.30 | 1.26 | 2.45 | 3.39 | 4.74 |
| Mixed, 16 in flight | 4.19 | 6.21 | 4.06 | 6.08 | 4.13 | 6.17 | 2.53 | 4.45 | 7.17 | 8.96 |

### Latency, forward mode

| Workload | musl p50 | musl p99 | glibc p50 | glibc p99 | profiling on p50 | profiling on p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.24 | 2.21 | 1.22 | 2.21 | 1.22 | 2.24 | 1.21 | 2.38 | 3.28 | 4.64 |
| SET | 3.66 | 6.43 | 3.63 | 6.21 | 3.69 | 6.40 | 1.33 | 1.96 | 3.52 | 4.64 |
| Mixed (1 write, 9 reads) | 1.35 | 6.72 | 1.35 | 6.69 | 1.37 | 6.85 | 1.26 | 2.45 | 3.39 | 4.74 |
| Mixed, 16 in flight | 7.36 | 11.39 | 7.36 | 11.52 | 7.23 | 11.13 | 2.53 | 4.45 | 7.17 | 8.96 |

On musl, the profiling-cost report measured p99 values 1.5 to 3 times higher with profiling on.

## CPU time

The CPU time per operation is the CPU time of the three Discus pods divided by the number of
operations. The cgroup counters do not include softirq time. Microseconds, median of 3 runs,
with the lowest and highest run in brackets.

| Mode | Workload | musl | glibc | glibc, profiling on |
| --- | --- | ---: | ---: | ---: |
| Cluster | GET | 4.77 (4.41–5.35) | 4.78 (4.57–4.83) | 4.92 (4.41–5.64) |
| Cluster | SET | 11.55 (9.79–11.60) | 11.66 (10.11–12.66) | 11.85 (11.03–13.19) |
| Cluster | Mixed (1 write, 9 reads) | 7.90 (5.58–9.28) | 7.74 (5.81–8.13) | 7.58 (6.20–8.47) |
| Cluster | Mixed, 16 in flight | 1.67 (1.66–1.71) | 1.86 (1.46–1.93) | 1.81 (1.43–1.92) |
| Forward | GET | 4.73 (4.02–5.25) | 4.91 (4.57–5.77) | 5.41 (4.68–6.06) |
| Forward | SET | 19.95 (19.56–21.41) | 18.20 (16.79–18.29) | 18.05 (14.35–20.52) |
| Forward | Mixed (1 write, 9 reads) | 8.68 (8.06–9.24) | 7.83 (7.09–8.66) | 7.74 (6.77–8.19) |
| Forward | Mixed, 16 in flight | 2.91 (2.87–2.93) | 2.98 (2.59–3.23) | 2.74 (2.44–3.02) |

The ranges of musl and glibc overlap in 7 of 8 rows. In forward-mode SET, glibc used 9% less
CPU time per operation, and the ranges do not overlap. In the other rows, the counters do not
show a direction.

The Discus pods used 0.3 to 0.85 cores each, below their limit of 1 core. No pod was throttled.
Without pipelining, the latency sets the rate. For this reason, a lower CPU cost per operation
gives little gain in throughput in these workloads.

## Conclusion

On this cluster, static glibc gives about the same throughput and latency as musl. A gain of 4%
showed only in cluster mode with 16 requests in flight. The main gain of static glibc is that
Pyroscope profiling costs almost nothing. On musl, it costs about 20% of the throughput.

## Limits of this benchmark

- The runs cover 3 nodes with 1 core only.
- No workload made a pod reach its core. The local test showed the CPU gain of glibc. A
  CPU-bound workload can show more of that gain in the throughput.
- The node CPU has AVX-512. The test does not show the result on a CPU without AVX-512.
- The profiles in Pyroscope from the glibc build were not inspected.

## Files in this directory

- `results-musl.jsonl`, `results-glibc.jsonl`, `results-glibc-profiling-on.jsonl`: one line for
  each run, with the memtier results and the cgroup CPU counters of each pod.
- `Dockerfile.k8s-glibc`: the build of the glibc images.

These commands ran one round. The script profiles by default, so the first two commands turn
profiling off.

```sh
export BENCH_CPUS=1 DISCUS_CHART=$PWD/charts/discus
python3 scripts/bench-k8s.py --reps 1 --systems discus-cluster-3-1.2.3,discus-3-1.2.3 \
  --pyroscope-url "" --out results-musl.jsonl
python3 scripts/bench-k8s.py --reps 1 --systems discus-cluster-3-glibc,discus-3-glibc \
  --pyroscope-url "" --out results-glibc.jsonl
python3 scripts/bench-k8s.py --reps 1 --systems discus-cluster-3-glibc-pyro,discus-3-glibc-pyro \
  --out results-glibc-profiling-on.jsonl
```
