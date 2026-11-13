# Benchmark: cost of Pyroscope profiling in Discus

Date: 2026-11-12. Cluster: `home-ops`, node `k8s-neo-alpha`. Valkey 9.

This report measures how much CPU profiling with Pyroscope costs Discus on Kubernetes. A local
test on 2026-11-13 found the cause and tested fixes. The runs follow the
[benchmark methodology](../methodology.md). Each pod has 1 core.

This report is part of the [SET throughput study](report.md). The study page lists the other
reports.

| Run | Time | Discus image | What changes |
| --- | --- | --- | --- |
| Profiling off | 17:32–17:56 | `hot-path-3bc4fea-pyroscope` | 3 nodes, profiling off |
| Profiling on | 17:40–18:04 | `hot-path-3bc4fea-pyroscope` | 3 nodes, profiles pushed to Pyroscope |

The `hot-path-3bc4fea-pyroscope` image is the `perf/hot-path` branch at commit 3bc4fea. It
contains the `pyroscope` feature. Both runs use the same image, and only `--pyroscope-url`
changes. The two runs alternated, one setup at a time. The "Profiling off" run is also the
"perf/hot-path" run of the other reports.

## Main results

- Profiling with Pyroscope on the musl image costs about 20% of the throughput in every
  workload.
- p99 is 1.5 to 3 times higher with profiling on.
- The profiler adds 15% to 35% CPU time per operation. The cause is a slow unwinder call on
  musl, not the sampling itself.
- A glibc image or a small patch to `pyroscope` removes the cost.
- Benchmarks must run with profiling off on musl. The
  [static glibc report](https://github.com/RonaldPhilipsen/Discus/blob/e9affe8/docs/benchmarks/2026-11-13-k8s-static-glibc/report.md)
  later removed the cost, and release 1.2.4 builds on static glibc.

## Results

| Mode | Workload | Profiling off | Profiling on | Change | p99, off | p99, on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cluster mode | GET | 218,500 | 174,100 | -20% | 2.01 | 5.92 |
| Cluster mode | SET | 144,500 | 114,800 | -21% | 3.44 | 6.78 |
| Cluster mode | Mixed (1 write, 9 reads) | 194,700 | 160,500 | -18% | 2.25 | 6.14 |
| Cluster mode | Mixed, 16 in flight | 1,068,800 | 888,700 | -17% | 6.30 | 9.41 |
| Forward mode | GET | 223,200 | 179,100 | -20% | 2.25 | 5.34 |
| Forward mode | SET | 89,400 | 68,700 | -23% | 6.53 | 11.13 |
| Forward mode | Mixed (1 write, 9 reads) | 173,700 | 141,000 | -19% | 6.85 | 10.43 |
| Forward mode | Mixed, 16 in flight | 695,000 | 568,800 | -18% | 11.52 | 16.51 |

The throughput is in operations per second, as the median of 3 runs of 30 seconds, rounded to
hundreds. p99 is in milliseconds.

For the same workloads at 1 core, Valkey Cluster did 228,100 GET/s and 210,500 SET/s on 3 nodes.
Valkey Sentinel did 85,700 GET/s and 80,500 SET/s. These numbers come from the
[1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md).

## Why profiling costs this much

The profiler adds CPU load. The cgroup counters show 15% to 35% more CPU time per operation with
profiling on. The pods were rarely throttled, because the counters showed less than 1 core per pod.
These counters leave out the softirq time, so the pods were closer to their limit than the counters
show. The [CPU limit report](cpu-and-threads.md#why-one-worker-thread-is-slow) describes this.

The local test ran one node on one pinned core. `redis-benchmark` on other cores sent the load. A
`perf` profile of the musl binary with profiling on showed this call chain in about 13% of the
CPU of the node:

```text
perf_signal_handler
  Collector::add (hash of the sample)
    _Unwind_FindEnclosingFunction
      libunwind::UnwindCursor::setInfoBasedOnIPRegister
        libunwind::CFI_Parser::findFDE
          libunwind::CFI_Parser::parseCIE
```

The signal handler of `pyroscope` 2.1.5 hashes each sample by the start address of each function.
It gets that address from `_Unwind_FindEnclosingFunction`, once for every frame of every sample.

On musl, Rust links LLVM libunwind. If the binary search in `.eh_frame_hdr` finds no entry for an
address, LLVM libunwind scans all of `.eh_frame`, which is 2.2 MB. The musl syscall stub `sccp`
has no unwind entry. Discus spends much of its time in system calls, so most samples cause one
full scan. On glibc, `libgcc` does not do this scan, so the call is fast.

The stack walk itself uses `framehop`. It is not part of the cost.

## Fixes that were tested

The table gives the CPU time per operation, in microseconds, for one node on one core. Each cell
has the results of two alternating runs. Profiling takes 100 samples per second (100 Hz), except
in the 19 Hz row.

| Build | Profiling off, GET | Profiling on, GET | Profiling off, SET | Profiling on, SET |
| --- | ---: | ---: | ---: | ---: |
| musl, release image | 1.57, 1.67 | 2.03, 1.99 | 2.13, 1.84 | 2.21, 2.02 |
| musl, `-C force-frame-pointers=yes` | 1.58, 1.61 | 2.09, 1.87 | 1.71, 1.83 | 2.25, 2.12 |
| musl, 19 Hz | 1.63, 1.63 | 1.72, 1.67 | 1.82, 1.88 | 1.98, 1.97 |
| musl, patched `pyroscope` | 1.53, 1.55 | 1.64, 1.54 | 1.78, 1.73 | 1.80, 1.73 |
| glibc, `distroless/cc-debian12` image | 1.44, 1.47 | 1.43, 1.44 | 1.67, 1.74 | 1.71, 1.81 |

The results of the tests:

- Frame pointers do not help. `pyroscope` 2.1.5 has no frame-pointer unwinder, and the slow call
  does not depend on the stack walk.
- A glibc base image removes the cost.
- A patch to `pyroscope` removes the cost on musl. The patch makes `symbol_address` return the
  instruction address on musl, as the crate already does on macOS.
- A lower sample rate reduces the cost in proportion. At 19 Hz, the cost is about 3% to 6%.

The patched `symbol_address` is used only to group samples before the upload. The symbol names
come from the instruction address, so the profiles do not change. Nobody inspected the content
of the patched profiles yet.

## Limits of this benchmark

- The Kubernetes runs cover 3 nodes with 1 core only.
- The local test of the fixes ran one node without replication, not the Kubernetes setup.

## Files in this directory

- `results-1cpu-profiling-off.jsonl`: 3 nodes, 1 core, profiling off.
- `results-1cpu-profiling-on.jsonl`: 3 nodes, 1 core, profiling on.

Each line has the memtier results and the cgroup CPU counters of each pod.
[`scripts/bench-k8s-cpu.py`](../../../scripts/bench-k8s-cpu.py) prints the medians, the CPU per
operation and the throttled periods from these files. This command gives the "Profiling on" setup.
For the "Profiling off" setup, leave out `--pyroscope-url`. Replace `<pyroscope-url>` with the push
URL of your Pyroscope server.

```sh
SYSTEMS=discus-cluster-3-hot-path-pyro,discus-3-hot-path-pyro
BENCH_CPUS=1 DISCUS_CHART=$PWD/charts/discus python3 scripts/bench-k8s.py \
  --systems "$SYSTEMS" --pyroscope-url <pyroscope-url> --out results-1cpu-profiling-on.jsonl
```
