# Benchmark: Discus on musl and on static glibc

Date: 2026-11-13. Local machine: AMD Ryzen 9 9950X3D, Linux 7.2. Valkey 9.1.3.

This report compares two builds of the same Discus code in a scratch image. The release image
links musl. The other build links glibc statically, so it also runs in a scratch image. The test
also measures the cost of Pyroscope profiling in both builds. The profiling-cost report found
that profiling costs about 20% of the throughput on musl. That report is
`docs/benchmarks/2026-11-12-k8s-cpu-and-threads/profiling-cost.md`, which is not merged yet.

This is a local test of one node. It does not follow the Kubernetes
[benchmark methodology](../methodology.md).

## Main results

- Static glibc uses 9% to 11% less CPU time per operation than musl, with profiling off.
- With profiling on, static glibc shows no measurable cost. musl uses 18% to 20% more CPU time
  per operation.
- The largest single difference with profiling off is `memcpy`. It takes 3.0% of the node CPU
  on musl and 0.25% on glibc.
- The static glibc image is 40.1 MB. The musl image is 35.9 MB.
- With profiling off, Discus on static glibc uses 9% less CPU time per GET than Valkey 9. Discus
  on musl uses about the same CPU time as Valkey.

## Setups

| Name | Build | Image |
| --- | --- | --- |
| musl | `x86_64-unknown-linux-musl`, as in the release `Dockerfile` | scratch, 35.9 MB |
| static glibc | `x86_64-unknown-linux-gnu` with `-C target-feature=+crt-static` | scratch, 40.1 MB |
| Valkey 9 | `docker.io/valkey/valkey:9`, no persistence | Debian, from Docker Hub |

Both Discus builds contain the `pyroscope` feature and come from commit 1b486a1. The build
image is `rust:1-bookworm`. The file `Dockerfile.static-glibc` builds the static glibc image.
Both image sizes include the CA bundle that the profiler needs.

## How the test ran

The script [`scripts/bench-profiling.py`](../../../scripts/bench-profiling.py) starts one node and
pins all its threads to core 4. The two Discus builds run directly on the host. Valkey runs in a
container with the host network. `redis-benchmark` 8.10.2 runs on cores 16 to 23 with 4 threads and
64 connections. Each run does these steps:

1. Write 500,000 SET requests to warm up the node.
2. Send 1,500,000 GET requests over 100,000 keys.
3. Send 1,500,000 SET requests over 100,000 keys.

With profiling on, the node sends profiles every 10 seconds to a local HTTP server that discards
them. The sample rate is 100 Hz.

The script reads the CPU time of the node from `/proc/<pid>/stat` before and after each step.
It divides the CPU time by the number of requests. The four Discus setups ran in 4 alternating
rounds. Valkey ran 4 times after them.

No node reached its core. Each node used 0.55 to 0.65 cores, so the client sets the throughput.
For this reason, the CPU time per operation is the main result.

## Results

CPU time per operation in microseconds. Each cell is the median of 4 runs, with the lowest and
highest run in brackets.

| Setup | Profiling | GET | SET |
| --- | --- | ---: | ---: |
| musl | off | 1.62 (1.56–1.73) | 1.81 (1.73–1.82) |
| musl | on | 1.95 (1.90–2.02) | 2.13 (2.06–2.40) |
| static glibc | off | 1.45 (1.41–1.51) | 1.65 (1.59–1.67) |
| static glibc | on | 1.43 (1.39–1.49) | 1.70 (1.70–1.87) |
| Valkey 9 | none | 1.59 (1.57–1.60) | 1.67 (1.59–1.71) |

The ranges of musl and static glibc do not overlap in any row.

Operations per second, median of 4 runs. p50 is in milliseconds. `redis-benchmark` reports the
rate in coarse steps, so small differences in this table are not significant.

| Setup | Profiling | GET | GET p50 | SET | SET p50 |
| --- | --- | ---: | ---: | ---: | ---: |
| musl | off | 363,800 | 0.167 | 333,200 | 0.179 |
| musl | on | 324,300 | 0.167 | 307,700 | 0.175 |
| static glibc | off | 387,300 | 0.159 | 363,800 | 0.167 |
| static glibc | on | 387,300 | 0.159 | 352,800 | 0.171 |
| Valkey 9 | none | 374,800 | 0.163 | 363,900 | 0.163 |

For Valkey Cluster and Valkey Sentinel on Kubernetes, see the
[1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md). Those numbers come from a different setup
and do not compare with this table.

## Where the difference comes from

A `perf record` at 999 Hz during the GET step of each Discus build gave about 3,000 samples.
Profiling was off.

| Part | musl | static glibc |
| --- | ---: | ---: |
| Kernel | 57.5% | 59.6% |
| Discus binary, with libc | 21.3% | 18.3% |
| `nf_tables` and `nf_conntrack` | 18.1% | 19.2% |
| `memcpy` | 3.0% | 0.25% |
| `memcmp` | 0.48% | 0.11% |

musl has one `memcpy` for x86_64, based on `rep movsq`. glibc picks an AVX-512 version
(`__memmove_avx512_unaligned_erms`) at start. This difference explains most of the gap in the
Discus binary. A profile of this size cannot explain the full 9% to 11% gap.

With profiling on, the musl build spends its extra time in LLVM libunwind. The profiling-cost
report describes the cause. The static glibc build uses the unwinder of `libgcc`, which does not
show this cost.

## Name lookup in the scratch image

A static glibc binary can need shared libraries of glibc at runtime for name lookup. glibc 2.34
and later contain the `files` and `dns` lookup modules, so the lookup of host names works
without them. A test program, built with the same flags, ran in the static glibc scratch image
with Podman:

| Name | Result |
| --- | --- |
| `localhost` | 127.0.0.1, from `/etc/hosts` |
| `github.com` | 140.82.121.3, from DNS |
| `kubernetes.default.svc.cluster.local` | Not found, because the test machine is not in a cluster |

The Discus binary in this image started and showed its help text.

## Limits of this benchmark

- One node ran without replication and without other nodes. The test did not measure the
  Kubernetes setup.
- The client was the limit, not the node. A node at its core limit can give other numbers.
- The machine has an AVX-512 CPU. On a CPU without AVX-512, glibc uses an AVX2 `memcpy`, and
  the gap can be smaller.
- The static glibc image was not tested in a cluster. Name lookup with `search` domains from a
  Kubernetes `resolv.conf` was not tested.
- The content of the profiles from the static glibc build was not inspected.

## Files in this directory

- `results.jsonl`: one line for each step of each run. Each line has the setup, the profiling
  mode, the workload, the rate, the CPU time per operation, the cores used, and p50.
- `Dockerfile.static-glibc`: the build of the static glibc image. It also builds the name
  lookup test program from a `dnstest` directory in the build context.

To repeat one run, build the binary and run the script from the root of the repository. The
second argument is `on` or `off`.
The third argument is the name of the setup. The variable `OUT` names the results file.

```sh
OUT=results.jsonl python3 scripts/bench-profiling.py ./discus-glibc-static off glibc-static
```
