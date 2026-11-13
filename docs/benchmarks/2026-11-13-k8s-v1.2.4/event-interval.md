# Benchmark: the tokio event interval in Discus

Date: 2026-11-13. Cluster: `home-ops`, node `k8s-neo-alpha`. Valkey 9.

This report tests whether a different tokio event interval makes Discus faster with 1 worker
thread. The event interval is the number of tasks that a tokio worker thread runs before it asks
the kernel for new network events. The default is 61. A lower value lets the worker see new
requests sooner, but it asks the kernel more often. The runs follow the
[benchmark methodology](../methodology.md).

This report is part of the [SET throughput study](report.md). The study page lists the other
reports.

| Run | Time | Discus image | What changes |
| --- | --- | --- | --- |
| Event interval | 09:45–10:36 on 2026-11-13 | `event-interval-0de9075-pyroscope` | 3 nodes, 1 core, `DISCUS_EVENT_INTERVAL` from 1 to 255 |

The image `event-interval-0de9075-pyroscope` is the `perf/hot-path` branch plus one commit. That
commit reads the event interval from the environment variable `DISCUS_EVENT_INTERVAL`. Profiling
is off. Each pod runs Discus in cluster mode with 1 core and 1 worker thread. The run tests the
values 1, 7, 31, 61, 127 and 255.

## Main results

- No value made Discus faster. From 7 to 255, every workload stayed within 4% of the default, in
  both directions.
- The value 1 raised the p99 3.3 to 4.4 times. Its p50 fell to 0.05 ms, but the average latency
  did not fall.
- The default value of 61 stays.

## Results

From 7 to 255, the differences are about as large as the noise between runs. For example, SET
did between 145,500 and 150,100 ops/s, and the default did 149,600. GET did between 209,400 and
223,700, and the default did 218,600. From 31 to 255, the p50 and p99 stayed within 0.3 ms of the
default. With the value 7, they were 0.1 to 0.45 ms higher than the default.

The value 1 changed the shape of the latency. The p50 fell to 0.05 ms, but the p99 rose to
between 8.9 and 22.3 ms. The throughput shows that the average latency did not fall. With 288
connections and 152,300 SET/s, the average is about 1.9 ms. Most requests were thus fast, and a
large group waited much longer. GET was 8% slower, and the pipelined mix was 10% faster.

The event interval does not lower the CPU cost of a request. The
[CPU limit report](cpu-and-threads.md#why-one-worker-thread-is-slow) shows that the CPU limits a
node with 1 worker thread. A different order of the work thus moves latency between requests but
does not add throughput.

For the same setup at 1 core and 3 nodes, Valkey Cluster did 210,500 SET/s and 228,100 GET/s.
Valkey Sentinel did 80,500 SET/s and 85,700 GET/s. These numbers come from the
[1.1.1 report](../2026-11-12-k8s-v1.1.1/report.md).

## Limits of this benchmark

- The run covers 3 nodes with 1 core only.
- The run took place on 2026-11-13, one day after the CPU limit, replication cost and profiling cost
  runs. Other workloads on the host can change the results between runs.
- With the value 1, the p50 of memtier does not show the typical latency. The report uses the
  throughput to find the average latency.

## Files in this directory

- `results-1cpu-event-interval-1.jsonl` to `results-1cpu-event-interval-255.jsonl`: 3 nodes,
  cluster mode, 1 core, one file for each event interval.

Each line has the memtier results and the cgroup CPU counters of each pod.
[`scripts/bench-k8s-cpu.py`](../../../scripts/bench-k8s-cpu.py) prints the medians, the CPU per
operation and the throttled periods from these files. The run used this command, once for each
value. The system `discus-cluster-3-event-interval` exists only on the branch `exp/event-interval`.

```sh
BENCH_CPUS=1 DISCUS_CHART=$PWD/charts/discus \
  DISCUS_HELM_SET_STRING='extraEnv[0].name=DISCUS_EVENT_INTERVAL,extraEnv[0].value=7' \
  python3 scripts/bench-k8s.py --systems discus-cluster-3-event-interval \
  --out results-1cpu-event-interval-7.jsonl
```

## Result tables

The throughput is in operations per second, as the median of 3 runs of 30 seconds, rounded to
hundreds. p50 and p99 are in milliseconds. A throttled-periods cell gives the throttled periods of
all pods against all periods.

| Workload | Event interval | Throughput | p50 | p99 | Throttled periods |
| --- | ---: | ---: | ---: | ---: | ---: |
| SET | 1 | 152,300 | 0.05 | 11.52 | 0 of 910 |
| SET | 7 | 148,400 | 2.02 | 3.68 | 0 of 896 |
| SET | 31 | 149,800 | 1.89 | 3.46 | 0 of 1037 |
| SET | 61 (default) | 149,600 | 1.93 | 3.46 | 0 of 843 |
| SET | 127 | 150,100 | 1.90 | 3.49 | 0 of 1015 |
| SET | 255 | 145,500 | 1.89 | 3.50 | 0 of 1060 |
| GET | 1 | 201,900 | 0.05 | 8.89 | 19 of 935 |
| GET | 7 | 209,400 | 1.46 | 2.21 | 6 of 905 |
| GET | 31 | 215,900 | 1.32 | 2.06 | 10 of 948 |
| GET | 61 (default) | 218,600 | 1.28 | 2.02 | 0 of 925 |
| GET | 127 | 216,400 | 1.28 | 1.99 | 0 of 968 |
| GET | 255 | 223,700 | 1.27 | 2.00 | 0 of 912 |
| Mixed (1 write, 9 reads) | 1 | 189,500 | 0.05 | 9.21 | 29 of 968 |
| Mixed (1 write, 9 reads) | 7 | 197,700 | 1.59 | 2.46 | 18 of 882 |
| Mixed (1 write, 9 reads) | 31 | 204,700 | 1.43 | 2.22 | 0 of 947 |
| Mixed (1 write, 9 reads) | 61 (default) | 197,100 | 1.43 | 2.25 | 0 of 1044 |
| Mixed (1 write, 9 reads) | 127 | 196,300 | 1.43 | 2.27 | 0 of 1008 |
| Mixed (1 write, 9 reads) | 255 | 198,600 | 1.42 | 2.22 | 0 of 982 |
| Mixed, 16 in flight | 1 | 1,169,200 | 0.10 | 22.27 | 0 of 824 |
| Mixed, 16 in flight | 7 | 1,085,000 | 4.67 | 6.69 | 0 of 958 |
| Mixed, 16 in flight | 31 | 1,089,500 | 4.19 | 6.05 | 0 of 815 |
| Mixed, 16 in flight | 61 (default) | 1,064,000 | 4.22 | 6.33 | 0 of 863 |
| Mixed, 16 in flight | 127 | 1,077,900 | 4.22 | 6.53 | 0 of 955 |
| Mixed, 16 in flight | 255 | 1,073,300 | 4.16 | 6.40 | 0 of 958 |
