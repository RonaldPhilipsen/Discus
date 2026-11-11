# Benchmark: Discus and Valkey Cluster with the same load

Date: 2026-11-10. Cluster: `home-ops`. Discus 0.2.3-rc.2, Valkey 9.

This is the first run that follows the [benchmark methodology](../methodology.md). Every setup
had 6 nodes with 1 CPU each, and 96 client connections per node, 576 in all. The Discus client
connected to each pod directly, with one memtier process and 96 connections per pod.

## Main results

- Discus failed under the pipelined load. In the first run with 16 commands in flight per
  connection, the membership failure detector marked healthy peers as dead. The nodes left the
  cluster epoch and joined again, and GETs started to miss. The cluster did not recover during
  the rest of the benchmark.
- Before the failure, Discus did 51% of the GET rate of Valkey Cluster, 29% of the SET rate, and
  39% of the mixed rate. With 100 connections, the earlier reports showed 38%, 18%, and 30%.
- Valkey Cluster had a lower latency at the same load. The GET p50 was 1.38 ms for Valkey and
  2.35 ms for Discus.

## Results

Only the first Discus run of each workload without pipelining is valid. Those rows are single
runs. The Valkey rows show the median of 3 runs.

| Workload | Discus rc.2 | Valkey Cluster, no replicas | Valkey Cluster, replicas |
| --- | ---: | ---: | ---: |
| GET | 209,300 | 407,400 | 410,200 |
| SET | 115,200 | 400,900 | 386,100 |
| Mixed (1 write, 9 reads) | 160,800 | 422,200 | 405,400 |
| Mixed, 16 in flight | failed | 3,204,400 | 3,049,600 |

Latency in milliseconds:

| Workload | Discus p50 | Discus p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: |
| GET | 2.35 | 6.98 | 1.38 | 2.45 |
| SET | 5.28 | 9.98 | 1.38 | 2.50 |
| Mixed (1 write, 9 reads) | 3.82 | 8.90 | 1.38 | 2.46 |

The Valkey latency columns show Cluster without replicas. The setup with replicas was within
0.1 ms.

The setups do not keep the same number of copies. Discus keeps three copies of each key, on
three of the six nodes. Valkey Cluster without replicas keeps one copy, and with replicas two.

## The failure

The first pipelined run had 576 connections with 16 commands each, so 9,216 requests were in
flight. Discus did 55,500 ops/s with 1,720 GET misses per second, and the p99 was 5.1 s. All
six pods reached their CPU limit: the kernel throttled each pod in 107 to 294 of the 100 ms
periods of the run.

The pod logs show this order of events:

1. Forwarded requests took longer than their timeouts. Each timeout counts as evidence that the
   peer failed, so healthy peers became Suspect and then Dead.
2. The nodes left the epoch, joined again, and discarded their counter state.
3. The `/healthz` and `/readyz` probes timed out, and `/readyz` returned 503.

The later runs ranged from 15,200 to 268,600 ops/s, with misses in every run except SET. No pod
restarted. Discus 0.2.2 showed probe timeouts and one pod restart at 100 connections in an earlier
run, so rc.2 did not cause the failure.

## Limits of this benchmark

- The script did not record the CPU use of the pods. A filter in the script took the counter of
  the pause container instead of the pod, so every value is 0. The script now has a fix. The
  throttle counters were correct.
- The Discus client had 6 memtier threads, one for each pod. The Valkey client had 8.
- The Discus rows are single runs, because the later runs happened after the failure.
- All pods and the client ran on one host, `k8s-neo-alpha`, with 32 cores.

## Files in this directory

| File | Content |
| --- | --- |
| `report.md` | This report. |
| `results.jsonl` | One JSON line per run, with the CFS counters of each pod in `cfs`, plus the data placement checks. |

To run the benchmark again, with the `home-ops` context and Helm available, run this command
from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-10-k8s-6-equal/results.jsonl \
  --systems discus-6-rc2,valkey-cluster-noreplica-6,valkey-cluster-replica-6
```
