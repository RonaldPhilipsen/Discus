# Benchmark: Discus and Valkey Cluster with the same load

Date: 2026-11-10. Cluster: `home-ops`. Discus 0.2.3-rc.2, Valkey 9.

This is the first run that follows the [benchmark methodology](../methodology.md). Every setup had
6 nodes and 96 client connections per node, 576 in all. This run had no 1-node, 3-node, or
Sentinel setups.

## Main results

- Discus failed under the pipelined load. Healthy peers were marked dead, GETs started to miss,
  and the cluster did not recover.
- Before the failure, Discus did 51% of the Valkey Cluster GET rate, 29% of the SET rate, and 39%
  of the mixed rate.
- The Discus GET p50 was 2.35 ms. The Valkey Cluster GET p50 was 1.38 ms.

## Results

Operations per second. Only the first Discus run of each workload without pipelining is valid, so
the Discus values are single runs. The Valkey values are the median of 3 runs of 30 seconds.

| Workload | Discus | Valkey Cluster, no replicas | Valkey Cluster, replicas |
| --- | ---: | ---: | ---: |
| GET | 209,300 | 407,400 | 410,200 |
| SET | 115,200 | 400,900 | 386,100 |
| Mixed (1 write, 9 reads) | 160,800 | 422,200 | 405,400 |
| Mixed, 16 in flight | failed | 3,204,400 | 3,049,600 |

Latency in milliseconds. The Valkey values are of Cluster without replicas. Cluster with replicas
was within 0.1 ms.

| Workload | Discus p50 | Discus p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: |
| GET | 2.35 | 6.98 | 1.38 | 2.45 |
| SET | 5.28 | 9.98 | 1.38 | 2.50 |
| Mixed (1 write, 9 reads) | 3.82 | 8.90 | 1.38 | 2.46 |

Discus kept 3 copies of each key. Valkey Cluster kept 1 copy without replicas and 2 with replicas.

## The failure

The first pipelined run had 9,216 requests in flight. Discus did 55,500 ops/s with 1,720 GET misses
per second, and the p99 was 5.1 s. All six pods reached their CPU limit. The pod logs show this
order of events:

1. Forwarded requests took longer than their timeouts. Each timeout counts as evidence that the
   peer failed, so healthy peers became Suspect and then Dead.
2. The nodes left the epoch, joined again, and discarded their counter state.
3. The `/healthz` and `/readyz` probes timed out, and `/readyz` returned 503.

The later runs did 15,200 to 268,600 ops/s, with misses in every run except SET. No pod restarted.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- The script did not record the CPU use of the pods, because it read the counter of the pause
  container. The script now has a fix. The throttle counters were correct.
- The Discus client had 6 memtier threads, one for each pod. The Valkey client had 8.

## Files in this directory

`results.jsonl` has one JSON line per run, with the CFS counters of each pod in `cfs`, plus the
data placement checks. To run the benchmark again, with the `home-ops` context and Helm available,
run this command from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-10-k8s-6-equal/results.jsonl \
  --systems discus-6-rc2,valkey-cluster-noreplica-6,valkey-cluster-replica-6
```
