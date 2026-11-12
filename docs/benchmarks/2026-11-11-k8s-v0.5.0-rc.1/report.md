# Benchmark: Discus 0.5.0-rc.1 in forward mode and cluster mode

Date: 2026-11-11. Cluster: `home-ops`. Discus 0.5.0-rc.1, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). The Discus runs took place from
14:50 to 15:39. The Valkey numbers come from the
[2026-11-10 report](../2026-11-10-k8s-v0.3.0/report.md).

Discus 0.5.0-rc.1 adds slot-aware client routing. A redirect listener on port 6380 answers the
Redis Cluster topology commands, such as `CLUSTER SLOTS`. It returns `-MOVED` for a key that the
node does not own. This report measures Discus in two modes:

- **Forward mode**: memtier connects to port 6379 of every pod. A node forwards each command that
  it does not own. The redirect listener does not run.
- **Cluster mode**: one memtier process with `--cluster-mode` reads the slot map from port 6380.
  It sends each command to the primary of its key, with 96 connections to each node.

## Main results

- Discus completed every run in both modes. No GET missed, and every node reported 1,000,000 keys
  before and after the runs.
- No cluster-mode run got a `-MOVED` or `-ASK` reply after the prefill.
- Cluster mode doubled the 6-node rates of forward mode.
- In cluster mode with 6 nodes, Discus did 96% of the Valkey Cluster GET rate. The spec target is
  80%. Discus met it.
- In cluster mode, Discus did 64% of the Valkey Cluster SET rate with 3 and 6 nodes. The spec
  target is 70%. Discus did not meet it.
- In cluster mode, Discus did 148% to 458% of the Valkey Sentinel rate with 3 and 6 nodes.

## Throughput

Operations per second, median of 3 runs of 30 seconds. A share is the cluster-mode rate divided by
the Valkey rate.

### Throughput, 1 node

| Workload | Forward | Cluster | Valkey | Share |
| --- | ---: | ---: | ---: | ---: |
| GET | 81,400 | 83,200 | 87,500 | 95% |
| SET | 73,900 | 75,100 | 85,200 | 88% |
| Mixed (1 write, 9 reads) | 79,800 | 81,900 | 86,700 | 94% |
| Mixed, 16 in flight | 426,700 | 431,300 | 669,500 | 64% |

### Throughput, 3 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel | Share of Cluster | Share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 220,900 | 219,300 | 226,700 | 87,000 | 97% | 252% |
| SET | 84,200 | 133,200 | 207,700 | 80,600 | 64% | 165% |
| Mixed (1 write, 9 reads) | 170,200 | 185,500 | 217,900 | 85,100 | 85% | 218% |
| Mixed, 16 in flight | 625,500 | 951,300 | 1,774,200 | 641,800 | 54% | 148% |

### Throughput, 6 nodes

| Workload | Forward | Cluster | Valkey Cluster | Valkey Sentinel | Share of Cluster | Share of Sentinel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 198,100 | 397,700 | 413,300 | 86,800 | 96% | 458% |
| SET | 116,600 | 244,900 | 381,200 | 77,900 | 64% | 314% |
| Mixed (1 write, 9 reads) | 170,500 | 327,200 | 403,200 | 83,400 | 81% | 392% |
| Mixed, 16 in flight | 482,600 | 1,574,100 | 3,126,600 | 603,300 | 50% | 261% |

In forward mode, the range of the three runs was within 6% of the median. In cluster mode, it was
within 7%, except with 3 nodes. There, the three SET runs did 145,100, 133,200, and 132,400, so 64%
to 70% of Valkey Cluster.

### Spec targets

| Target | Result | Met |
| --- | ---: | --- |
| Cluster-mode SET with 3 nodes, at least 70% of Valkey Cluster | 64% | No |
| Cluster-mode SET with 6 nodes, at least 70% of Valkey Cluster | 64% | No |
| Cluster-mode GET with 6 nodes, at least 80% of Valkey Cluster | 96% | Yes |

The spec calls these values targets, not gates.

## Latency

p50 and p99 in milliseconds, median of the 3 runs. In forward mode, each value is the highest
percentile of the memtier processes.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.17 | 1.74 | 1.14 | 1.70 | 1.07 | 2.10 |
| SET | 1.29 | 1.97 | 1.27 | 2.01 | 1.09 | 2.13 |
| Mixed (1 write, 9 reads) | 1.18 | 1.85 | 1.16 | 1.84 | 1.08 | 2.11 |
| Mixed, 16 in flight | 3.57 | 4.67 | 3.54 | 4.61 | 2.25 | 3.13 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.27 | 2.24 | 1.32 | 2.08 | 1.22 | 2.37 | 3.25 | 4.83 |
| SET | 4.03 | 6.94 | 2.04 | 3.73 | 1.34 | 2.02 | 3.52 | 4.61 |
| Mixed (1 write, 9 reads) | 1.40 | 6.97 | 1.47 | 2.32 | 1.27 | 2.43 | 3.31 | 4.58 |
| Mixed, 16 in flight | 8.32 | 12.35 | 4.77 | 6.97 | 2.51 | 4.42 | 7.07 | 8.89 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.90 | 7.55 | 1.40 | 2.29 | 1.36 | 2.42 | 6.46 | 8.83 |
| SET | 5.28 | 9.47 | 2.37 | 4.09 | 1.45 | 2.65 | 7.23 | 9.66 |
| Mixed (1 write, 9 reads) | 3.74 | 8.32 | 1.71 | 2.85 | 1.36 | 2.45 | 6.75 | 9.21 |
| Mixed, 16 in flight | 17.41 | 50.17 | 5.60 | 8.38 | 3.02 | 4.80 | 15.04 | 18.43 |

In cluster mode, the SET p50 is 0.7 ms to 0.9 ms above Valkey Cluster, and the GET p50 is within
0.1 ms. Without pipelining, the latency sets the rate, so the SET latency gap gives the SET
throughput gap.

## CPU use

CPU use in cores, as the mean over the server pods, median of the 3 runs.

| Workload | Forward, 1 | Cluster, 1 | Valkey, 1 | Forward, 3 | Cluster, 3 | Valkey Cluster, 3 | Forward, 6 | Cluster, 6 | Valkey Cluster, 6 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.57 | 0.35 | 0.55 | 0.43 | 0.43 | 0.41 | 0.58 | 0.51 | 0.52 |
| SET | 0.53 | 0.52 | 0.50 | 0.64 | 0.54 | 0.70 | 0.66 | 0.59 | 0.60 |
| Mixed (1 write, 9 reads) | 0.51 | 0.54 | 0.48 | 0.57 | 0.41 | 0.54 | 0.63 | 0.51 | 0.54 |
| Mixed, 16 in flight | 0.62 | 0.69 | 0.51 | 0.65 | 0.71 | 0.69 | 0.76 | 0.66 | 0.75 |

No server pod and no client pod was throttled. The client used at most 4.9 of its 8 CPU.

## Proof that cluster mode reached the primaries

Discus exports its forward and redirect counters only over OTLP, so this report does not have
them. The evidence comes from memtier and a manual check:

- memtier reported 0 MOVED and 0 ASK replies per second in all 36 cluster-mode runs.
- With 6 nodes, cluster mode did about twice the forward-mode rate. Forwards cannot give that
  change.
- Before the runs, `CLUSTER SLOTS` on port 6380 returned the slot map of a 3-node setup. A `SET`
  without `-c` returned `-MOVED` for keys on other nodes.

## Scale-out from 3 to 4 nodes

Discus ran with 3 nodes in cluster mode, and memtier ran SET for 60 seconds. After 20 seconds,
`kubectl scale deployment discus --replicas=4` added a fourth pod.

| Phase | Seconds | SET ops/s |
| --- | --- | ---: |
| 3 nodes, before the scale command | 0 to 20 | 135,300 to 151,600 |
| Lowest second | 23 | 114,600 |
| Join in progress | 24 to 31 | 132,700 to 150,200 |
| 4 nodes | 40 to 59 | 162,800 to 179,100 |

The rate fell by about 20% for one second. With 4 nodes, the mean was 170,900, about 19% above
the rate with 3 nodes. The run had no errors. memtier got about 200 MOVED replies in all, and a GET
run after the scale-out had no misses. This was one run, so it shows the size of the dip, not its
spread.

Two anomalies came after the scale-out:

- `DBSIZE` reported 1,254,945 keys on each of the 4 nodes, for a key space of 1,000,000 keys. The
  value did not go back to 1,000,000 in the 2 minutes after the run.
- Two of the three original nodes started a Raft election about every 2 seconds. The other nodes
  rejected each vote, because their leader lease had not expired. The elections did not stop
  before the teardown.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- `INFO discus` reported `discus_build:dev` on the 0.5.0-rc.1 image. The image tag and the chart
  version were 0.5.0-rc.1.
- Three runs do not show whether the 3-node SET median is near 64% or near 70%.

## Files in this directory

| File | Content |
| --- | --- |
| `results-discus.jsonl` | Discus with 1, 3, and 6 nodes, in forward mode and in cluster mode. |
| `scale-out-memtier.txt` | The memtier output of the scale-out run, with the per-second rates. |
| `scale-out-memtier.json` | The memtier JSON result of the scale-out run. |

`results-discus.jsonl` has one JSON line per run, with the CFS counters of each pod in `cfs`. The
fields `moved_s` and `ask_s` hold the MOVED and ASK replies per second. The Valkey results are in
the [2026-11-10 directory](../2026-11-10-k8s-v0.3.0/).

To run the Discus part again, with the `home-ops` context and Helm available, run this command
from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-11-k8s-v0.5.0-rc.1/results-discus.jsonl \
  --systems discus-cluster-1-0.5.0-rc.1,discus-cluster-3-0.5.0-rc.1,discus-cluster-6-0.5.0-rc.1,discus-1-0.5.0-rc.1,discus-3-0.5.0-rc.1,discus-6-0.5.0-rc.1
```

The scale-out run is not part of the script. It used the setup `discus-cluster-3-0.5.0-rc.1`.
