# Benchmark: Discus 0.5.0-rc.1 in forward mode and cluster mode

Date: 2026-11-11. Cluster: `home-ops`. Discus 0.5.0-rc.1, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). Every node was one pod with
1 CPU and 2 GiB. Every setup got 96 client connections per node. The Discus runs took place from
14:50 to 15:39.

Discus 0.5.0-rc.1 adds slot-aware client routing. A redirect listener on port 6380 answers the
Redis Cluster topology commands, such as `CLUSTER SLOTS`. If a command arrives at a node that is
not the primary of its key, the listener returns `-MOVED`. A Redis Cluster client then sends each
command straight to the primary of its key. Ordinary clients use port 6379, and the node forwards
their commands as before.

This report compares three setups at 1, 3, and 6 nodes:

- **Forward mode**: memtier connects to port 6379 of every pod, as in the earlier reports. The
  nodes forward each command that they do not own. The chart value `clusterClients.enabled` was
  `false`, so the redirect listener did not run.
- **Cluster mode**: one memtier process runs with `--cluster-mode` against port 6380 of one pod.
  It reads the slot map and connects to every node. It uses 8 threads with 12 connections each,
  as the Valkey Cluster runs did. Each node gets 96 connections.
- **Valkey**: the numbers come from the [2026-11-10 report](../2026-11-10-k8s-v0.3.0/report.md)
  and its files `results-matrix.jsonl` and `results-valkey-cluster-6.jsonl`. This run did not
  repeat them. With 1 node, Valkey ran without Cluster. With 3 and 6 nodes, Valkey Cluster had 2
  replicas for each primary.

## Main results

- Discus 0.5.0-rc.1 completed every run at 1, 3, and 6 nodes in both modes. No GET missed, and
  every node reported 1,000,000 keys before and after the runs.
- No cluster-mode run got a `-MOVED` or `-ASK` reply after the prefill. memtier reported 0 MOVED
  and 0 ASK replies per second in all 36 runs.
- In cluster mode with 6 nodes, Discus did 96% of the Valkey Cluster GET rate. The spec target is
  80%. Discus met it.
- In cluster mode, Discus did 64% of the Valkey Cluster SET rate with 3 nodes and with 6 nodes.
  The spec target is 70%. Discus did not meet it.
- Cluster mode doubled the 6-node rates of forward mode. The GET rate went from 198,100 to
  397,700. The SET rate went from 116,600 to 244,900.
- Forward mode in 0.5.0-rc.1 did 97% to 100% of the 0.4.1-rc.1 rate in every workload. The
  difference is within the noise between sessions, so the forward path did not get slower.

## Results

Operations per second. Each value is the median of 3 runs of 30 seconds. The 0.4.1-rc.1 values
come from the [0.4.1-rc.1 report](../2026-11-11-k8s-v0.4.1-rc.1/report.md).

### Throughput, 1 node

| Workload | Forward mode | Cluster mode | 0.4.1-rc.1 | Valkey |
| --- | ---: | ---: | ---: | ---: |
| GET | 81,400 | 83,200 | 82,300 | 87,500 |
| SET | 73,900 | 75,100 | 74,600 | 85,200 |
| Mixed (1 write, 9 reads) | 79,800 | 81,900 | 81,200 | 86,700 |
| Mixed, 16 in flight | 426,700 | 431,300 | 427,800 | 669,500 |

### Throughput, 3 nodes

| Workload | Forward mode | Cluster mode | 0.4.1-rc.1 | Valkey Cluster |
| --- | ---: | ---: | ---: | ---: |
| GET | 220,900 | 219,300 | 221,500 | 226,700 |
| SET | 84,200 | 133,200 | 86,800 | 207,700 |
| Mixed (1 write, 9 reads) | 170,200 | 185,500 | 172,100 | 217,900 |
| Mixed, 16 in flight | 625,500 | 951,300 | 635,500 | 1,774,200 |

### Throughput, 6 nodes

| Workload | Forward mode | Cluster mode | 0.4.1-rc.1 | Valkey Cluster |
| --- | ---: | ---: | ---: | ---: |
| GET | 198,100 | 397,700 | 198,500 | 413,300 |
| SET | 116,600 | 244,900 | 120,700 | 381,200 |
| Mixed (1 write, 9 reads) | 170,500 | 327,200 | 171,100 | 403,200 |
| Mixed, 16 in flight | 482,600 | 1,574,100 | 496,900 | 3,126,600 |

### Discus as a share of Valkey

With 1 node, the share is of Valkey without Cluster. With 3 and 6 nodes, it is of Valkey Cluster.

| Workload | Forward, 1 | Cluster, 1 | Forward, 3 | Cluster, 3 | Forward, 6 | Cluster, 6 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 93% | 95% | 97% | 97% | 48% | 96% |
| SET | 87% | 88% | 41% | 64% | 31% | 64% |
| Mixed (1 write, 9 reads) | 92% | 94% | 78% | 85% | 42% | 81% |
| Mixed, 16 in flight | 64% | 64% | 35% | 54% | 15% | 50% |

### Spec targets

| Target | Result | Met |
| --- | ---: | --- |
| Cluster-mode SET with 3 nodes, at least 70% of Valkey Cluster | 64% | No |
| Cluster-mode SET with 6 nodes, at least 70% of Valkey Cluster | 64% | No |
| Cluster-mode GET with 6 nodes, at least 80% of Valkey Cluster | 96% | Yes |

The spec calls these values targets, not gates.

### Spread of the three runs

In forward mode, the range of the three runs was within 6% of the median for every setup. In
cluster mode with 1 and 6 nodes, the range was within 7%. The cluster-mode runs with 3 nodes
varied more:

| Workload, cluster mode, 3 nodes | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: |
| SET | 145,100 | 133,200 | 132,400 |
| Mixed (1 write, 9 reads) | 174,500 | 185,500 | 194,500 |
| Mixed, 16 in flight | 833,400 | 951,300 | 973,400 |

The best SET run with 3 nodes did 70% of the Valkey Cluster rate. The median did 64%.

## Latency

Latency in milliseconds, median of the 3 runs. In forward mode, each value is the highest
percentile of the memtier processes. Cluster mode has one memtier process.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.17 | 1.74 | 1.14 | 1.70 | 1.07 | 2.10 |
| SET | 1.29 | 1.97 | 1.27 | 2.01 | 1.09 | 2.13 |
| Mixed (1 write, 9 reads) | 1.18 | 1.85 | 1.16 | 1.84 | 1.08 | 2.11 |
| Mixed, 16 in flight | 3.57 | 4.67 | 3.54 | 4.61 | 2.25 | 3.13 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.27 | 2.24 | 1.32 | 2.08 | 1.22 | 2.37 |
| SET | 4.03 | 6.94 | 2.04 | 3.73 | 1.34 | 2.02 |
| Mixed (1 write, 9 reads) | 1.40 | 6.97 | 1.47 | 2.32 | 1.27 | 2.43 |
| Mixed, 16 in flight | 8.32 | 12.35 | 4.77 | 6.97 | 2.51 | 4.42 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.90 | 7.55 | 1.40 | 2.29 | 1.36 | 2.42 |
| SET | 5.28 | 9.47 | 2.37 | 4.09 | 1.45 | 2.65 |
| Mixed (1 write, 9 reads) | 3.74 | 8.32 | 1.71 | 2.85 | 1.36 | 2.45 |
| Mixed, 16 in flight | 17.41 | 50.17 | 5.60 | 8.38 | 3.02 | 4.80 |

In cluster mode, the SET p50 is 0.7 ms to 0.9 ms above Valkey Cluster. The GET p50 is within
0.1 ms. Without pipelining, the latency sets the rate. Thus the SET latency gap gives the SET
throughput gap.

## CPU use

CPU use in cores, as the mean over the server pods, median of the 3 runs. The values come from
`usage_seconds` in the `cfs` field, divided by the run duration.

| Workload | Forward, 1 | Cluster, 1 | Valkey, 1 | Forward, 3 | Cluster, 3 | Valkey Cluster, 3 | Forward, 6 | Cluster, 6 | Valkey Cluster, 6 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 0.57 | 0.35 | 0.55 | 0.43 | 0.43 | 0.41 | 0.58 | 0.51 | 0.52 |
| SET | 0.53 | 0.52 | 0.50 | 0.64 | 0.54 | 0.70 | 0.66 | 0.59 | 0.60 |
| Mixed (1 write, 9 reads) | 0.51 | 0.54 | 0.48 | 0.57 | 0.41 | 0.54 | 0.63 | 0.51 | 0.54 |
| Mixed, 16 in flight | 0.62 | 0.69 | 0.51 | 0.65 | 0.71 | 0.69 | 0.76 | 0.66 | 0.75 |

No server pod and no client pod was throttled in any Discus run. The client used at most 4.9 of
its 8 CPU, in a cluster-mode GET run with 6 nodes. With 6 nodes, cluster mode did about 2.1 times
the SET rate of forward mode with less CPU per pod. No pod used its full CPU, so these runs
measure latency more than capacity.

## Forwards and redirects

The spec asks for the forward counter and `discus.client.redirects` of each node before and after
each run. Discus exports these metrics only over OTLP. `INFO` shows the redirect and client
addresses, but no forward or redirect counts. This benchmark did not add a new export path.

The evidence for cluster mode comes from memtier instead:

- memtier counts the `-MOVED` and `-ASK` replies that it gets. All 36 cluster-mode runs reported 0
  per second. The prefill also ran in cluster mode, so memtier had the slot map before the runs.
- In cluster mode, the 6-node rates were about twice the forward-mode rates. Forwards cannot
  give that change, so the clients reached the primaries directly.

Before the runs, a manual check on a 3-node setup gave these results:

- `CLUSTER SLOTS` on port 6380 returned the slot map with 3 nodes on port 6380.
- A `SET` without `-c` returned `-MOVED` for keys on other nodes.
- A 10-second memtier run in cluster mode reported 0 MOVED replies per second.

The client pod has no `redis-cli`, so the check used the `toolbox` pod.

## Scale-out from 3 to 4 nodes

One extra run tested a node that joins under load. Discus ran with 3 nodes in cluster mode. After
the prefill, memtier ran SET for 60 seconds with 96 connections per node. After 20 seconds, the
command `kubectl scale deployment discus --replicas=4` added a fourth pod.

| Phase | Seconds | SET ops/s |
| --- | --- | ---: |
| 3 nodes, before the scale command | 0 to 20 | 135,300 to 151,600 |
| Lowest second | 23 | 114,600 |
| Join in progress | 24 to 31 | 132,700 to 150,200 |
| 4 nodes | 40 to 59 | 162,800 to 179,100 |

The values come from the per-second output of memtier. The rate fell to 114,600 for one second.
That value is about 20% below the mean of 143,700 before the scale command. From second 32, the
rate went up. From second 40 to 59, the mean was 170,900, about 19% above the rate with 3 nodes.

The run had no errors. memtier reported 3.3 MOVED replies per second over the 60 seconds, about
200 in all. These replies sent memtier to the new owners of the moved slots. A GET run of 5
seconds after the scale-out had no misses.

The spec also asks for the topology requests and their bytes. Discus exports these values only
over OTLP, so this report does not have them.

Two anomalies came after the scale-out:

- `DBSIZE` reported 1,254,945 keys on each of the 4 nodes. The key space has 1,000,000 keys. The
  value did not go back to 1,000,000 in the 2 minutes after the run.
- Two of the three original nodes started a Raft election about every 2 seconds. The other nodes
  rejected each vote request because their leader lease had not expired. The terms went from 7
  to 25 in 2 minutes. The elections did not stop before the teardown.

The scale-out was one run. It shows the size of the dip, not its spread.

## Copies of each key

| Nodes | Discus | Valkey Cluster |
| --- | ---: | ---: |
| 1 | 1 | |
| 3 | 3 | 3 |
| 6 | 3 | 3 |

Valkey Cluster had 2 replicas for each primary. Each Cluster pod ran one primary and two replicas
of other primaries, and the three processes shared the 1 CPU of the pod. The single Valkey node
had no replicas.

## Limits of this benchmark

- The Discus and Valkey numbers come from two sessions on two days. Load from other pods on the
  host can differ between sessions.
- The forward and redirect counters of Discus are not in the results. Discus exports them only
  over OTLP, and this benchmark did not collect OTLP data.
- `INFO discus` reported `discus_build:dev` on the 0.5.0-rc.1 image. The image tag and the chart
  version were 0.5.0-rc.1.
- The cluster-mode runs with 3 nodes varied by up to 15% between runs. Three runs do not show
  whether the SET median is near 64% or near 70%.
- The CPU values come from the cAdvisor counters of the kubelet. The kubelet updates them at
  intervals, so a value can be off by a part of the run.
- In forward mode, the client connects to each pod IP, not through the ClusterIP Service. A real
  client connects through the Service and gets a less even spread.
- In cluster mode, memtier gets the slot map from one pod. A real Redis Cluster client does the
  same.
- All pods and the client ran on one host, `k8s-neo-alpha`, with 32 cores. The network round trip
  does not cross a physical network.

## Files in this directory

| File | Content |
| --- | --- |
| `report.md` | This report. |
| `results-discus.jsonl` | Discus 0.5.0-rc.1 with 1, 3, and 6 nodes, in forward mode and in cluster mode. |
| `scale-out-memtier.txt` | The memtier output of the scale-out run, with the per-second rates. |
| `scale-out-memtier.json` | The memtier JSON result of the scale-out run. |

`results-discus.jsonl` has one JSON line per run, with the CFS counters of each pod in `cfs`. The
fields `moved_s` and `ask_s` hold the MOVED and ASK replies per second. The lines with a `verify`
field hold the key count of each node before and after the runs. The Valkey results are in the
[2026-11-10 directory](../2026-11-10-k8s-v0.3.0/).

To run the Discus part again, with the `home-ops` context and Helm available, run this command
from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 \
  --out docs/benchmarks/2026-11-11-k8s-v0.5.0-rc.1/results-discus.jsonl \
  --systems discus-cluster-1-0.5.0-rc.1,discus-cluster-3-0.5.0-rc.1,discus-cluster-6-0.5.0-rc.1,discus-1-0.5.0-rc.1,discus-3-0.5.0-rc.1,discus-6-0.5.0-rc.1
```

The scale-out run is not part of the script. It used the same setup as
`discus-cluster-3-0.5.0-rc.1`.
