# Benchmark: Discus, Redis, and Valkey on Kubernetes

Date: 2026-11-09. Cluster: `home-ops`. Discus 0.2.2, Redis 8, Valkey 9.

Status: complete. The run ended at 22:44 without failures.

## What was measured

Each "node" is one Kubernetes pod with 1 CPU and 2 GiB of memory. The request equals the limit, so
each pod is Guaranteed. All server pods ran on `k8s-neo-alpha` (32 cores, Talos, Cilium in native
routing mode). No setup writes to disk: Redis and Valkey run with `save ""` and `appendonly no`.
Redis and Valkey use `maxmemory 1536mb` (75% of the limit, the same budget that Discus takes) and
`allkeys-lru`.

The setups:

| Setup | Layout |
| --- | --- |
| `discus-3` | Discus Helm chart, 3 pods, replication factor 3, through the ClusterIP Service. |
| `redis-sentinel-3`, `valkey-sentinel-3` | 1 primary and 2 replicas. Each pod also runs a Sentinel process in the same 1 CPU. The client talks to the primary. |
| `*-cluster-noreplica-3` | Redis Cluster with 3 primaries and no replicas. One node loss loses a third of the keys. |
| `*-cluster-replica-3` | Redis Cluster with 3 primaries and 3 replicas. Each pod runs one primary and the replica of the next primary, so two processes share 1 CPU and 2 GiB. |
| `discus-1`, `redis-1`, `valkey-1` | One node. |

The load generator is `memtier_benchmark` 2.2.1 in its own pod on the same host, with a limit of
8 CPU. It used 2 CPU or less, so it was not the bottleneck.

- Data: 1,000,000 keys (`key:1` to `key:1000000`), 256-byte values, loaded before the runs.
- Key choice: uniform random.
- Connections: 4 threads with 25 connections each, 100 connections in total. In Cluster mode,
  memtier opens these connections to every primary, so a Cluster setup has 300 connections.
- Each workload ran 30 seconds, 3 times. The tables show the median and the range.
- Latency percentiles are per request, in milliseconds.

The workloads:

| Workload | Meaning |
| --- | --- |
| `get` | Reads only. |
| `set` | Writes only. |
| `mixed-1w9r` | 1 write for every 9 reads. |
| `mixed-1w9r-p16` | The same mix, with 16 commands in flight per connection (pipelining). |

For Sentinel, the `get` workload ran a second time with the reads spread over all three nodes, in
three memtier processes. Those rows say "reads on all 3 nodes". Their latency columns show the
worst of the three processes.

## Main results

### Three nodes

- Reads: Discus did 217,000 GET/s. This is 2.5 times Sentinel with all reads on the primary
  (87,000 to 88,000). It is close to Redis Cluster (about 240,000) and close to Sentinel with the
  reads spread over all nodes (about 241,000). Discus reached this through one Service address. The
  client did not need to know the topology.
- Writes: Discus did 52,500 SET/s. Sentinel did about 80,000 and Cluster about 225,000 to 237,000.
  With 3 nodes and replication factor 3, every node stores every key, so every write costs CPU on
  all three nodes. Writes therefore do not scale with the node count at 3 nodes. Discus on 3 nodes
  also wrote 23% slower than Discus on 1 node (68,100).
- Write latency: Discus had a p99 of 4.3 ms for SET. Sentinel had 1.65 ms and Cluster 2.1 to 2.3 ms.
- Mixed load (1 write, 9 reads): Discus did 141,000 ops/s. This is 1.7 times Sentinel (about 84,500)
  and 0.6 times Cluster (about 230,000 to 242,000). Discus had the lowest p50 (0.59 ms) but a p99
  of 2.7 ms, because the writes are slow.
- Pipelining: Discus gained least from pipelining. With 16 commands in flight it did 398,000 ops/s.
  Sentinel did about 640,000 and Cluster about 1.8 million. Discus had a p50 of 4.2 ms.
- Replicas in Redis Cluster cost little here: 3% to 5% for writes. The replica shares the CPU of a
  primary, but kernel network work is likely not charged to the pod (see the limits).
- Redis 8 and Valkey 9 were within 3% of each other in every 3-node setup.
- No run had a GET miss. Every Discus node held all 1,000,000 keys (314 MiB each). Each Cluster
  primary held about 333,000 keys and its replica the same set.

### One node

- Reads: Discus did 78,100 GET/s. Redis did 86,200 and Valkey 87,500, so Discus was 9% to 11%
  slower. Its p99 was lower: 1.90 ms against 2.16 to 2.19 ms.
- Writes: Discus did 68,100 SET/s. Redis and Valkey did 86,100 to 86,200, so Discus was 21% slower.
- Mixed load: Discus did 75,900 ops/s. Redis and Valkey did 86,400 to 87,600, so Discus was 13%
  slower.
- Pipelining: Discus did 352,000 ops/s. Redis did 659,000 and Valkey 671,000, so Discus reached
  about half. Its p50 was 4.5 ms against 2.3 to 2.4 ms.
- Redis 8 and Valkey 9 were within 2% of each other, except with pipelining, where Valkey was 2%
  faster.

### What this means for Discus

- At 3 nodes, Discus behaves like Sentinel with read scaling built in, but with slower writes than
  Sentinel. It does not reach the write throughput of Redis Cluster. Cluster stores each key once,
  or twice with replicas. Discus stores each key three times.
- The largest gaps are the cost of one write on one node and the gain from pipelining. Both are
  visible already on one node, so a profile of a single node can show where the time goes.

## Limits of this benchmark

- All pods ran on one host. Traffic between nodes never crossed a physical network, so network
  latency between nodes is near zero. On real separate machines, Discus forwards and replicates over
  the network, and its numbers drop more than those of a single Redis primary.
- The CPU limit does not hold all CPU work. A single-threaded Redis process was saturated at about
  80,000 operations per second, but used only 45% to 60% of its CPU quota, with no throttling. A
  likely cause is that kernel network work (softirq) is not charged to the pod. This favors Redis
  and Valkey, which replicate over kernel TCP. Discus runs QUIC in user space, and that work counts
  against its quota. This is not proven.
- Cluster rows ran with 300 connections and the others with 100. Compare Cluster throughput with
  the others, but do not compare its latency directly.
- Discus received its connections through the Service, which balances each connection to a random
  pod. Redis and Valkey received direct pod addresses.
- The test does not include failover, node loss, keys larger than 256 bytes, or Discus counter keys.
- The rest of the cluster ran its normal workload during the test. The spread between runs is small,
  so the noise was low.

## Files in this directory

| File | Content |
| --- | --- |
| `report.md` | This report. |
| `results.jsonl` | One JSON line per run, plus the data placement checks (`verify`) and `kubectl top` samples. |
| `run.log` | The log of the run. |
| `build.sh` | Builds `report.md` from `head.md` and the tables. |

The scripts are in the `scripts` directory of the repository. `scripts/bench-k8s.py` deployed every
setup and ran the load. `scripts/bench-k8s-aggregate.py` makes the tables from `results.jsonl`.

To run the benchmark again, with the `home-ops` context and Helm available, run this from the
repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 --out docs/benchmarks/2026-11-09-k8s/results.jsonl
```

The script creates the namespace `discus-bench` and removes each setup after its runs. When you are
done, delete the namespace: `kubectl --context home-ops delete ns discus-bench`.

## Tables

### get

| Setup | Ops/s (median) | Spread min to max | p50 ms | p99 ms | p99.9 ms | Runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| discus-3 | 217,060 | 213,192 to 221,572 | 0.45 | 0.77 | 2.58 | 3 |
| redis-sentinel-3 | 86,980 | 86,791 to 87,178 | 1.11 | 2.17 | 2.54 | 3 |
| redis-sentinel-3 (reads on all 3 nodes) | 241,183 | 233,328 to 249,780 | 0.41 | 0.78 | 1.18 | 3 |
| valkey-sentinel-3 | 87,976 | 84,106 to 88,642 | 1.09 | 2.14 | 2.53 | 3 |
| valkey-sentinel-3 (reads on all 3 nodes) | 242,080 | 242,008 to 242,429 | 0.41 | 0.78 | 1.20 | 3 |
| redis-cluster-noreplica-3 | 241,391 | 238,740 to 245,015 | 1.22 | 2.03 | 2.59 | 3 |
| valkey-cluster-noreplica-3 | 237,481 | 217,907 to 239,572 | 1.24 | 2.06 | 2.70 | 3 |
| redis-cluster-replica-3 | 238,926 | 238,450 to 239,051 | 1.24 | 2.06 | 2.72 | 3 |
| valkey-cluster-replica-3 | 241,271 | 240,376 to 242,500 | 1.21 | 2.03 | 2.62 | 3 |
| discus-1 | 78,113 | 77,674 to 78,160 | 1.26 | 1.90 | 2.42 | 3 |
| redis-1 | 86,177 | 83,607 to 87,409 | 1.12 | 2.19 | 2.62 | 3 |
| valkey-1 | 87,467 | 85,884 to 87,603 | 1.10 | 2.16 | 2.59 | 3 |

GET misses per second summed over all runs: 0.0

### set

| Setup | Ops/s (median) | Spread min to max | p50 ms | p99 ms | p99.9 ms | Runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| discus-3 | 52,538 | 52,396 to 52,923 | 2.04 | 4.29 | 5.25 | 3 |
| redis-sentinel-3 | 80,217 | 79,948 to 80,588 | 1.22 | 1.65 | 2.64 | 3 |
| valkey-sentinel-3 | 81,111 | 80,811 to 81,867 | 1.20 | 1.65 | 2.61 | 3 |
| redis-cluster-noreplica-3 | 237,127 | 236,990 to 238,697 | 1.24 | 2.06 | 2.70 | 3 |
| valkey-cluster-noreplica-3 | 232,615 | 228,166 to 234,998 | 1.26 | 2.11 | 2.91 | 3 |
| redis-cluster-replica-3 | 224,423 | 223,929 to 229,835 | 1.29 | 2.27 | 3.69 | 3 |
| valkey-cluster-replica-3 | 224,910 | 223,059 to 226,178 | 1.27 | 2.25 | 3.92 | 3 |
| discus-1 | 68,106 | 68,101 to 68,767 | 1.45 | 2.24 | 2.70 | 3 |
| redis-1 | 86,070 | 85,944 to 86,658 | 1.12 | 2.19 | 2.58 | 3 |
| valkey-1 | 86,206 | 85,900 to 86,629 | 1.12 | 2.21 | 2.59 | 3 |

GET misses per second summed over all runs: 0.0

### mixed-1w9r

| Setup | Ops/s (median) | Spread min to max | p50 ms | p99 ms | p99.9 ms | Runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| discus-3 | 140,948 | 140,573 to 142,900 | 0.59 | 2.70 | 3.52 | 3 |
| redis-sentinel-3 | 84,195 | 83,272 to 84,358 | 1.15 | 2.24 | 2.64 | 3 |
| valkey-sentinel-3 | 84,781 | 84,500 to 85,223 | 1.14 | 2.22 | 2.65 | 3 |
| redis-cluster-noreplica-3 | 241,825 | 240,096 to 242,158 | 1.22 | 2.01 | 2.54 | 3 |
| valkey-cluster-noreplica-3 | 235,494 | 233,344 to 237,835 | 1.25 | 2.06 | 2.64 | 3 |
| redis-cluster-replica-3 | 230,581 | 229,853 to 232,994 | 1.25 | 2.14 | 3.10 | 3 |
| valkey-cluster-replica-3 | 232,957 | 231,367 to 234,835 | 1.25 | 2.13 | 2.91 | 3 |
| discus-1 | 75,943 | 75,840 to 76,121 | 1.29 | 2.05 | 2.46 | 3 |
| redis-1 | 86,357 | 85,415 to 87,333 | 1.12 | 2.19 | 2.62 | 3 |
| valkey-1 | 87,620 | 87,006 to 87,699 | 1.10 | 2.16 | 2.56 | 3 |

GET misses per second summed over all runs: 0.0

### mixed-1w9r-p16

| Setup | Ops/s (median) | Spread min to max | p50 ms | p99 ms | p99.9 ms | Runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| discus-3 | 397,625 | 397,255 to 417,734 | 4.22 | 7.55 | 9.02 | 3 |
| redis-sentinel-3 | 632,299 | 626,610 to 632,455 | 2.50 | 3.39 | 5.28 | 3 |
| valkey-sentinel-3 | 643,328 | 642,479 to 649,553 | 2.43 | 3.39 | 5.28 | 3 |
| redis-cluster-noreplica-3 | 1,867,860 | 1,865,817 to 1,875,393 | 2.59 | 4.01 | 5.02 | 3 |
| valkey-cluster-noreplica-3 | 1,845,109 | 1,844,933 to 1,855,601 | 2.64 | 4.06 | 5.25 | 3 |
| redis-cluster-replica-3 | 1,793,959 | 1,773,836 to 1,803,352 | 2.70 | 4.13 | 5.38 | 3 |
| valkey-cluster-replica-3 | 1,777,809 | 1,773,692 to 1,791,459 | 2.72 | 4.19 | 5.44 | 3 |
| discus-1 | 352,463 | 349,724 to 356,662 | 4.48 | 5.98 | 6.85 | 3 |
| redis-1 | 658,849 | 654,690 to 659,920 | 2.37 | 3.52 | 5.15 | 3 |
| valkey-1 | 671,442 | 668,512 to 672,888 | 2.33 | 3.31 | 5.05 | 3 |

GET misses per second summed over all runs: 0.0

### Data placement after the runs

- discus-3:
  - 10.42.0.35:6379 ? keys=1000000 mem=314MiB
  - 10.42.0.211:6379 ? keys=1000000 mem=314MiB
  - 10.42.0.114:6379 ? keys=1000000 mem=314MiB
- redis-sentinel-3:
  - 10.42.0.213:6379 master keys=1000000 mem=351MiB
  - 10.42.0.145:6379 slave keys=1000000 mem=351MiB
  - 10.42.0.76:6379 slave keys=1000000 mem=351MiB
- valkey-sentinel-3:
  - 10.42.0.143:6379 master keys=1000000 mem=363MiB
  - 10.42.0.135:6379 slave keys=1000000 mem=363MiB
  - 10.42.0.42:6379 slave keys=1000000 mem=363MiB
- redis-cluster-noreplica-3:
  - 10.42.0.181:6379 master keys=333340 mem=119MiB
  - 10.42.0.143:6379 master keys=333354 mem=119MiB
  - 10.42.0.114:6379 master keys=333306 mem=119MiB
- valkey-cluster-noreplica-3:
  - 10.42.0.124:6379 master keys=333340 mem=120MiB
  - 10.42.0.143:6379 master keys=333354 mem=120MiB
  - 10.42.0.114:6379 master keys=333306 mem=120MiB
- redis-cluster-replica-3:
  - 10.42.0.149:6379 master keys=333340 mem=121MiB
  - 10.42.0.149:6380 slave keys=333306 mem=120MiB
  - 10.42.0.134:6379 master keys=333354 mem=121MiB
  - 10.42.0.134:6380 slave keys=333340 mem=120MiB
  - 10.42.0.118:6379 master keys=333306 mem=121MiB
  - 10.42.0.118:6380 slave keys=333354 mem=120MiB
- valkey-cluster-replica-3:
  - 10.42.0.88:6379 master keys=333340 mem=130MiB
  - 10.42.0.88:6380 slave keys=333306 mem=130MiB
  - 10.42.0.119:6379 master keys=333354 mem=130MiB
  - 10.42.0.119:6380 slave keys=333340 mem=130MiB
  - 10.42.0.157:6379 master keys=333306 mem=130MiB
  - 10.42.0.157:6380 slave keys=333354 mem=130MiB
- discus-1:
  - 10.42.0.58:6379 ? keys=1000000 mem=314MiB
- redis-1:
  - 10.42.0.176:6379 master keys=1000000 mem=349MiB
- valkey-1:
  - 10.42.0.114:6379 master keys=1000000 mem=353MiB
