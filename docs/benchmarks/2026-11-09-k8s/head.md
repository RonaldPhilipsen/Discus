# Benchmark: Discus, Redis, and Valkey on Kubernetes

Date: 2026-11-09. Cluster: `home-ops`. Discus 0.2.2, Redis 8, Valkey 9.

STATUS_LINE

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

FINDINGS

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

The scripts are in the `scripts` directory of the repository. `scripts/bench-k8s.py` deployed every setup and ran the
load. `scripts/bench-k8s-aggregate.py` makes the tables from `results.jsonl`.

To run the benchmark again, with the `home-ops` context and Helm available, run this from the repository root:

```bash
python3 -I scripts/bench-k8s.py --duration 30 --reps 3 --out docs/benchmarks/2026-11-09-k8s/results.jsonl
```

The script creates the namespace `discus-bench` and removes each setup after its runs. When you are
done, delete the namespace: `kubectl --context home-ops delete ns discus-bench`.

## Tables
