# Benchmark methodology

This document describes the rules for the Kubernetes benchmarks in this directory.
[`scripts/bench-k8s.py`](../../scripts/bench-k8s.py) applies them. Each report describes its
setups and names the points where it does not follow these rules.

## Closed-loop load

The load generator is `memtier_benchmark`. Each connection sends a request, waits for the reply,
and then sends the next request. This is a closed-loop load. With pipelining, a connection keeps
a fixed number of requests in flight instead of one.

With a closed-loop load, the throughput is about the number of requests in flight divided by the
latency of one request. More connections give more operations per second, also with servers that
are not faster. The number of connections is part of the result.

## Node counts and setups

Each benchmark runs every setup with 1, 3, and 6 nodes. A node is one pod with 1 CPU and 2 GiB.

| Nodes | Setups |
| --- | --- |
| 1 | Discus, and Valkey without Cluster or Sentinel |
| 3 and 6 | Discus, Valkey Cluster with 2 replicas for each primary, and Valkey Sentinel |

Discus keeps 3 copies of each key. Valkey Cluster with 2 replicas for each primary keeps 3 copies
too, so both setups do the same replication work. Each Cluster pod runs one primary and two
replicas of other primaries, and the three processes share the 1 CPU of the pod. The three copies
of a slot are always on three different pods.

Valkey Sentinel keeps a copy on every node: 3 copies with 3 nodes and 6 copies with 6 nodes. Only
the primary takes writes, so the Sentinel write rate does not go up with more nodes. A single
Valkey node has no replicas.

## The same load for every setup

Every setup gets the same number of client connections per node. The default is 96 connections
per node. A six-node setup gets 576 connections in all. You set the number with `--connections`
and the total number of memtier threads with `--threads`.

The client spreads the connections evenly over the nodes that take client requests:

| Setup | How the connections reach the nodes |
| --- | --- |
| Discus | One memtier process for each pod, with 96 connections to the pod IP. Each pod gets the same number of connections. |
| Redis or Valkey Cluster | One memtier process in Cluster mode. It opens 96 connections to every primary. |
| Sentinel, client on the primary | One memtier process with all connections to the primary, 96 for each node, because only the primary takes writes. Six nodes give 576. |
| Sentinel, reads on all nodes | One memtier process for each pod, with 96 connections to the pod IP. |
| One node | One memtier process with 96 connections. |

The Discus client does not connect through the ClusterIP Service. The Service places each
connection on a random pod, so some pods get more connections than others. A real client
connects through the Service, but the even spread makes the comparison with Cluster exact.

## CPU measurement

The script reads the cAdvisor metrics of the kubelet before and after each run. The `cfs` field
of each result has these counters for each pod:

- `usage_seconds`: the CPU time that the pod used. Divide it by the run duration to get the CPU
  use in cores.
- `cfs_throttled_periods` and `cfs_throttled_seconds`: the time that the kernel stopped the pod
  at its CPU limit.

The `top` field holds one `kubectl top` sample from the middle of the run. Do not use it to
compare setups. The samples can be far from the real use: in one Valkey Cluster run at
403,000 GET/s, three of the six pods showed 1m to 5m CPU.

## Profiling

From 2026-11-13, every Discus run has CPU profiling on. The Discus nodes push profiles to the
Pyroscope server of the cluster. The script does this by default. Each result line has
`start_ms` and `end_ms`. Use them to select the time range of one run in Pyroscope.

The release images do not contain the `pyroscope` feature. For each image that you test, build
and push a second image with the feature, with the same tag and the suffix `-pyroscope`. Run
this command in the repository root:

```sh
docker build --build-arg FEATURES=pyroscope --build-arg VERSION=<tag> \
  -t ghcr.io/ronaldphilipsen/discus:<tag>-pyroscope .
```

The `VERSION` argument sets the `service_version` label of the profiles. Use it to select or
compare the builds in Pyroscope.

On the static glibc image, profiling costs at most 2% of the throughput. See the
[static glibc report](https://github.com/RonaldPhilipsen/Discus/blob/e9affe8/docs/benchmarks/2026-11-13-k8s-static-glibc/report.md).
To turn profiling off, pass `--pyroscope-url ""`.

## Client limits

The client pod has a limit of 8 CPU, and its counters are in the `cfs` field too. If the client
uses most of its 8 CPU or is throttled, the client sets the result, not the servers. In that case,
the report must say so. With 4 memtier threads, the client limited Cluster to about 230,000
operations per second.

## Limits of every benchmark

These limits apply to every report. A report lists only its own extra limits.

- All pods and the client run on one host, `k8s-neo-alpha`, with 32 cores. The network round trip
  does not cross a physical network.
- The kubelet updates the cAdvisor counters at intervals, so a CPU value can be off by a part of
  the run. Use the CPU values only for large differences.
- The CPU limit does not count kernel network work. This favors Valkey, which replicates over
  kernel TCP. Discus runs QUIC in user space, and that work counts against its 1 CPU.
- Without pipelining, no pod reaches its full CPU. The latency sets the rate, so these runs
  measure latency more than capacity. The pipelined runs come closer to capacity.
- A report that reuses the Valkey numbers of an earlier day compares two sessions. Load from other
  pods on the host can differ between sessions.
