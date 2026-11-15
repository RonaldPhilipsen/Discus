# Benchmark: Discus 0.7.1 against Valkey Cluster and Valkey Sentinel, with a CPU profile of SET

Date: 2026-11-15. Cluster: `home-ops`, node `k8s-neo-alpha`. Discus `0.7.1-pyroscope`, Valkey 9.

This run follows the [benchmark methodology](../methodology.md). Every connection uses TLS on both
sides. The run also looks at where a SET spends its CPU, and at what can make SET faster.

In forward mode, memtier connects to port 6379 of every pod. The nodes forward the commands that
they do not own. In cluster mode, memtier reads the slot map from port 6380 and sends each command
to the primary of its key.

The Valkey numbers come from the
[TLS run of 2026-11-13](../2026-11-13-k8s-v0.5.0-rc0-tls/report.md). This run did not run Valkey
again.

## Main results

- Discus completed all 72 runs. No GET missed, and memtier got 0 MOVED and 0 ASK replies in the
  cluster-mode runs.
- On 1 node, Discus did more operations per second than Valkey in all four workloads.
- In cluster mode on 3 and 6 nodes, Discus did more GET and pipelined operations per second than
  Valkey Cluster and Valkey Sentinel. Valkey Cluster did more SET operations per second.
- In cluster mode on 3 nodes, Discus did 131,300 SET operations per second. Valkey Cluster did
  157,400, and Valkey Sentinel did 60,400. On 6 nodes, Discus did 224,000, Valkey Cluster did
  279,700, and Valkey Sentinel did 52,000.
- One SET in cluster mode on 3 nodes costs 13.8 µs of CPU over the three pods. One GET costs
  7.9 µs.
- The CPU profile of SET has no single large cost in the Discus code. The reply to the client
  takes 6.15 µs of the 13.8 µs, and all replication work together takes 3.0 µs.
- The reply to the client costs 1.15 µs more for a SET than for a GET. This run did not find the
  cause. It is the largest cost that only SET has.

## Throughput

Operations per second, median of 3 runs of 30 seconds, rounded to hundreds.

### Throughput, 1 node

| Workload | Discus, forward | Discus, cluster | Valkey |
| --- | ---: | ---: | ---: |
| GET | 77,600 | 75,600 | 67,700 |
| SET | 70,200 | 70,200 | 66,500 |
| Mixed (1 write, 9 reads) | 74,600 | 72,700 | 67,000 |
| Mixed, 16 in flight | 447,300 | 463,800 | 410,300 |

### Throughput, 3 nodes

| Workload | Discus, forward | Discus, cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 198,500 | 190,600 | 174,400 | 66,800 |
| SET | 84,100 | 131,300 | 157,400 | 60,400 |
| Mixed (1 write, 9 reads) | 154,500 | 175,800 | 167,500 | 64,600 |
| Mixed, 16 in flight | 652,700 | 962,800 | 445,000 | 375,100 |

### Throughput, 6 nodes

| Workload | Discus, forward | Discus, cluster | Valkey Cluster | Valkey Sentinel |
| --- | ---: | ---: | ---: | ---: |
| GET | 191,400 | 349,900 | 313,300 | 61,400 |
| SET | 116,200 | 224,000 | 279,700 | 52,000 |
| Mixed (1 write, 9 reads) | 160,900 | 291,000 | 303,500 | 59,400 |
| Mixed, 16 in flight | 506,100 | 1,605,100 | 735,900 | 337,200 |

The Sentinel columns use the runs with all connections on the primary. With reads on all nodes,
Valkey Sentinel did 189,300 GET/s on 3 nodes and 291,500 on 6 nodes.

The three runs of each Discus workload were within 8% of their median.

## Latency

p50 and p99 in milliseconds, median of the 3 runs.

### Latency, 1 node

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey p50 | Valkey p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.25 | 2.10 | 1.25 | 2.10 | 1.38 | 2.65 |
| SET | 1.37 | 2.19 | 1.35 | 2.24 | 1.40 | 2.72 |
| Mixed (1 write, 9 reads) | 1.28 | 2.13 | 1.26 | 2.10 | 1.38 | 2.67 |
| Mixed, 16 in flight | 3.36 | 4.70 | 3.26 | 4.58 | 3.50 | 5.89 |

### Latency, 3 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1.40 | 2.62 | 1.43 | 2.51 | 1.55 | 3.02 | 4.29 | 6.11 |
| SET | 4.06 | 7.13 | 2.13 | 3.74 | 1.76 | 2.53 | 4.67 | 6.33 |
| Mixed (1 write, 9 reads) | 1.52 | 7.71 | 1.60 | 2.73 | 1.62 | 3.10 | 4.35 | 6.46 |
| Mixed, 16 in flight | 8.06 | 12.41 | 4.64 | 7.23 | 10.30 | 16.38 | 11.52 | 19.58 |

### Latency, 6 nodes

| Workload | Forward p50 | Forward p99 | Cluster p50 | Cluster p99 | Valkey Cluster p50 | Valkey Cluster p99 | Sentinel p50 | Sentinel p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 2.85 | 7.87 | 1.60 | 2.78 | 1.76 | 3.20 | 9.09 | 12.73 |
| SET | 5.28 | 9.47 | 2.43 | 4.29 | 1.94 | 3.57 | 10.69 | 14.46 |
| Mixed (1 write, 9 reads) | 3.90 | 9.02 | 1.85 | 3.18 | 1.77 | 3.29 | 9.41 | 12.80 |
| Mixed, 16 in flight | 16.19 | 50.43 | 5.60 | 8.57 | 11.84 | 19.58 | 25.73 | 42.49 |

The Sentinel primary takes all 288 or 576 connections, so its latency is high on 3 and 6 nodes.

## CPU per operation

Microseconds of server CPU for each operation: the `usage_seconds` of all server pods together,
divided by the number of operations. The value is the median of the 3 runs.

| Setup | SET | Mixed, 16 in flight |
| --- | ---: | ---: |
| Discus, cluster, 3 nodes | 13.5 | 2.1 |
| Valkey Cluster, 3 nodes | 11.8 | 3.5 |
| Discus, cluster, 6 nodes | 16.0 | 2.6 |
| Valkey Cluster, 6 nodes | 15.1 | 5.2 |

In forward mode, a SET cost 19.9 µs on 3 nodes and 32.5 µs on 6 nodes.

These counters leave out the kernel time for the network (softirq). The methodology explains why
this favors Valkey.

## Where a SET spends its CPU

The numbers in this section come from the Pyroscope CPU profiles of two runs in cluster mode on 3
nodes. The first SET run is from 12:51:03 to 12:51:31 UTC on 2026-11-15. The first GET run is from
12:50:28 to 12:50:56 UTC. Each value is the CPU time of all three pods, divided by the number of
operations in the window. The profile counts user time and system call time of the Discus process.

| Part | µs for each SET | µs for each GET |
| --- | ---: | ---: |
| Client connection task | 10.2 | 7.5 |
| Replication | 3.0 | 0.0 |
| Other tasks (I/O driver, TTL sweep, Raft) | 0.6 | 0.4 |
| Total | 13.8 | 7.9 |

The client connection task reads the request, runs the command, and writes the reply:

| Part of the client connection task | µs for each SET | µs for each GET |
| --- | ---: | ---: |
| `writev` of the reply | 6.15 | 5.00 |
| `recv` of the request | 0.98 | 0.87 |
| TLS and buffer work on the read path | 0.55 | 0.47 |
| The command, from `Pipeline::submit` | 1.62 | 0.58 |
| Other (TLS on the write path, parser, task overhead) | 0.9 | 0.6 |

Replication has three parts. A primary sends batches of entries to the other two owners of a key,
and each owner applies them.

| Part of replication | µs for each SET | Largest items |
| --- | ---: | --- |
| QUIC drivers | 1.12 | UDP `sendmsg` 0.48, packet decode 0.23, packet build 0.21 |
| Inbound batches | 1.01 | `Store::apply` 0.56, `decode_frame` 0.39 |
| Send loop | 0.87 | `fill` 0.39, `encode_frame` 0.29 |

Three costs are spread over all tasks. The allocator takes 0.72 µs for each SET, and 0.09 µs for
each GET. `memmove` takes 0.48 µs, and AES-GCM takes 0.37 µs. The TTL sweep takes 0.13 µs.

In forward mode, one SET costs 22.4 µs. The QUIC drivers take 5.0 µs and the inbound streams take
2.8 µs. A node forwards two of every three writes to the primary, and each forward is a request
and a reply over QUIC.

## What can make SET faster

Valkey Cluster did 20% more SET operations per second than Discus in cluster mode. To match it,
Discus must save about 2.3 µs of the 13.8 µs for each SET. The items are in the order of their
possible gain.

### 1. The reply `writev` costs more for a SET

A SET reply has 5 bytes, and a GET reply has 256 bytes of value. But the `writev` of a SET reply
costs 6.15 µs, and the `writev` of a GET reply costs 5.00 µs. The difference of 1.15 µs is the
largest cost that only SET has.

This run did not find the cause. One possible cause is that the kernel does network work for
replication packets inside this system call. A CPU profile with kernel stacks on the node can
show it. A count of the `writev` calls for each SET on a local cluster can show whether a SET
writes more than once.

### 2. QUIC work on the one worker thread

The QUIC drivers take 1.12 µs for each SET. Valkey replicates over kernel TCP. The kernel then
handles acknowledgments and packets outside the process, and that work does not count against the
1 CPU of the pod.

Larger batches cut this cost, because each batch needs fewer packets and system calls for each
key. Discus acknowledges a SET before it replicates, so a short wait before a batch adds
replication lag but no client latency. Today the send loop only yields twice before it takes the
queue (`SEND_YIELDS` in `crates/discus/src/replication/mod.rs`). Discus has no metric for the
batch size. Add that metric first, to see how large the batches are now.

### 3. Read and encode each entry once

With 3 copies on 3 nodes, every key goes to two peers. Each peer has its own queue and its own
send loop. `fill` reads the entry from the store once for each peer, and the codec encodes it once
for each peer. One read and one encoding for both peers saves about 0.35 µs.

### 4. Copies of the value

The RESP parser copies each argument of a command out of the read buffer (0.15 µs, plus 0.18 µs
in the allocator). `decode_frame` copies each key and each value again on each replica (0.17 µs,
most of it in the slow path of the allocator).

A slice of the buffer avoids a copy, but a slice keeps its whole buffer alive. The read buffer of
a client has at least 64 KiB of room, so one stored value of 256 bytes can hold 64 KiB. The memory
budget does not count that memory, and an eviction does not free it while another value in the
same buffer lives. For this reason, values must stay copies.

One part is safe. On a replica, the store copies a key only when it inserts a new key. For a key
that the replica already holds, the key from `decode_frame` is waste. A key that is a slice of the
frame lives only until the batch is applied. Discus 0.7.2-rc0 has this change. See the
[0.7.2-rc0 report](../2026-11-15-k8s-v0.7.2-rc0/report.md).

### 5. The write set

The write set records the writes of a connection, so that a later read on that connection sees
them. `WriteSet::record` and `evict_oldest` together take about 0.3 µs for each SET. The
benchmark writes random keys, so the set of each connection is always full and each SET evicts
the oldest key.

### What the items add up to

Items 3, 4, and 5 together save about 1 µs, which is 7% of the CPU for each SET. That is less
than the 2.3 µs that Discus needs to match Valkey Cluster. Item 1 or item 2 must also give a
gain.

The TTL sweep stays as it is. It costs 0.13 µs for each SET, and real workloads need it.

## Limits of this benchmark

The [methodology](../methodology.md#limits-of-every-benchmark) lists the limits of every run. This
run also has these limits:

- The Valkey numbers are two days older than the Discus numbers. Load from other pods on the host
  can differ between the sessions.
- The clients present a certificate from a separate client CA. The 0.7.1 chart refuses to verify
  clients against the cluster CA. The Valkey runs of 2026-11-13 used one CA for nodes and clients.
  A client connects once, so this does not change the throughput.
- The client used at most 6.1 of its 8 CPU. The kernel throttled no client run.
- The kernel throttled the Discus server pods in a few periods only. The most was 29 periods in
  the 3 pipelined forward-mode runs on 6 nodes. No SET run in cluster mode was throttled.
- The 3-node runs are from 13:50 to 14:06, and the 1-node and 6-node runs are from 14:21 to 14:49.
- The profile numbers come from one run of each workload. The SET run did 133,200 operations per
  second, and the GET run did 205,300.
- The cause of the higher `writev` cost for SET is not known. Section 1 above is a guess until a
  kernel profile shows the cause.

## Files in this directory

- `results.jsonl` has one JSON line per Discus 0.7.1 run. The fields `start_ms` and `end_ms`
  select the run in Pyroscope.

To run the Discus setups again, run this command from the repository root:

```bash
S=discus-1-0.7.1-pyro,discus-cluster-1-0.7.1-pyro
S=$S,discus-3-0.7.1-pyro,discus-cluster-3-0.7.1-pyro
S=$S,discus-6-0.7.1-pyro,discus-cluster-6-0.7.1-pyro
python3 scripts/bench-k8s.py --systems "$S" --out results.jsonl
```
