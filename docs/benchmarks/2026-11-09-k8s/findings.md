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
