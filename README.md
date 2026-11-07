# Discus

A Redis-compatible cache that scales horizontally and stays highly available without Redis Cluster or Redis Sentinel.

## Goal

Discus is a shared-nothing cache server written in Rust. It speaks RESP, so ordinary Redis clients work, but clients and load balancers never need to know the cluster topology: a connection can land on any node, and the cluster routes each request internally.

The target is a cache, not a durable database. Discus should:

- scale throughput by adding nodes;
- survive node loss without taking the cache offline;
- keep clients topology-unaware;
- stay permissively licensed.

## Why not Redis Cluster or Sentinel?

**Sentinel** provides failover, but not horizontal scale. Replicas are passive and read-only, so the extra nodes do not add usable capacity.

**Redis Cluster** provides scale, but it exposes an N×M slot topology to clients and operators. Clients must understand `MOVED`/`ASK`, multi-key operations are constrained by slots, and resharding is an operational burden.

**Dragonfly** shows that a single logical database can scale without client-visible slots, but it uses a more restrictive license than a permissive open-source license.

Discus takes a different path: gossip-based membership, deterministic placement, and any-node routing.

## Why this design?

**Cache semantics unlock a simpler consistency model.** A cache can tolerate stale reads and last-write-wins conflicts. That removes the need for consensus, two-phase commit, and cross-shard transactions, which are what make distributed databases expensive.

**Shared-nothing scales horizontally.** Each node owns its memory and its slice of the keyspace. There is no shared disk or shared database to become a bottleneck.

**Gossip plus deterministic hashing removes the placement service.** Gossip carries liveness and node metadata, never a key directory. Every node computes the same owners from the member list, so placement needs no central coordinator and no per-key index.

**Any-node routing keeps clients simple.** A load balancer can send each connection to any healthy node. The entry node serves local keys or forwards one hop to an owner. Clients stay ordinary Redis clients.

**Asynchronous replication keeps writes fast.** The primary applies a write locally and acknowledges, then replicates in the background. The tradeoff is cache-grade durability: a node failure can lose the last writes, which is acceptable for a cache.

**Automatic failover replaces Sentinel.** Failure detection and placement recomputation promote a secondary without manual intervention.

**Rust for predictable latency.** No garbage-collector pauses, memory safety, and a mature async networking ecosystem.

## What it does

- One tier of identical nodes: data, routing, gossip, and replication all live in each node.
- Rendezvous hashing over the member list, replication factor 3 by default.
- Any node accepts any key; the entry node routes internally.
- Async replication, read repair, and promotion reconciliation.
- A small command surface: strings, hashes, sets, and sorted sets.
- No transactions, Lua, pub/sub, streams, queues, or blocking commands.

## Non-goals

Durability, persistence, transactions, Lua, pub/sub, streams, blocking commands, Redis Cluster protocol compatibility, Sentinel compatibility, cross-datacenter replication, strong consistency, and multi-tenancy.

## Status

Design phase. This README records the goal and the reasoning behind the design.

## License

TBD. The intent is a permissive license, and every dependency on the critical path must also be permissively licensed.
