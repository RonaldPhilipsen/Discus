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
- Bulk handoff on join: current owners stream moved entries to the joining node, with lazy refill as the fallback.
- A small command surface: strings, hashes, sets, and sorted sets.
- No transactions, Lua, pub/sub, streams, queues, or blocking commands.

## Strong counters

Counters are the one place where Discus offers a strong guarantee. Keys under the `cnt:` prefix (configurable) form a separate keyspace backed by Raft groups. `INCR`, `GET`, `SET`, `DEL`, and `EXPIRE` on those keys are linearizable: any node returns the same value, and the minority side of a partition rejects writes with `-TRYAGAIN`.

The tradeoffs are explicit: strong counters pay a consensus round-trip, are CP during partitions, and are lost if the whole cluster restarts cold. Normal keys keep the fast, asynchronous, AP path.

This is a keyspace rather than a per-command flag because `SET` and `GET` on the same key must follow the same path. A strong `INCR` mixed with an asynchronous `SET` would break the guarantee.

## Decisions

- **Deployment:** Kubernetes, single zone initially.
- **Security:** mTLS enforced with cert-manager-issued certificates, including node-to-node traffic.
- **Restarts:** cold restarts are acceptable; the cache starts empty.
- **Latency targets:** local hit p99 ≤ 1 ms, remote hit p99 ≤ 2 ms, and p99 ≤ 5 ms during a membership change.
- **Data types:** strings, hashes, sets, and sorted sets.
- **Strong counters:** linearizable `INCR` on an opt-in counter keyspace (`cnt:` prefix), backed by per-group Raft with RF=3. CP during partitions, and counters are lost on a full cold restart. Normal keys stay AP.
- **Handoff:** join handoff is in v0.1. Current owners stream the moved entries through a `HANDOFF` RPC. The new owner applies only newer HLC entries and falls back to the previous owners for misses. Drain streaming reuses the mechanism in a later release.

## Non-goals

Durability, persistence, transactions, Lua, pub/sub, streams, blocking commands, Redis Cluster protocol compatibility, Sentinel compatibility, cross-datacenter replication, strong consistency, and multi-tenancy.

## Status

Design phase for v0.1. This README records the goal and the reasoning behind the design.

## License

TBD. The intent is a permissive license, and every dependency on the critical path must also be permissively licensed.
