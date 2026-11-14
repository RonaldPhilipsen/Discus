# Command compatibility

This page lists what a single Discus node supports today. The matrix is tested by
`scripts/compat.sh`, which drives `redis-cli` through every command below. Run it with
`mise run compat`.

Discus reports Redis version 7.2.0 in `INFO` and in the `HELLO` reply, because clients choose
features from that value.

## Supported commands

| Family | Commands |
| --- | --- |
| Connection | `PING`, `ECHO`, `QUIT`, `AUTH`, `HELLO` (RESP2 and RESP3), `SELECT` (every database), `COMMAND` (`COUNT`, `INFO`, `GETKEYS`, and the full list), `READONLY`, `READWRITE`, `ASKING`, `CLUSTER` (see below), `CLIENT` (`SETNAME`, `GETNAME`, `ID`, `SETINFO`), `INFO`, `CONFIG GET` (`maxmemory`, `databases`, `maxclients`) |
| Strings | `GET`, `SET` (`EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `KEEPTTL`, `GET`), `GETDEL`, `GETEX`, `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN` |
| Keys and expiry | `DEL`, `EXISTS`, `TYPE`, `RENAME`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `MOVE`, `COPY` (`DB`, `REPLACE`), `SCAN` (`MATCH`, `COUNT`), `RANDOMKEY`, `DBSIZE`, `FLUSHDB`, `FLUSHALL` |
| Hashes | `HSET`, `HSETNX`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HKEYS`, `HVALS` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER` |
| Sorted sets | `ZADD` (`NX`, `XX`, `CH`), `ZSCORE`, `ZMSCORE`, `ZREM`, `ZCARD`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE` (`WITHSCORES`, `LIMIT`), `ZINCRBY`, `ZCOUNT` |

`SWAPDB` returns `-ERR SWAPDB is not allowed in cluster mode`, as a Valkey cluster does.

## Numbered databases

A node serves the databases from 0 to the cluster count minus 1. The default count is 1. The flag
`--databases` and the chart value `databases` set a higher count. See
[Databases](cluster.md#databases). A connection starts in database 0, and `RESET` returns it to
database 0.

| Command | Behavior |
| --- | --- |
| `SELECT <db>` | Selects a database. A number outside the count returns `-ERR DB index is out of range`. |
| `MOVE <key> <db>` | Moves a key to another database. It returns 0 if the destination holds the key. |
| `COPY <source> <destination> [DB <db>] [REPLACE]` | Copies a key, also into another database. |
| `FLUSHDB` | Clears the selected database only. |
| `FLUSHALL` | Clears every database. |
| `DBSIZE`, `SCAN`, `RANDOMKEY` | See the selected database only. |
| `INFO keyspace` | Shows one `db<n>` line for each database that holds keys. |
| `CONFIG GET databases` | Returns the count of the cluster. |

While any member of the cluster lacks support for numbered databases, `SELECT` above 0, `MOVE`, and
`COPY ... DB` return `-ERR multiple databases need every node to support them`. `MOVE` and `COPY`
refuse counter keys. `COPY` to the same key in the same database returns
`-ERR source and destination objects are the same`.

## Unsupported commands

These commands return `-ERR command not supported by Discus`:

`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`,
`SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH`, `PUBSUB`, `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `SORT`,
`RENAMENX`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `BITOP`, `BITFIELD`, `PFADD`,
`PFCOUNT`, `MIGRATE`, `WAIT`, `DEBUG`, every stream command that starts with `X`, and every
command that starts with `GEO`.

`CLUSTER` answers only on a node with a redirect listener or with `--cluster-topology on`. See
[Cluster commands](#cluster-commands). On any other node, `CLUSTER` returns an error that
explains that Discus is not a Redis Cluster and that clients do not need topology awareness.

Any other unknown command returns `-ERR unknown command '<name>'`.

## Cluster commands

A node with a redirect listener or with `--cluster-topology on` answers these commands. Clients use
them to find the owner of each key. See [Cluster clients](cluster.md#cluster-clients).

| Command | Reply |
| --- | --- |
| `CLUSTER SLOTS` | One entry for each run of consecutive slots that have the same owners. The first node of an entry is the primary. The others are replicas. |
| `CLUSTER SHARDS` | One shard for each distinct list of owners. |
| `CLUSTER NODES` | One line for each member. The lines show no replicas. |
| `CLUSTER INFO` | The state of the cluster as text. If every slot has a primary, `cluster_state` is `ok`. |
| `CLUSTER KEYSLOT <key>` | The slot of the key. |
| `CLUSTER MYID` | The ID of this node. |
| `CLUSTER COUNTKEYSINSLOT`, `CLUSTER GETKEYSINSLOT` | An error. Discus does not keep an index by slot. |
| Any other `CLUSTER` subcommand | An error. Discus places the slots by itself. |
| `READONLY`, `READWRITE` | `OK`. On a redirect connection, they let this node serve reads of slots that it holds as a replica. |
| `ASKING` | `OK`. It has no effect, because Discus never sends `-ASK`. |

A Discus node ID has 32 hex digits. A reply adds `00000000` to it, so that clients that expect
40 digits accept it.

`COMMAND` returns the Redis 7 shape. Each entry has the name, the arity, the flags, the first key,
the last key, and the step. The flags are `readonly` or `write`. The lists of ACL categories, tips,
key specifications, and subcommands are empty. `COMMAND INFO <name>` returns the entries of the
named commands. `COMMAND GETKEYS` returns the keys of a command. `COMMAND COUNT` returns the number
of commands. `COMMAND DOCS` returns an empty reply.

`INFO` has a `# Cluster` section. It shows `cluster_enabled:1` while the topology commands answer.
Otherwise it shows `cluster_enabled:0`. Some tools treat a node with `cluster_enabled:1` as a
cluster node.

## Counter keys

Keys with the prefix `--counter-prefix` (default `cnt:`) are counter keys. Every command on them is
linearizable, also on one node. Counter keys accept the string and expiry commands:

| Family | Commands |
| --- | --- |
| Strings | `GET`, `SET` (every option), `GETDEL`, `GETEX`, `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN` |
| Keys and expiry | `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST` |

Any other command on a counter key returns `-ERR counter keys accept only string and expiry
commands`.

`INCR`, `DECR`, `INCRBY`, and `DECRBY` on a counter key accept one expiry option at the end:

```text
INCR key [EX seconds | PX milliseconds | EXAT unix-seconds | PXAT unix-milliseconds]
INCRBY key increment [EX seconds | PX milliseconds | EXAT unix-seconds | PXAT unix-milliseconds]
```

If the key has no deadline, the option sets one, as `EXPIRE NX` does. So the first increment
starts a fixed window, and later increments keep it. This makes a rate limit one atomic
command. On a normal key, these commands keep the Redis arity and reject the option.

The leader of the group stamps each command with its clock. The deadline of a counter key follows
that stamp on every copy, so the copies agree on expiry.

A command that mixes counter keys and other keys returns
`-ERR counter keys and other keys cannot be mixed in one command`. A multi-key command on counter
keys of one group is atomic.

## Commands in a cluster

In a cluster, any node accepts any command and forwards it to the owner of the key. Some commands
behave differently from Redis:

- `MSET` and `DEL` with several keys are not atomic. If one part fails, the command returns
  `-TRYAGAIN` and the other parts stay applied. A retry of the whole command is safe.
- `SCAN` and `RANDOMKEY` see only the keys of the node that the client is connected to. They never
  list counter keys.
- `DBSIZE` adds the counts of every member that answers and divides the sum by the replication
  factor. The result is an estimate. It does not count counter keys.
- `FLUSHDB` and `FLUSHALL` clear every member that answers. This is best-effort. A write that is on
  its way during the flush can survive it. If a counter group has no leader or no quorum, the
  command returns `-TRYAGAIN FLUSHALL cleared the cache and <n> of <G> counter groups`.
- `MOVE` and `COPY` are not atomic across a failover. `COPY` reads its source on the primary.

If another node serves the slot of a command on a redirect connection, the command gets `-MOVED`.
This applies to a command on keys of one slot. Discus never returns `-CROSSSLOT`. It serves a
command with keys in several slots, as it does on the forward listener. These commands differ on a
redirect connection:

- `SCAN` lists only the keys of the slots that this node is the primary of. A cluster client scans
  every primary.
- `DBSIZE` returns the local key count divided by the number of copies of each key. A cluster
  client adds the replies of all primaries.
- Commands on counter keys never get `-MOVED`. The node forwards them to the leader of the group.

See [Running a cluster](cluster.md) for the details.

## Authentication

`AUTH` and `HELLO ... AUTH` accept the shared token or an OAuth 2.0 token. `AUTH OAUTH2 <token>`
selects OAuth 2.0. The username is only a hint. A failed `AUTH` always returns the same
`-WRONGPASS` error. A command that the scopes of the connection do not allow returns
`-NOPERM this user has no permissions to run the '<command>' command`. An expired token turns the
next command into `-NOAUTH`, and the client can send `AUTH` again on the same connection.

See [security.md](security.md) for TLS, the authentication policy, scopes, and the admin port.

## Limits and defaults

| Limit | Default | Flag |
| --- | --- | --- |
| Key size | 64 KiB | `--max-key-bytes` |
| Value size | 64 MiB | `--max-value-bytes` |
| In-flight commands per connection | 10000 | `--max-pipeline` |
| Members in one collection | 1048576 | `--max-collection-members` |
| Memory budget | 1 GiB | `--max-memory` |
| Counter state | 5% of the memory budget | `--counter-memory-percent` |
| Store shards | 256 | `--shards` |

A request over a size limit returns an error. A client that sends more commands than the pipeline
limit without reading its replies stops being read until the replies drain.

When the memory budget is full, a write evicts the least recently used entries from a small
random sample. A single value larger than the whole budget returns
`-OOM value larger than maxmemory` and evicts nothing. Counter state is never evicted. When it
fills its share, a counter write that can grow it returns
`-OOM command not allowed when the counter reserve is full`.

## Build features

Cargo features select optional subsystems and the TLS backend at compile time.
[Security](security.md) describes the security features, and [Telemetry](telemetry.md) describes
`otel` and `pyroscope`. The `discus` section of `INFO` and the first log line show the build
identifier, the enabled features, the TLS backend, and the trust root source.

| Feature | Default | Adds |
| --- | --- | --- |
| `otel` | on | OpenTelemetry metrics and traces |
| `pyroscope` | off | Continuous CPU profiles for Pyroscope |
| `oauth2` | on | OAuth 2.0 token validation |
| `oauth2-introspection` | off | RFC 7662 introspection, turns on `oauth2` |
| `k8s` | on | The Kubernetes `Lease` provider and service account tokens |
| `tls-aws-lc` | on | The `aws-lc-rs` TLS backend |
| `tls-ring` | off | The `ring` TLS backend |
| `tls-openssl` | off | The system OpenSSL as the TLS backend |
| `roots-system` | on | Trust roots from the system store |
| `roots-webpki` | off | Compiled-in Mozilla trust roots |

You must enable exactly one TLS backend and one trust root feature, or the build fails. For
example, this command builds a binary with no optional subsystems:

```bash
cargo build -p discus --no-default-features --features tls-aws-lc,roots-system
```

Run `mise run build-matrix` to check every supported combination.

## Documented deviations

- `SET ... NX` or `XX` with `GET` returns the old value, as in Redis 7.
- `EXPIRE`, `PEXPIRE`, `EXPIREAT`, and `PEXPIREAT` do not accept the `NX`, `XX`, `GT`, or `LT`
  options.
- `INCRBYFLOAT` adds in the 128-bit IEEE format and prints the result as Redis does on 64-bit ARM
  Linux. It prints 17 digits after the point, then removes the trailing zeros. So `0.1` plus `0.2`
  gives `0.3`, and `1e30` gives `1000000000000000000000000000000`. Redis on other platforms uses
  another size of `long double`. Redis on x86-64 uses 80 bits and gives
  `1000000000000000000024696061952` for `1e30`. Redis on Apple silicon uses 64 bits. Its results
  on those platforms differ from Discus in the last digits of large or long values.
- `ZADD` does not accept `GT`, `LT`, or `INCR`. Use `ZINCRBY`.
- `ZRANGE` accepts rank ranges only. It does not accept `BYSCORE`, `BYLEX`, `REV`, or `LIMIT`.
- `SCAN` counts entries examined, as Redis does, so a `MATCH` batch can be empty before the cursor
  ends. It does not accept `TYPE`.
- `DBSIZE` includes expired keys that the background sweep has not removed yet.
- `COMMAND` returns command names only. `COMMAND DOCS` and `COMMAND INFO` return empty arrays.
- `CONFIG GET` answers three names. Other names return an empty array, so `redis-benchmark` prints
  `Could not fetch server CONFIG` and continues.
- `FLUSHDB` and `FLUSHALL` accept `ASYNC` or `SYNC` and ignore it.
- `SWAPDB` is refused, as in a Valkey cluster.
- `MOVE` and `COPY` are not atomic across a failover. In a cluster, `SCAN` and `RANDOMKEY` stay
  local, and `DBSIZE` stays an estimate, for each database.
- Collections keep the whole value under one lock. Every write to a very large collection costs
  time in proportion to its size, so keep collections far below the member limit.
- There is no persistence. A restart starts with an empty cache.

In a cluster, replication adds these deviations:

- Reads can be stale, because a secondary can serve a read before it receives the last write.
  Read-your-writes holds on a best-effort basis for one connection only. It ends with the
  connection, and it does not cover a key that did not fit the write set.
- A node loss can lose the last writes, including deletes. After a lost `DEL`, the old value stays
  until its TTL ends. A cache that relies on deletes for invalidation needs a TTL as a backstop.
- A write to a failed primary waits up to 3 s for a new primary. If the delivery of a write is in
  doubt and the write is not safe to send twice, the write returns `-TRYAGAIN`. Examples are
  `INCR`, `APPEND`, `GETDEL`, `SPOP`, and `SET NX`. A retried `SET NX` can report a failure for a
  value that the client did set.
- A normal-key `INCR` can lose increments across a failover. Collections replicate as whole
  values, so during a split-brain only the field updates of one side survive.
- A delete leaves a tombstone for one hour. Tombstones take memory, up to 5% of `--max-memory`.
- `MSET` and multi-key `DEL` hold their failed parts like single writes. If a part still fails,
  the command returns `-TRYAGAIN` with the other parts applied.
- `RENAME` is not atomic. A failure in a cluster can lose the value.
- A cluster `RENAME` refuses a value whose encoded form is larger than `--max-value-bytes`. The
  source then stays unchanged.

Counter keys add these deviations:

- A counter command that cannot reach a leader within `--counter-timeout-ms` (default 1 s) returns
  `-TRYAGAIN the counter group has no leader, try again later`. Nothing was applied.
- A counter command whose outcome is in doubt returns
  `-TRYAGAIN the counter command did not finish, it may have applied`. A retry of an `INCR` can
  then count twice.
- Below three nodes, a counter failover can lose the increments that the follower missed. With one
  node, a node loss loses every counter.
- During a partition that makes both sides declare the other side `DEAD`, both sides can serve
  counters. On heal, the counters of the losing side are lost.
- A full cold restart loses every counter.

## Baseline

This is a smoke test of one node on one machine, not a gate. Run it with `mise run bench`, which
builds a release binary and runs `redis-benchmark --tls -t set,get -n 200000 -P 16 -q` over the
loopback interface. The numbers are from 2026-11-13.

| Item | Value |
| --- | --- |
| Machine | AMD Ryzen 9 9950X3D, 16 cores (32 threads), Linux 7.2 |
| `SET` | 1,183,432 requests per second, p50 0.623 ms |
| `GET` | 2,985,074 requests per second, p50 0.119 ms |
