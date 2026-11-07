# Command compatibility

This page lists what a single Discus node supports today. The matrix is tested by
`scripts/compat.sh`, which drives `redis-cli` through every command below. Run it with
`mise run compat`.

Discus reports Redis version 7.2.0 in `INFO` and in the `HELLO` reply, because clients choose
features from that value.

## Supported commands

| Family | Commands |
| --- | --- |
| Connection | `PING`, `ECHO`, `QUIT`, `AUTH`, `HELLO` (RESP2 and RESP3), `SELECT` (database 0 only), `COMMAND` (names and count), `CLIENT` (`SETNAME`, `GETNAME`, `ID`, `SETINFO`), `INFO`, `CONFIG GET` (`maxmemory`, `databases`, `maxclients`) |
| Strings | `GET`, `SET` (`EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `KEEPTTL`, `GET`), `GETDEL`, `GETEX`, `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN` |
| Keys and expiry | `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `SCAN` (`MATCH`, `COUNT`), `RANDOMKEY`, `DBSIZE`, `FLUSHDB`, `FLUSHALL` |
| Hashes | `HSET`, `HSETNX`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HKEYS`, `HVALS` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER` |
| Sorted sets | `ZADD` (`NX`, `XX`, `CH`), `ZSCORE`, `ZMSCORE`, `ZREM`, `ZCARD`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE` (`WITHSCORES`, `LIMIT`), `ZINCRBY`, `ZCOUNT` |

## Unsupported commands

These commands return `-ERR command not supported by Discus`:

`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`,
`SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH`, `PUBSUB`, `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `SORT`,
`RENAME`, `RENAMENX`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `BITOP`, `BITFIELD`, `PFADD`,
`PFCOUNT`, `MIGRATE`, `WAIT`, `DEBUG`, every stream command that starts with `X`, and every
command that starts with `GEO`.

`CLUSTER` returns an error that explains that Discus is not a Redis Cluster and that clients do not
need topology awareness.

Any other unknown command returns `-ERR unknown command '<name>'`.

## Commands in a cluster

In a cluster, any node accepts any command and forwards it to the owner of the key. Some commands
behave differently from Redis:

- `MSET` and `DEL` with several keys are not atomic. If one part fails, the command returns
  `-TRYAGAIN` and the other parts stay applied. A retry of the whole command is safe.
- `SCAN` and `RANDOMKEY` see only the keys of the node that the client is connected to.
- `DBSIZE` adds the counts of every member that answers.
- `FLUSHDB` and `FLUSHALL` clear every member that answers.

See [Running a static cluster](cluster.md) for the details.

## Limits and defaults

| Limit | Default | Flag |
| --- | --- | --- |
| Key size | 64 KiB | `--max-key-bytes` |
| Value size | 64 MiB | `--max-value-bytes` |
| In-flight commands per connection | 10000 | `--max-pipeline` |
| Members in one collection | 1048576 | `--max-collection-members` |
| Memory budget | 1 GiB | `--max-memory` |
| Store shards | 256 | `--shards` |

A request over a size limit returns an error. A client that sends more commands than the pipeline
limit without reading its replies stops being read until the replies drain.

When the memory budget is full, a write evicts the least recently used entries from a small
random sample. A single value larger than the whole budget returns
`-OOM value larger than maxmemory` and evicts nothing.

## Build features

Cargo features select optional subsystems and the TLS backend at compile time. The subsystems
arrive in later releases, so today each feature changes only what `INFO` reports. The `discus`
section of `INFO` and the first log line show the build identifier, the enabled features, the TLS
backend, and the trust root source.

| Feature | Default | Adds |
| --- | --- | --- |
| `otel` | on | OpenTelemetry metrics and traces |
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
- `ZADD` does not accept `GT`, `LT`, or `INCR`. Use `ZINCRBY`.
- `ZRANGE` accepts rank ranges only. It does not accept `BYSCORE`, `BYLEX`, `REV`, or `LIMIT`.
- `SCAN` counts entries examined, as Redis does, so a `MATCH` batch can be empty before the cursor
  ends. It does not accept `TYPE`.
- `DBSIZE` includes expired keys that the background sweep has not removed yet.
- `COMMAND` returns command names only. `COMMAND DOCS` and `COMMAND INFO` return empty arrays.
- `CONFIG GET` answers three names. Other names return an empty array, so `redis-benchmark` prints
  `Could not fetch server CONFIG` and continues.
- `FLUSHDB` and `FLUSHALL` behave the same. Both accept `ASYNC` or `SYNC` and ignore it.
- Collections keep the whole value under one lock. Every write to a very large collection costs
  time in proportion to its size, so keep collections far below the member limit.
- There is no persistence. A restart starts with an empty cache.

## Baseline

This is a smoke test of one node on one machine, not a gate. Run it with `mise run bench`, which
builds a release binary and runs `redis-benchmark -t set,get -n 200000 -P 16 -q` over the loopback
interface.

| Item | Value |
| --- | --- |
| Machine | AMD Ryzen 9 9950X3D, 16 cores (32 threads), Linux 7.2 |
| `SET` | 1,149,425 requests per second, p50 0.679 ms |
| `GET` | 4,255,319 requests per second, p50 0.095 ms |
