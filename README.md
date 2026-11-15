# ![Discus: connect to any node](docs/resources/logo/discus.svg)

This README is a quick start. The [documentation](#documentation) section links to the full guides.

## What is Discus?

Discus is a Redis-compatible cache server, written in Rust. You add nodes to get more throughput.
If a node fails, the cache stays online. It needs no Redis Cluster and no Redis Sentinel.

Every node is the same. Each node stores data, routes requests, finds the other nodes, and copies
data to them. A client can connect to any node. That node serves the key, or it forwards the
command to the node that owns the key. Thus ordinary Redis clients and load balancers do not have
to know the cluster layout. Redis Cluster clients can also read the slot map and send each command
straight to the owner.

Discus is a cache, not a database. It keeps all data in memory and writes nothing to disk. When
a whole cluster restarts, the cache starts empty.

The main parts of the design are these:

- Gossip (nodes tell each other about members) finds the nodes and detects failures.
- Rendezvous hashing picks three owners for each key from the member list. No central service
  keeps a key directory.
- The primary owner applies a write, answers the client, and copies the write to the other owners
  later. A node failure can lose the last writes.
- When a node fails, a secondary owner becomes the primary. No person has to act.
- When a node joins, the current owners send it the keys that it now owns.

Discus supports strings, hashes, sets, and sorted sets. It does not support transactions, Lua,
Pub/Sub, streams, or blocking commands. [Command compatibility](docs/compatibility.md) lists every
command.

### Strong counters

Keys under the prefix `cnt:` are strong counters. `INCR`, `GET`, `SET`, `DEL`, and `EXPIRE` on
these keys are linearizable: every node returns the same value. During a network partition, the
smaller side refuses writes with `-TRYAGAIN`. Strong counters cost more latency than normal keys,
and a full cold restart loses them. Normal keys keep the fast path.

### Why not Redis Cluster or Sentinel?

Sentinel gives failover, but not more capacity. Its replicas only serve reads.

Redis Cluster gives more capacity, but clients must know the slot layout. They must handle `MOVED`
and `ASK`, and multi-key commands must stay in one slot. If the cluster grows, operators must move
slots by hand.

Discus gives both. A client can connect to any node, and nodes find each other without a central
service.

## Building Discus

Discus builds on Linux with the Rust toolchain. The file `mise.toml` lists the versions that the
project uses.

```sh
cargo build --release -p discus
```

The binary is `target/release/discus`.

Cargo features select optional parts and the TLS library at build time. The default build has
OpenTelemetry, OAuth 2.0, Kubernetes support, and the `aws-lc-rs` TLS library. This command builds
a binary with no optional parts:

```sh
cargo build --release -p discus --no-default-features --features tls-aws-lc,roots-system
```

[Build features](docs/compatibility.md#build-features) lists every feature.

## Running Discus

Every client connection uses TLS, so a node needs a certificate and a key. If you do not have a
certificate, make a test CA and a certificate for `localhost` with the script in this repository:

```sh
scripts/dev-cert.sh certs
```

The script writes `certs/ca.pem`, `certs/cert.pem`, and `certs/key.pem`. Then start one node:

```sh
./target/release/discus --tls-cert certs/cert.pem --tls-key certs/key.pem
```

The node listens for clients on port 6379. After about ten seconds, it forms a cluster of one node.
Run `discus --help` to see every flag.

## Running Discus with TLS

TLS on the client port is always on. There is no plaintext mode. The flags `--tls-cert` and
`--tls-key` take PEM files, and the certificate file can hold a chain.

To ask every client for a certificate (mTLS), add the CA that signs the client certificates:

```sh
./target/release/discus --tls-cert certs/cert.pem --tls-key certs/key.pem \
  --tls-client-ca certs/client-ca.pem
```

Discus can also ask clients for an OAuth 2.0 token or a Kubernetes service account token.
[Security](docs/security.md) tells how to set the authentication policy.

Node-to-node traffic always uses QUIC with mTLS. Each node of a cluster needs a certificate from a
CA that serves only that cluster. The flags are `--cluster-ca`, `--cluster-cert`, and
`--cluster-key`.

## Running a cluster

On Kubernetes, install the Helm chart. Before you install it, create a Secret with the keys
`ca.crt`, `tls.crt`, and `tls.key`.

```sh
helm install <release> oci://ghcr.io/ronaldphilipsen/charts/discus \
  --version <version> \
  --namespace <namespace> \
  --set cluster.tls.secretName=<secret>
```

Outside Kubernetes, give each node the other nodes as seeds with `--peer`, and set
`--lease-provider none`. [Running a cluster](docs/cluster.md) shows a three-node example on one
host. It also covers failure detection, replication, drain, and memory limits.

## Playing with Discus

Use `redis-cli` to send commands. Start a node as described in
[Running Discus](#running-discus), then run this command in another terminal:

```text
$ redis-cli --tls --cacert certs/ca.pem
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET foo bar
OK
127.0.0.1:6379> GET foo
"bar"
127.0.0.1:6379> HSET user:1 name Ada
(integer) 1
127.0.0.1:6379> INCR cnt:visits
(integer) 1
127.0.0.1:6379> INFO discus
# discus
discus_build:dev
...
```

`INFO discus` shows the build, the node ID, the cluster ID, and the members.

## Installing Discus

Each release publishes a container image and a Helm chart:

- The image `ghcr.io/ronaldphilipsen/discus:<version>`.
- The image `ghcr.io/ronaldphilipsen/discus:<version>-pyroscope`, with the CPU profiler.
- The chart `oci://ghcr.io/ronaldphilipsen/charts/discus`, with the same version as the image.

The [releases page](https://github.com/RonaldPhilipsen/discus/releases) lists the versions.

## Documentation

- [Command compatibility](docs/compatibility.md): the supported commands and the differences from
  Redis.
- [Running a cluster](docs/cluster.md): deployment, flags, membership, replication, and failover.
- [Security](docs/security.md): TLS, client authentication, scopes, and the admin port.
- [Telemetry](docs/telemetry.md): metrics, traces, logs, and CPU profiles.
- [Design](docs/design.md): the full design and the reasons for each choice.
- [Stress testing](docs/stress-testing.md): load and fault tests for a release candidate.
- [Benchmark methodology](docs/benchmarks/methodology.md): how the benchmarks compare Discus with
  Valkey. The results are in [docs/benchmarks](docs/benchmarks).

## Code contributions

Read [CONTRIBUTING.md](CONTRIBUTING.md) before you open a pull request. It describes the pull
request titles, the commands to run first, and the release process.

## License

Discus uses the [MIT License](LICENSE). Every dependency on the critical path also has a permissive
license.
