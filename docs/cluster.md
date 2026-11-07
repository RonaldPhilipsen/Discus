# Running a static cluster

This page explains how to run two or more Discus nodes as one cache. Each node accepts any key. A
node that does not own a key forwards the command to the owner and returns the reply. Clients do
not need to know the topology.

Discus has no gossip yet. You list the members by hand with static peers. A later release finds
members by itself.

## How nodes talk

Nodes talk over QUIC on one UDP port per node. The default port is 7379. Every connection uses
TLS 1.3 with mutual authentication (mTLS). Discus never sends node-to-node traffic as plain text,
also not in development.

Each node presents a certificate. The peer accepts the certificate only in two cases together:

- The certificate chains to the CA in `--cluster-ca`.
- The certificate carries a URI SAN equal to `--cluster-san`.

The certificate proves that a node belongs to the cluster. It does not prove which node it is. So
one certificate can serve every node. The certificate is used for both sides of a connection. It
needs both the `serverAuth` and the `clientAuth` extended key usage.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--node-id` | random | This node's ID, 32 hex digits. Required with `--peer`. |
| `--cluster-id` | minted | The cluster ID, 32 hex digits. Every member uses the same value. Required with `--peer`. |
| `--cluster-listen` | `0.0.0.0:7379` | The UDP address for node-to-node traffic. |
| `--peer` | none | One other member as `<node-id>@<ip>:<port>`. Repeat the flag for each member. |
| `--cluster-ca` | none | PEM file with the CA certificate. |
| `--cluster-cert` | none | PEM file with this node's certificate chain. |
| `--cluster-key` | none | PEM file with this node's private key. |
| `--cluster-san` | `spiffe://cluster.local/ns/default/sa/discus` | The URI SAN that every peer must carry. |
| `--forward-read-timeout-ms` | 20 | Time to wait for a forwarded read. |
| `--forward-write-timeout-ms` | 200 | Time to wait for a forwarded write. |
| `--cluster-handshake-timeout-ms` | 2000 | Time to wait for a new connection and its `HELLO`. |
| `--cluster-idle-timeout-ms` | 30000 | A quiet connection closes after this time. |
| `--cluster-keep-alive-ms` | 5000 | Interval of keep-alive packets on a quiet connection. |
| `--cluster-max-frame-bytes` | 135266304 | The largest message this node accepts from a peer. |
| `--read-from-replicas` | off | Read from any owner. Keep it off until replication exists. |

A node with `--peer` and without the three certificate flags does not start. The error names the
missing flags. With static peers, `--weight` must be 1, because every member has weight 1.

A node with no peers and no certificates runs alone with no UDP port, as before.

## Start three nodes

Every node gets the same cluster ID and lists the other two nodes. This example runs three nodes
on one host.

```bash
CLUSTER=$(openssl rand -hex 16)
A=$(openssl rand -hex 16); B=$(openssl rand -hex 16); C=$(openssl rand -hex 16)
TLS="--cluster-ca ca.pem --cluster-cert node.pem --cluster-key node-key.pem"

discus --listen 127.0.0.1:6380 --admin-listen 127.0.0.1:9101 --cluster-listen 127.0.0.1:7381 \
  --node-id "$A" --cluster-id "$CLUSTER" $TLS \
  --peer "$B@127.0.0.1:7382" --peer "$C@127.0.0.1:7383" &
discus --listen 127.0.0.1:6381 --admin-listen 127.0.0.1:9102 --cluster-listen 127.0.0.1:7382 \
  --node-id "$B" --cluster-id "$CLUSTER" $TLS \
  --peer "$A@127.0.0.1:7381" --peer "$C@127.0.0.1:7383" &
discus --listen 127.0.0.1:6382 --admin-listen 127.0.0.1:9103 --cluster-listen 127.0.0.1:7383 \
  --node-id "$C" --cluster-id "$CLUSTER" $TLS \
  --peer "$A@127.0.0.1:7381" --peer "$B@127.0.0.1:7382" &

redis-cli -p 6380 SET greeting hello
redis-cli -p 6382 GET greeting
```

`INFO discus` on each node shows `discus_cluster_addr` and `discus_peers`.

## Make a test CA

Use a real PKI such as cert-manager in production. For a local test, `openssl` makes a CA and one
node certificate:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
  -subj "/CN=discus test CA" -keyout ca-key.pem -out ca.pem
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=discus node" -keyout node-key.pem -out node.csr
printf '%s\n' \
  "subjectAltName=URI:spiffe://cluster.local/ns/default/sa/discus" \
  "extendedKeyUsage=serverAuth,clientAuth" > node.ext
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days 30 \
  -extfile node.ext -out node.pem
```

## How a command finds its owner

The members form one epoch. Every node computes the same owners for a key from the epoch.

- A write goes to the primary of the key.
- A read also goes to the primary for now. Secondaries hold no data until replication exists. With
  `--read-from-replicas`, a node that owns the key serves the read itself. Otherwise it asks the
  owners in turn and moves to the next owner after the read timeout.
- A forwarded command carries the epoch of the sender. A receiver with an older epoch adopts the
  newer one before it acts.
- A command hops at most twice. After two hops, a node that owned the key in its previous epoch
  serves the command. Otherwise it returns `-TRYAGAIN the cluster is changing, try again later`.
- If the owner of a write does not answer in time, the command returns the same `-TRYAGAIN`.

Replies keep the order of the pipeline. Commands on the same key run in the order that the client
sent them. Commands on different keys run at the same time.

## Multi-key and keyspace commands

| Command | Behavior in a cluster |
| --- | --- |
| `MGET` | Groups the keys by owner, asks each owner once, and returns the values in the order of the request. |
| `EXISTS` | Groups the keys by owner and adds the counts. |
| `MSET`, `DEL` | Group the keys by owner. This is not atomic. If one part fails, the command returns `-TRYAGAIN` and the other parts stay applied. A retry of the whole command is safe. |
| `SCAN`, `RANDOMKEY` | Look at the keys of the node that the client is connected to only. |
| `DBSIZE` | Adds the key counts of every member that answers. |
| `FLUSHDB`, `FLUSHALL` | Clear every member that answers and return `OK`. |

## Limits of this release

- Membership is static. A node that stops stays in the member list. Commands for its keys return
  `-TRYAGAIN` until it comes back.
- There is no replication. A node that stops loses its keys.
- 0-RTT is off. Every new connection takes a full handshake.
