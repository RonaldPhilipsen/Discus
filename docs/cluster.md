# Running a cluster

This page explains how to run two or more Discus nodes as one cache. Each node accepts any key. A
node that does not own a key forwards the command to the owner and returns the reply. Clients do
not need to know the topology.

The nodes find each other by themselves. They use SWIM gossip (a membership protocol in which
each node probes a few random peers) over QUIC. You give each node a few seed addresses, or the
name of a Kubernetes headless Service. A cluster lease makes sure that only one node forms a new
cluster.

## How nodes talk

Nodes talk over QUIC on UDP. With gossip membership, each node uses two ports. Gossip uses the
cluster port, 7379 by default. Node-to-node requests use the RPC port, 7380 by default. With
`--membership static`, the RPC traffic stays on `--cluster-listen`, and the node uses one port.
Every connection uses TLS 1.3 with mutual authentication (mTLS). Discus never sends node-to-node
traffic as plain text, also not in development.

Each node presents a certificate. The peer accepts the certificate only in two cases together:

- The certificate chains to the CA in `--cluster-ca`.
- The certificate carries a URI SAN equal to `--cluster-san`. If `--cluster-san` is empty, the
  peer does not do this test.

The certificate proves that a node belongs to the cluster. It does not prove which node it is. So
one certificate can serve every node. The certificate is used for both sides of a connection. It
needs both the `serverAuth` and the `clientAuth` extended key usage.

## How a node joins

A node starts as `JOINING` with a new random node ID. A node ID lives for one process, so a
restarted pod is a new node.

1. The node looks for a `NORMAL` peer (a peer that serves in a cluster) among its seeds. The
   seeds are the `--peer` addresses and the addresses of `--discovery-dns`.
2. If it finds one, it asks to join. The peer sends the current epoch (the numbered member list)
   and the highest HLC timestamp in the cluster. The node raises its clock to that timestamp and
   becomes `NORMAL`.
3. If it finds none within the bootstrap deadline (default 10 s), it tries to take the cluster
   lease. The winner forms a new cluster alone. A loser reads the address of the holder, adds it
   as a seed, and keeps looking.
4. If a `NORMAL` peer answers the connection but not the join, the node does not form a cluster
   and does not serve writes. It keeps trying.

A joining node returns `-TRYAGAIN the cluster is changing, try again later` for data commands
until it is `NORMAL`.

## Failure detection

Each node probes one member per gossip period (default 1 s). A member that misses a probe gets a
second chance through three other members. A phi-accrual detector turns the silence and the
failures of a member into a score. A score of 8 makes the member suspect. A suspect member that
does not answer within the suspicion timeout (3 s to 10 s) is `DEAD`. Every node that also
suspects the member shortens the timeout.

A forward timeout, a reset connection, or a failed handshake toward a member weighs more than a
missed probe. So a node that the clients reach through several entry nodes becomes `DEAD` faster.

A node whose data runtime lags does not trust its own timers. It measures the lag and turns it into
a multiplier from 1 to 9. The probe timeouts and the suspicion timeout grow by this multiplier. The
gossip period does not grow, so the peers keep hearing from the node at the normal rate. If a
longer probe timeout is still running when the next probe is due, the next probe starts anyway and
goes to another member. Each probe keeps its own result. While the multiplier is 1.5 or higher, the
node ignores forward failures as evidence.

A `DEAD` member leaves the epoch. One node builds the next epoch: the live `NORMAL` member with the
lowest node ID. It waits a random 0 to 50 ms, then creates the next epoch. The other nodes wait
longer: three gossip periods, plus one period for each doubling of the cluster size. They build
only if no new epoch arrived by then. Every node adopts the highest epoch.

## Partitions and merges

During a network partition, both sides keep serving and mark the other side `DEAD`. When the
partition heals, the higher epoch wins. A node that the winning epoch does not list joins again
with a higher incarnation (a counter that only the node itself raises).

Two clusters that formed independently also merge. The cluster that the lease names wins. Without
lease access, the larger cluster wins, then the higher cluster ID. The nodes of the losing cluster
discard their data and join the winner.

## Joins from a crash loop

A member admits a join from one IP address at most once per backoff. The backoff starts at 1 s
and doubles to 60 s for repeated joins from that address. It resets after 5 minutes without a
join. A container that restarts in a loop keeps its pod IP, so this limits the epoch churn that it
causes. Each member keeps its own count.

## The cluster lease

| Provider | Use |
| --- | --- |
| `k8s` | The Kubernetes `Lease` `discus-cluster` in the pod namespace. With the `k8s` build feature, it is the default. |
| `none` | No lease. A node that finds no peer forms a cluster alone after the deadline. Two such nodes can form two clusters. For single-node development only. The node logs a warning at every start. |

With `k8s`, every `NORMAL` member renews the lease every 5 s for 15 s. So a live cluster always
holds it. The holder writes its node ID to `holderIdentity`, and its address and cluster ID to the
annotations `discus.io/address` and `discus.io/cluster-id`.

The lease matters only for the formation of a cluster. If the API server is down, a running cluster
keeps serving. A new node that reaches neither a `NORMAL` peer nor the API server stays unready.

After a full cold restart, the old lease expires after 15 s. Then the first node to take it forms
a new cluster.

The service account of the pods needs these permissions in its namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: discus-lease
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: discus-lease
subjects:
  - kind: ServiceAccount
    name: discus
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: discus-lease
```

## Probes

| Path | Answers 200 when |
| --- | --- |
| `/healthz` | The process runs. |
| `/startupz` | The node was `NORMAL` at least once, the store is ready, and the counter tier is settled. The store is ready once the node received its handoff (see [Handoff](#handoff)). The counter tier is settled once the node holds what each counter group needs from it (see [Strong counters](#strong-counters)). The node stops this wait after `--counter-settle-timeout-ms`. |
| `/readyz` | The node is `NORMAL` in its current epoch and does not drain. |

A join does not make the serving nodes unready.

## Drain

SIGTERM and `POST /admin/drain` start a drain. The drain route needs admin credentials, like every
route under `/admin/`.

1. The node turns unready and marks itself `LEAVING`. The next epoch keeps it out of placement,
   so its keys move to other members.
2. The node stops accepting connections. It keeps serving the open connections, sends the
   entries of its keys to their new owners, and empties its replication queues. A leader-follower
   leader stops at once, and its follower takes over. The Raft leaders move the voter slots of the
   node to other members. The node waits for all of this, at most `--shutdown-timeout-secs`
   (default 30 s). Keys that did not move in time refill as misses.
3. The node tells three members that it is gone, leaves gossip, and stops. The other members
   remove it from the epoch at once, without the suspicion timeout.

## Deploy on Kubernetes

Run Discus as a plain Deployment. Each pod is a new node with a new node ID. A pod needs no
volume and no stable name.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: discus-peers
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: discus
  ports:
    - name: cluster
      port: 7379
      protocol: UDP
    - name: rpc
      port: 7380
      protocol: UDP
---
apiVersion: v1
kind: Service
metadata:
  name: discus
spec:
  selector:
    app: discus
  ports:
    - name: resp
      port: 6379
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: discus
spec:
  replicas: 3
  minReadySeconds: 10
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: discus
  template:
    metadata:
      labels:
        app: discus
    spec:
      serviceAccountName: discus
      terminationGracePeriodSeconds: 45
      containers:
        - name: discus
          image: discus:latest
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
          args:
            - --discovery-dns=discus-peers
            - --cluster-advertise=$(POD_IP):7379
            - --cluster-ca=/tls/ca.crt
            - --cluster-cert=/tls/tls.crt
            - --cluster-key=/tls/tls.key
            - --shutdown-timeout-secs=30
          startupProbe:
            httpGet:
              path: /startupz
              port: 9100
            failureThreshold: 60
            periodSeconds: 1
          readinessProbe:
            httpGet:
              path: /readyz
              port: 9100
          livenessProbe:
            httpGet:
              path: /healthz
              port: 9100
```

Follow these rules:

- The headless Service sets `publishNotReadyAddresses: true`. A joining node is not ready, and
  the other nodes still find it.
- `maxSurge: 1` with `maxUnavailable: 0` starts the new pod before the old pod drains.
  `minReadySeconds` makes one replacement finish before the next one begins.
- `terminationGracePeriodSeconds` is longer than `--shutdown-timeout-secs`, so the drain ends
  before Kubernetes kills the container.
- The service account `discus` needs the Role of the cluster lease above.
- Mount the node certificate at `/tls`, for example from a cert-manager `Certificate` with the URI
  SAN `spiffe://cluster.local/ns/<namespace>/sa/discus`. Set `--cluster-san` to the same value.

### Install with Helm

The chart in `charts/discus` creates the resources above and a PodDisruptionBudget. Each release
publishes the chart with the same version as the image.

Before you install the chart, create a Secret with the keys `ca.crt`, `tls.crt`, and `tls.key`.
Use a CA that serves only this release, because every certificate from the CA is a member of the
cluster. Then install the chart:

```bash
helm install <release> oci://ghcr.io/ronaldphilipsen/charts/discus \
  --version <version> \
  --namespace <namespace> \
  --set cluster.tls.secretName=<secret>
```

The chart does not require a URI SAN by default. To require a SPIFFE identity, set
`cluster.san.enabled=true`. The certificate must then carry the URI SAN
`spiffe://cluster.local/ns/<namespace>/sa/<account>`. The value `<account>` is the service account
of the release. By default, it is the full name of the release, for example `discus` or
`blue-discus`. To require a different URI, set `cluster.san.value`.

You can install more than one release in a namespace. The chart gives each release its own lease
names, so each release is a separate cluster. Use a different CA for each release, or require a
URI SAN that is different for each release.

### Memory budget

If you do not set `--max-memory`, the node reads the memory limit of the container from the
cgroup. The budget is then 75% of that limit. Connections, replication, and the allocator use the
remaining 25%. If the container has no memory limit, the budget is 1 GiB. The node logs its budget
at startup in the field `max_memory`.

### CPU limits

The node starts one Tokio worker thread for each CPU that the container can use. The node reads
the CPU limit of the container from the cgroup (the Linux resource control group).

| `resources.limits.cpu` | Worker threads |
| --- | --- |
| `4` | 4 |
| `3500m` | 3 |
| `500m` | 1 |
| Not set | One for each host core |

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--membership` | `gossip` | `gossip` finds members by itself. `static` uses the fixed `--peer` list with no failure detection. |
| `--node-id` | random | This node's ID, 32 hex digits. Required with `--membership static`. |
| `--cluster-id` | minted | The cluster ID, 32 hex digits. With `static`, every member uses the same value, and the flag is required. With `gossip`, it is the ID that this node gives a cluster it forms. |
| `--cluster-listen` | `0.0.0.0:7379` | The UDP address for node-to-node traffic. |
| `--cluster-advertise` | see below | The gossip address that other members use. |
| `--rpc-listen` | `0.0.0.0:7380` | The UDP address for node-to-node requests. With `--membership static`, requests use `--cluster-listen` instead. |
| `--rpc-advertise` | see below | The RPC address that other members use. Default: the IP of the advertised cluster address with the bound `--rpc-listen` port. |
| `--redirect-listen` | off | The TCP address of the redirect listener. It is a second client listener. |
| `--client-advertise` | see below | The client address that other members and cluster clients use. Default: the IP of the advertised cluster address with the bound `--redirect-listen` port. Without a redirect listener, it uses the bound `--listen` port. |
| `--client-hostname` | none | A host name template for client addresses. `{ip}` becomes the advertised IP with dashes, for example `10-1-2-3`. An empty value means no host name. |
| `--cluster-topology` | `auto` | `auto` answers the topology commands when the node has a redirect listener. `on` always answers. `off` never answers. |
| `--data-nice` | `5` | Raise the nice value of the data runtime threads by this much over the process value, from 0 to 19. The result is capped at 19. The control thread keeps the process value, so it gets CPU first under a quota. 0 changes nothing. Linux only. |
| `--peer` | none | A seed as `<ip>:<port>`, or `<node-id>@<ip>:<port>`. Repeat the flag. With `static`, list every other member with its node ID. Add `/<client-ip>:<client-port>` to give the member a client address. |
| `--discovery-dns` | none | A headless Service name, as `<host>` or `<host>:<port>`. The port defaults to the `--cluster-listen` port. |
| `--lease-provider` | `k8s` | `k8s` or `none`. Without the `k8s` build feature, the default is `none`. |
| `--lease-name` | `discus-cluster` | The name of the cluster lease. |
| `--lease-namespace` | pod namespace | The namespace of the cluster lease. |
| `--lease-duration-ms` | 15000 | The lease duration. |
| `--lease-renew-ms` | 5000 | The renewal interval. |
| `--bootstrap-deadline-ms` | 10000 | Time to look for a `NORMAL` peer before the node tries the lease. |
| `--gossip-interval-ms` | 1000 | The gossip period: one probe per period. |
| `--gossip-probe-timeout-ms` | 500 | Time to wait for a direct probe. |
| `--gossip-indirect-probes` | 3 | Members that probe a target that missed a direct probe. |
| `--phi-suspect` | 8 | The detector score that makes a member suspect. One failed probe adds this much. |
| `--phi-dead` | 16 | The score that makes a suspect member `DEAD` after the minimum suspicion time. |
| `--data-path-weight` | 10 | The score that one forward timeout, reset, or failed handshake adds. |
| `--suspicion-min-ms` | 3000 | The suspicion timeout once enough nodes confirm a suspicion. No suspect member becomes `DEAD` sooner. |
| `--suspicion-max-ms` | 10000 | The suspicion timeout without confirmations. |
| `--cluster-ca` | none | PEM file with the CA certificate. |
| `--cluster-cert` | none | PEM file with this node's certificate chain. |
| `--cluster-key` | none | PEM file with this node's private key. |
| `--cluster-san` | `spiffe://cluster.local/ns/default/sa/discus` | The URI SAN that every peer must carry. If the value is empty, any certificate from the CA is accepted. |
| `--forward-read-timeout-ms` | 500 | Time to wait for a forwarded read. |
| `--forward-write-timeout-ms` | 2000 | Time to wait for a forwarded write. |
| `--cluster-handshake-timeout-ms` | 2000 | Time to wait for a new connection and its `HELLO`. |
| `--cluster-idle-timeout-ms` | 30000 | A quiet connection closes after this time. |
| `--cluster-keep-alive-ms` | 5000 | Interval of keep-alive packets on a quiet connection. |
| `--cluster-max-frame-bytes` | 135266304 | The largest message this node accepts from a peer. |
| `--read-from-replicas` | `true` | Read from any owner. With `false`, every read goes to the primary. |
| `--replication-factor` | 3 | The owners of each key: the primary and its secondaries. |
| `--replication-queue-keys` | 65536 | Keys that wait in the replication queue of one peer. Past it, the oldest keys go to hints. |
| `--replication-batch-keys` | 512 | Keys in one replication batch. |
| `--replication-timeout-ms` | 1000 | Time for a peer to acknowledge a replication batch. |
| `--replication-retry-ms` | 1000 | Time between probes of a peer that missed a batch. |
| `--hint-max-entries` | 262144 | Hints for one peer, at most. |
| `--hint-max-bytes` | 67108864 | Bytes of hints for one peer, at most. |
| `--hint-memory-percent` | 5 | The share of `--max-memory` for all hints. |
| `--handoff-rate-bytes` | 33554432 | Bytes per second that one handoff scan sends, at most. |
| `--handoff-timeout-ms` | 300000 | Time after which a node gives up on an incoming handoff. |
| `--tombstone-ttl-ms` | 3600000 | How long a delete marker lives. |
| `--tombstone-reserve-percent` | 5 | The share of `--max-memory` for tombstones. |
| `--write-set-keys` | 4096 | Keys in the write set of one connection, at most. |
| `--write-set-bytes` | 1048576 | Bytes in the write set of one connection, at most. |
| `--write-set-memory-percent` | 1 | The share of `--max-memory` for all write sets. |
| `--write-set-ttl-ms` | 5000 | Time that a written key stays in the write set. |
| `--hold-budget-ms` | 3000 | Time that a write to a failed primary waits for a new primary. |
| `--counter-prefix` | `cnt:` | Keys with this prefix are counter keys. The counter groups serve them. |
| `--counter-groups` | 8 | The number of counter groups. A change is an offline procedure. |
| `--counter-memory-percent` | 5 | The share of `--max-memory` for counter state. |
| `--counter-timeout-ms` | 1000 | Time that a counter command waits for a leader, a quorum, or a commit. |
| `--counter-settle-timeout-ms` | 45000 | The longest time that `/startupz` waits for the counter tier after the node turns `NORMAL`. Keep it below the startup probe budget. |
| `--counter-lease-prefix` | `discus-counter` | The name prefix of the counter group leases. Every member of a cluster must use the same value. |
| `--counter-lease-duration-ms` | 5000 | The duration of a counter group lease. |
| `--counter-lease-renew-ms` | 1000 | The renewal interval of a counter group lease. |
| `--counter-lease-margin-ms` | 1000 | A leader stops this long before its lease ends. |
| `--counter-follower-timeout-ms` | 50 | Time that a leader-follower write waits for the follower. |
| `--counter-dwell-ms` | 30000 | Time with three `NORMAL` members before a group switches to Raft. |
| `--raft-heartbeat-ms` | 100 | The Raft heartbeat interval. |
| `--raft-election-timeout-ms` | 1000 | The shortest Raft election timeout. The longest is twice this value. |
| `--raft-snapshot-entries` | 10000 | Log entries between two Raft snapshots. |
| `--raft-catchup-entries` | 100 | A learner this close to the log end of the leader becomes a voter. |
| `--raft-voter-timeout-ms` | 10000 | The leader removes a voter that does not answer for this long. |

A node with `--peer` and without the three certificate flags does not start. The error names the
missing flags. With `static`, `--weight` must be 1, because every member has weight 1.

A node with no peers and no certificates runs alone with no UDP port, as before.

The advertised address is the first of these that applies:

1. `--cluster-advertise`.
2. `--cluster-listen` with a specific IP, not `0.0.0.0` or `::`.
3. The local address of the network interface toward the first seed.
4. `127.0.0.1` with the listen port.

In Kubernetes, set `--cluster-advertise $(POD_IP):7379` from the downward API. The RPC address
follows from it, so `--rpc-advertise` is rarely needed.

If a NetworkPolicy or a firewall limits pod traffic, allow both UDP ports between the pods. This is
the ingress rule of the policy:

```yaml
ingress:
  - from:
      - podSelector:
          matchLabels:
            app: discus
    ports:
      - port: 7379
        protocol: UDP
      - port: 7380
        protocol: UDP
```

## Start three nodes

Outside Kubernetes, give every node the other nodes as seeds and use the lease provider `none`.
With `none`, start one node first and let it form the cluster. Start the others after it is
ready. This example runs three nodes on one host. Each node needs its own `--rpc-listen`, so the
nodes do not collide on port 7380.

```bash
TLS="--cluster-ca ca.pem --cluster-cert node.pem --cluster-key node-key.pem"
GOSSIP="--lease-provider none $TLS"

discus --listen 127.0.0.1:6380 --admin-listen 127.0.0.1:9101 --cluster-listen 127.0.0.1:7381 \
  --rpc-listen 127.0.0.1:7391 \
  $GOSSIP &
sleep 12
discus --listen 127.0.0.1:6381 --admin-listen 127.0.0.1:9102 --cluster-listen 127.0.0.1:7382 \
  --rpc-listen 127.0.0.1:7392 \
  $GOSSIP --peer 127.0.0.1:7381 &
discus --listen 127.0.0.1:6382 --admin-listen 127.0.0.1:9103 --cluster-listen 127.0.0.1:7383 \
  --rpc-listen 127.0.0.1:7393 \
  $GOSSIP --peer 127.0.0.1:7381 --peer 127.0.0.1:7382 &

redis-cli -p 6380 SET greeting hello
redis-cli -p 6382 GET greeting
```

`INFO discus` on each node shows `discus_membership`, `discus_member_state`,
`discus_incarnation`, `discus_epoch`, and `discus_members`. `discus_cluster_addr` is the bound
`--cluster-listen` address, which serves gossip, and `discus_rpc_addr` is the bound `--rpc-listen`
address. With static membership, both show the one cluster address.

## Static membership

`--membership static` keeps a fixed member list. Every node gets the same cluster ID and lists
every other node as `<node-id>@<ip>:<port>`. Every member is `NORMAL` in epoch 1. There is no
gossip and no failure detection, so a stopped node stays in the list. Use it for fixed test
setups.

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
- A node that owns the key serves a read itself. Otherwise it asks the owners in turn, starting at
  a different owner for each command of the connection. It moves to the next owner after the read
  timeout.
- A forwarded command carries the epoch of the sender. A receiver with an older epoch adopts the
  newer one before it acts.
- A command hops at most twice. After two hops, a node that owned the key in its previous epoch
  serves the command. Otherwise it returns `-TRYAGAIN the cluster is changing, try again later`.
- If the primary of a write does not answer in time, the entry node holds the write for a new
  primary (see [Failover](#failover)).

Replies keep the order of the pipeline. Commands on the same key run in the order that the client
sent them. Commands on different keys run at the same time.

## Replication

Each key has up to three owners (`--replication-factor`, default 3). The first owner is the
primary, and the others are secondaries. A cluster of one node has no secondaries, and a cluster
of two nodes has one.

1. A write goes to the primary. The primary applies the write, gives it an HLC timestamp (a
   version from a hybrid logical clock), and replies at once.
2. Then the primary puts the key in one queue per secondary. A queue holds each key once. As the
   queue drains, the primary sends the latest value of the key.
3. A secondary ignores an entry that is not newer than the one it holds. So the order of the
   batches does not matter, and a repeated batch changes nothing.

Replication uses a second QUIC connection per peer. A slow replication stream therefore does not
hold up forwarded commands.

A delete writes a tombstone (a delete marker) with a TTL of `--tombstone-ttl-ms` (default one
hour). A tombstone replicates like a value, so an older copy of the key cannot come back. A
tombstone reads as a miss. `SCAN`, `DBSIZE`, `RANDOMKEY`, `TYPE`, and `EXISTS` ignore it.

A TTL replicates as an absolute deadline in Unix milliseconds. The clocks of the nodes need to
agree within about 100 ms, so run NTP.

When a new epoch changes the owners of a key, the queues stop carrying the key to nodes that no
longer own it.

## Reads

A node that owns the key serves a read itself. Otherwise it asks the owners in turn, starting at
a different owner for each command of the connection. Reads of a hot key therefore spread over
every owner. A read moves to the next owner after `--forward-read-timeout-ms`.

A secondary that has no entry for a key asks the primary, then the other owners, and stores what
it finds. This is read repair. A secondary that holds a tombstone does not ask.

Reads can be stale, because replication is asynchronous. Each connection gets read-your-writes
on a best-effort basis through its write set:

- After a write, the entry node records the key and the version that the primary acknowledged.
- A read of a key in the write set goes to the primary. If the entry node owns the key and
  already holds the version, it serves the read itself.
- A key leaves the write set after 5 s (`--write-set-ttl-ms`), by LRU, or after a read that shows
  that every secondary holds the version.
- The bounds are 4096 keys or 1 MiB per connection, and 1% of `--max-memory` for the whole node.
  A write that does not fit is not recorded. The write set never takes memory from the cache.
- The write set ends with the connection. A new connection, also to the same node, can read a
  stale value.
- Counter keys never enter a write set. Every read of a counter key goes to its group leader.

## Hinted handoff

If the primary cannot deliver a write to a secondary, it records a hint. A hint holds the latest
entry of one key for one secondary. These events record hints:

- A replication batch fails or times out (`--replication-timeout-ms`). The secondary is then down
  for the primary, and later writes go to hints at once.
- The queue of a secondary overflows (`--replication-queue-keys`). The oldest keys go to hints,
  and the secondary is lagging.

The primary probes a down secondary every `--replication-retry-ms`. The primary replays the hints
in two cases:

- A probe succeeds, and the epoch lists the secondary as `NORMAL`.
- The queue of a lagging secondary drains below a quarter of its size.

A replay sends the hints in HLC order, one acknowledged batch at a time.

Hints have these bounds:

- `--hint-max-entries` and `--hint-max-bytes` per secondary.
- 5% of `--max-memory` for all secondaries together (`--hint-memory-percent`).
- An age of the tombstone TTL.

A bound evicts the oldest hints. An evicted hint is lost. The first replay then names the part of
the key space of each lost hint. The secondary drops its copies of those keys, and they refill
from the primary as misses.

The primary drops the hints of a secondary that becomes `DEAD` or no longer owns the key. Hints
live only in the memory of the primary.

## Failover

A node loss changes no data placement until the failure detector declares the node `DEAD`. Then
the next epoch removes the node, and the next owner of each of its keys becomes the primary.
Reads go on during detection, because the other owners answer them.

After a promotion, the new primary compares a digest of the moved keys with each other owner once.
It pulls the entries that are newer there. This covers the writes that the old primary replicated
to only some of the secondaries.

A write to a primary that fails or times out (`--forward-write-timeout-ms`) is held:

1. The transport reports the peer to the failure detector.
2. The entry node waits for an epoch with a new primary, and tries again at least every 200 ms.
3. After `--hold-budget-ms` (default 3000), the command returns `-TRYAGAIN`.

A held command keeps its place in the pipeline. Later commands on the same key wait for it.

A command that never left the entry node is always sent again. A command that was sent but got no
reply can have run already. The entry node sends such a command again for these commands only,
because they are safe to apply twice:

`SET` without `NX` or `XX`, `DEL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`,
`MSET`, `HSET`, `HDEL`, `SADD`, `SREM`, `ZADD` without `INCR`, and `ZREM`.

Every other command returns `-TRYAGAIN` at once, because it can have run already.

## Handoff

A node that gains owner slots receives the entries of those keys. A join, a death, and a drain all
start a handoff, so a new owner does not start empty.

- Each node that held the keys scans its store and sends the entries that the new owner gained.
  For each key, the first old owner that is still a member sends the full entry. The other old
  owners send only the key and its version, and the new owner asks for the keys it lacks.
- A node runs one scan at a time. A scan covers every ownership change since the oldest change
  that waits. Changes that arrive during a scan wait for the next scan, so a burst of epochs costs
  one or two scans.
- A scan sends at most `--handoff-rate-bytes` per second (default 32 MiB). Entries that change
  after the scan starts replicate as usual.
- While its handoff is incomplete, a new owner that misses a key asks the old owners that no
  longer own it. This applies to reads and writes, so an `INCR` of a moved key starts from the old
  value. The fallback ends in three cases: every old owner finished or gave up, the old owners
  leave, or `--handoff-timeout-ms` (default 5 minutes) ends. After that, a miss is a miss.

`/startupz` passes only after the node received its handoff. A rolling restart therefore waits for
each new pod to fill before it replaces the next one.

## Strong counters

Keys with the prefix `--counter-prefix` (default `cnt:`) are counter keys. Every command on a
counter key is linearizable. Each command acts at one moment between its request and its reply,
and every client sees the same order. Counter keys never take the asynchronous replication path.
The prefix decides the path before the value is read. So a plain `SET` of a counter key cannot
overwrite a counter with a stale value.

### Groups and leaders

The counter keys split into `--counter-groups` groups (default 8) by a hash of the key. Each group
has one leader, and the leader runs every command of the group, also the reads. The node that a
client is connected to forwards the command to the leader. Nodes learn the leader from the leader
hints in gossip, from the group lease, and from the `RAFT_LEADER` question to the other members.

A group runs in one of two modes:

- Leader-follower mode with fewer than three `NORMAL` members.
- Raft mode with three or more `NORMAL` members. A Raft group of two voters cannot survive the
  loss of one, so a small cluster uses leader-follower mode.

The target voters of a group are the top three `NORMAL` members by HRW (rendezvous hashing) with
the group as the key.

### Leader-follower mode

The Kubernetes `Lease` `<prefix>-<g>` names the leader of group `g`. The value `<prefix>` is
`--counter-lease-prefix` (default `discus-counter`). The duration of the lease is
`--counter-lease-duration-ms` (default 5 s), and the leader renews it every
`--counter-lease-renew-ms` (default 1 s).

- The leader serves only while its lease is valid by its own clock, minus
  `--counter-lease-margin-ms` (default 1 s). Otherwise it returns `-TRYAGAIN`. Two leaders never
  serve one group at the same time.
- The leader sends each write to its follower and waits up to `--counter-follower-timeout-ms`
  (default 50 ms) for the acknowledgement. After a timeout, it replies anyway and sends a fresh
  snapshot to the follower later. A new follower starts with a snapshot.
- Leadership is sticky. When the leader goes silent for one lease duration, the follower tries to
  take the lease first. The other members wait one more lease duration.
- A failover takes about one lease duration. It loses the writes that a lagging follower missed.
  With one node, a node loss loses the counters. In this mode the counters depend on the API
  server.

With `--lease-provider none` and with static membership, no lease fences the leader. The epoch
names the leader: the previous leader, else the previous follower, else the top member by HRW.
This mode cannot fence a leader and is for development only.

### Raft mode

The leader writes each command to a replicated log. It replies once two of three voters hold the
entry. A read asks a quorum first (ReadIndex), so a read never sees an old value. The log, the
snapshots, and the votes live in memory.

- A restarted pod has a new node ID, so it enters a group only as a learner. A node that discards
  its counters also enters as a new learner.
- The leader adds each missing target as a learner. It waits until the learner is within
  `--raft-catchup-entries` (default 100) entries of the log end. When all three targets are ready,
  one change replaces the voters with the targets. A node starts at most one change every 200 ms.
- The leader never makes a group of two voters, because one lost replica stops such a group.
- A voter is dead when the epoch marks it `DEAD`, or when it does not answer for
  `--raft-voter-timeout-ms` (default 10 s), or when a live node refuses its replica. The leader
  replaces a dead voter when the replacement
  is ready, and it removes a dead learner at once.
- A group that lost a majority of its voters starts a new instance on the leader or on the top
  live voter. This also frees a joint change that can no longer commit.
- A minority side of a partition returns `-TRYAGAIN`.
- A snapshot follows every `--raft-snapshot-entries` (default 10000) entries. A learner that is
  far behind receives the snapshot.

### Mode switches

- Up: after three `NORMAL` members for `--counter-dwell-ms` (default 30 s), the lease holder of
  each group creates a Raft group with itself as the only voter and its state as the start. It
  lets the lease expire. The other members join as learners.
- Down: below three members, a group switches at once. The Raft leader takes the lease. If the
  leader is gone and the `NORMAL` voters are a majority, they elect a new leader first, and that
  leader switches. Only when they are no majority does a surviving voter take the lease. Other
  members wait one lease duration first. If the quorum was already lost, the last committed
  writes can be lost.

### Quorum loss

- When two of three voters are `DEAD`, the surviving voter makes itself the only voter of a new
  Raft group with its state.
- When every voter is `DEAD`, the top target creates the group again, empty. Its counters are
  lost.
- `POST /admin/raft/force-reconfigure` acts at once, before the failure detector declares the
  voters `DEAD`. On every group in which the node is a voter, the node becomes the only voter
  with its state. Add `?group=<g>` for one group. The reply lists the groups that changed. The
  route needs admin credentials, like `/admin/drain`.

`GET /admin/counters` shows the role of the node in each group, the leader, the voters, and the
learners.

A false `DEAD` during a partition lets both sides rebuild a group. On heal, the losing side
discards its counters, and the writes that it accepted are lost.

### Startup and drain

After `--counter-settle-timeout-ms` (default 45 s), the node passes `/startupz` without the counter
tier and logs a warning. Its promotion continues. A restart gives a pod a new node ID and starts the
promotion again, so a wait without an end can stop a rollout.

`/startupz` looks at every group. A group is settled for the node in four cases:

- The node is a voter of a Raft group that targets it.
- The node is the synced follower.
- The node is the leader.
- The group does not need the node.

A later change of the targets does not make a serving node unready.

A drain hands the counter roles over. See [Drain](#drain).

### Change the number of groups

A change of `--counter-groups` moves keys to other groups, so it is an offline procedure.
Linearizability does not hold across it, and Discus does not automate it:

1. Stop the clients that write counter keys.
2. Read every counter key with `GET` and `PTTL`, and save the values.
3. Restart every node with the new `--counter-groups`. A full cold restart clears the counters.
4. Write the saved values back with `SET` and `PX`.
5. Start the clients again.

### Growth from one node to three

- One node holds the cluster lease and every group lease. The effective replication factor is 1.
- A second node gains every key. It receives a handoff of the whole store and a snapshot of every
  group as follower. The effective replication factor is 2.
- A third node gains about a third of the owner slots. After the dwell time, every group switches
  to Raft with three voters, and the replication factor is 3.

Shrinking runs the same steps in reverse.

## Rejoin after a partition

A node that the winning epoch does not list joins again. It discards its counters. The node
measures the time since it last heard from the winning members:

- Shorter than the tombstone TTL: it keeps its store. It clears only the parts of the store that
  dropped tombstones early.
- The tombstone TTL or longer: it clears the store, because the deletes that it missed can be gone
  everywhere.

## Memory

`--max-memory` is one budget for entries, tombstones, hints, write sets, and counter state. At the
limit, the node evicts in this order:

1. Normal entries, by sampled LRU.
2. The oldest hints.
3. The oldest tombstones. The node marks their part of the store for a refill, and a rejoin
   clears that part.

Tombstones also have their own share, `--tombstone-reserve-percent` (default 5). Past it, the oldest
tombstones go first. Write sets are never evicted for memory. Each key that enters a write set must
fit its bounds.

Counter state is never evicted. It has its own share, `--counter-memory-percent` (default 5). Each
counter key costs its key, its value, and 64 bytes on every node that holds the group. When the
counter bytes of the leader reach the share, a write that can grow the state returns
`-OOM command not allowed when the counter reserve is full`. `DEL`, `GETDEL`, `PERSIST`, the expiry
commands, and `FLUSHALL` still work.

## Multi-key and keyspace commands

| Command | Behavior in a cluster |
| --- | --- |
| `MGET` | Groups the keys by owner, asks each owner once, and returns the values in the order of the request. |
| `EXISTS` | Groups the keys by owner and adds the counts. |
| `MSET`, `DEL` | Group the keys by owner. This is not atomic. If one part fails, the command returns `-TRYAGAIN` and the other parts stay applied. A retry of the whole command is safe. |
| `SCAN`, `RANDOMKEY` | Look at the keys of the node that the client is connected to only. Counter keys are not listed. |
| `DBSIZE` | Adds the key counts of every member that answers and divides the sum by the replication factor. The result is an estimate. Counter keys do not count. |
| `FLUSHDB`, `FLUSHALL` | Clear every member that answers. Each member also drops its replication queues, its hints, and its handoffs. A write that is on its way during the flush can survive it. Then each counter group clears its counters. If a group has no leader or no quorum, the reply is `-TRYAGAIN FLUSHALL cleared the cache and <n> of <G> counter groups`. A retry is safe. |

Multi-key commands on counter keys split by group. The keys of one group run as one command on
its leader, so they are atomic. Keys of several groups fan out like normal keys. A command that
mixes counter keys and other keys returns
`-ERR counter keys and other keys cannot be mixed in one command`.

## Limits of this release

- Replication is asynchronous. A node loss can lose the last writes, including deletes, that the
  primary did not replicate yet.
- Multi-key reads (`MGET`, `EXISTS`) do not use read repair or the previous-epoch fallback. A
  missing key reads as a miss.
- 0-RTT is off. Every new connection takes a full handshake.
