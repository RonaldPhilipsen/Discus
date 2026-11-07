# Running a cluster

This page explains how to run two or more Discus nodes as one cache. Each node accepts any key. A
node that does not own a key forwards the command to the owner and returns the reply. Clients do
not need to know the topology.

The nodes find each other by themselves. They use SWIM gossip (a membership protocol in which
each node probes a few random peers) over QUIC. You give each node a few seed addresses, or the
name of a Kubernetes headless Service. A cluster lease makes sure that only one node forms a new
cluster.

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

A `DEAD` member leaves the epoch. A node that sees the death waits a random 0 to 50 ms, then
creates the next epoch. The other nodes adopt the highest epoch.

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
| `/startupz` | The node was `NORMAL` at least once, the store is ready, and the counter tier is settled. Counters do not exist yet, so the last condition always holds. |
| `/readyz` | The node is `NORMAL` in its current epoch and does not drain. |

A join does not make the serving nodes unready.

## Drain

SIGTERM and `POST /admin/drain` start a drain. The drain route needs admin credentials, like every
route under `/admin/`.

1. The node turns unready and marks itself `LEAVING`. The next epoch keeps it out of placement,
   so its keys move to other members.
2. The node stops accepting connections. It keeps serving the open connections until they close
   or `--shutdown-timeout-secs` (default 30 s) ends.
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

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--membership` | `gossip` | `gossip` finds members by itself. `static` uses the fixed `--peer` list with no failure detection. |
| `--node-id` | random | This node's ID, 32 hex digits. Required with `--membership static`. |
| `--cluster-id` | minted | The cluster ID, 32 hex digits. With `static`, every member uses the same value, and the flag is required. With `gossip`, it is the ID that this node gives a cluster it forms. |
| `--cluster-listen` | `0.0.0.0:7379` | The UDP address for node-to-node traffic. |
| `--cluster-advertise` | see below | The node-to-node address that other members use. |
| `--peer` | none | A seed as `<ip>:<port>`, or `<node-id>@<ip>:<port>`. Repeat the flag. With `static`, list every other member with its node ID. |
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
| `--cluster-san` | `spiffe://cluster.local/ns/default/sa/discus` | The URI SAN that every peer must carry. |
| `--forward-read-timeout-ms` | 20 | Time to wait for a forwarded read. |
| `--forward-write-timeout-ms` | 200 | Time to wait for a forwarded write. |
| `--cluster-handshake-timeout-ms` | 2000 | Time to wait for a new connection and its `HELLO`. |
| `--cluster-idle-timeout-ms` | 30000 | A quiet connection closes after this time. |
| `--cluster-keep-alive-ms` | 5000 | Interval of keep-alive packets on a quiet connection. |
| `--cluster-max-frame-bytes` | 135266304 | The largest message this node accepts from a peer. |
| `--read-from-replicas` | `true` | Read from any owner. With `false`, every read goes to the primary. |

A node with `--peer` and without the three certificate flags does not start. The error names the
missing flags. With `static`, `--weight` must be 1, because every member has weight 1.

A node with no peers and no certificates runs alone with no UDP port, as before.

The advertised address is the first of these that applies:

1. `--cluster-advertise`.
2. `--cluster-listen` with a specific IP, not `0.0.0.0` or `::`.
3. The local address of the network interface toward the first seed.
4. `127.0.0.1` with the listen port.

In Kubernetes, set `--cluster-advertise $(POD_IP):7379` from the downward API.

## Start three nodes

Outside Kubernetes, give every node the other nodes as seeds and use the lease provider `none`.
With `none`, start one node first and let it form the cluster. Start the others after it is
ready. This example runs three nodes on one host.

```bash
TLS="--cluster-ca ca.pem --cluster-cert node.pem --cluster-key node-key.pem"
GOSSIP="--lease-provider none $TLS"

discus --listen 127.0.0.1:6380 --admin-listen 127.0.0.1:9101 --cluster-listen 127.0.0.1:7381 \
  $GOSSIP &
sleep 12
discus --listen 127.0.0.1:6381 --admin-listen 127.0.0.1:9102 --cluster-listen 127.0.0.1:7382 \
  $GOSSIP --peer 127.0.0.1:7381 &
discus --listen 127.0.0.1:6382 --admin-listen 127.0.0.1:9103 --cluster-listen 127.0.0.1:7383 \
  $GOSSIP --peer 127.0.0.1:7381 --peer 127.0.0.1:7382 &

redis-cli -p 6380 SET greeting hello
redis-cli -p 6382 GET greeting
```

`INFO discus` on each node shows `discus_membership`, `discus_member_state`,
`discus_incarnation`, `discus_epoch`, and `discus_members`.

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

- There is no replication and no handoff. A node that stops loses its keys, and a node that gains
  keys starts empty. The next release adds both.
- 0-RTT is off. Every new connection takes a full handshake.
