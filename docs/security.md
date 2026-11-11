# Security

This page tells operators how to secure the client side of a Discus node. It covers TLS on the
client listener, client authentication, scopes, OAuth 2.0 tokens, Kubernetes service account
tokens, and the admin port.

Node-to-node traffic uses QUIC with mTLS. See [Running a cluster](cluster.md) for its
certificates. The section [Commands from other nodes](#commands-from-other-nodes) tells how the two
sides meet.

## Quick start

A production node uses TLS and OAuth 2.0 tokens. This is the default policy when TLS is on:

```bash
discus \
  --tls-mode node \
  --tls-cert /etc/discus/tls.crt \
  --tls-key /etc/discus/tls.key \
  --auth-config /etc/discus/auth.toml
```

The file `/etc/discus/auth.toml` lists the identity providers that the node trusts:

```toml
[[issuer]]
issuer = "https://login.example.com"
audience = "discus"
```

A client then connects with TLS and sends its token:

```bash
redis-cli --tls -h cache.example.com AUTH "$ACCESS_TOKEN"
```

## TLS modes

The flag `--tls-mode` sets the TLS mode of the client listener.

| Mode | Meaning |
| --- | --- |
| `off` | Plaintext. Use it for development only. This is the default. |
| `node` | The node terminates TLS. It can also verify client certificates (mTLS). |
| `lb` | The load balancer terminates TLS. It forwards the client identity to the node. |

Each TLS mode needs `--tls-cert` and `--tls-key`. Both are PEM files. The certificate file can hold
a chain.

### Mode `node`

Set `--tls-client-ca` to ask clients for a certificate. The node verifies a client certificate
against this CA only. It never uses the system trust store for client certificates.

By default a client certificate is optional. Set `--tls-client-auth required` to refuse every
client without one.

The node reads the identity of a client certificate in this order:

1. The first URI SAN, for example `spiffe://example.org/ns/web/sa/frontend`.
2. The first DNS SAN.
3. The subject CN.

### Mode `lb`

In this mode an L4 load balancer accepts the client TLS connection. The load balancer opens a new
connection to the node and sends a PROXY protocol header first. Then it runs a TLS handshake with
the node and presents its own client certificate.

The node trusts the header only when both of these checks pass:

- The connection comes from a network in `--lb-trusted-source`.
- The load balancer presents a certificate that `--tls-client-ca` signed.

The node closes any other connection. It reads no header from an untrusted address.

The node accepts PROXY protocol v1 and v2. It reads these values from the header:

- The client address. The node uses it for the failed-attempt limit.
- The client identity. The node takes it from a custom TLV when `--lb-identity-tlv` names one,
  for example `0xE0`. Otherwise the node takes the CN from the `PP2_TYPE_SSL` TLV, but only when
  the load balancer says that it verified the client certificate.
- Whether the client used TLS. The `tls` factor holds unless the `PP2_TYPE_SSL` TLV says that the
  client did not use TLS. Configure the load balancer to accept TLS clients only.

Example with HAProxy:

```text
backend discus
    server node1 10.0.1.5:6379 send-proxy-v2 send-proxy-v2-ssl-cn ssl crt lb.pem ca-file ca.pem
```

### TLS on the redirect listener

The redirect listener uses the same TLS mode, certificates, and client CA as `--listen`. The flag
`--tls-mode lb` together with `--redirect-listen` is a start error. In `lb` mode, clients reach the
nodes only through the load balancer, so they cannot route by themselves.

A cluster client connects to the address in the topology reply. The client compares the certificate
of the node with that address. The certificate must therefore name the address. Two setups work:

- A certificate for each pod with an IP SAN. The IP SAN is the pod IP, which is the address that
  the node advertises. In Kubernetes, the pod IP changes at each restart, so the certificate must
  follow it.
- The flag `--client-hostname` with a wildcard DNS SAN. For example, the template
  `{ip}.discus-peers.<namespace>.svc.cluster.local` fits the SAN
  `*.discus-peers.<namespace>.svc.cluster.local`. The topology replies then name hosts, so one
  certificate serves every pod. The template is optional. The Discus tests do not test that
  CoreDNS answers such names for the pods of a headless Service.

Every connection must authenticate. A cluster client opens one connection to each node and
authenticates on each one. The limit on failed attempts counts for each node.

If an authentication policy is on, the `CLUSTER` subcommands, `READONLY`, `READWRITE`, and `ASKING`
need an authenticated connection. The replies show the internal address of every node. A command
that the scopes deny returns `-NOPERM`, never `-MOVED`.

## Authentication policy

The flag `--auth-policy` sets which factors a client needs. There are four factors:

| Factor | The client has it when |
| --- | --- |
| `tls` | It uses TLS. This factor never identifies a client, so it never works alone. |
| `mtls` | It presents a verified client certificate. |
| `oauth2` | It sends a valid OAuth 2.0 token with `AUTH`. |
| `shared_token` | It sends the shared token with `AUTH`. |

The policy has one of these forms:

- `all:<factor>,<factor>`. The client needs every factor.
- `any:<factor>,<factor>`. The client needs one factor.
- `none`. Every client is in, with read and write access. Use it for development only.

The node refuses to start with a policy that `tls` alone can meet, for example `all:tls` or
`any:tls,oauth2`.

Without `--auth-policy`, the node picks a default:

| Setting | Default policy |
| --- | --- |
| TLS on | `all:tls,oauth2` |
| TLS off, `--shared-token` set | `any:shared_token` |
| TLS off, no shared token | `none` |

If you set `--shared-token` with TLS on, you must also set a policy that uses `shared_token`, for
example `all:tls,shared_token`. A shared token that the policy does not use stops the node at
startup.

Examples:

| Goal | Policy |
| --- | --- |
| TLS and a token from an identity provider | `all:tls,oauth2` |
| A client certificate or a token | `any:mtls,oauth2` |
| A client certificate and a token | `all:mtls,oauth2` |
| A simple setup with one password over TLS | `all:tls,shared_token` |

### Commands

A client sends its credential with one of these commands:

- `AUTH <token>`
- `AUTH <username> <token>`. The username is only a hint.
- `AUTH OAUTH2 <token>`. This form uses OAuth 2.0 only.
- `HELLO 3 AUTH <username> <token>`

With a policy that has both `shared_token` and `oauth2`, `AUTH <token>` tries the shared token
first.

A failed `AUTH` returns `-WRONGPASS invalid username-password pair or user is disabled.` for every
reason. The reason goes to the debug log. A failed `AUTH` does not remove an earlier successful
login on the same connection.

A command before a successful login returns `-NOAUTH Authentication required.`.

## Scopes

Discus has three scopes:

| Scope | Grants |
| --- | --- |
| `discus.read` | Commands that read data. |
| `discus.write` | Commands that read or change data. |
| `discus.admin` | The admin routes. It grants no data access. |

A read-only client that runs a write command gets
`-NOPERM this user has no permissions to run the '<command>' command`.

Connection commands need a login but no scope. They are `PING`, `ECHO`, `QUIT`, `AUTH`, `HELLO`,
`SELECT`, `COMMAND`, `CLIENT`, and `CLUSTER`. `INFO` and `CONFIG GET` count as reads.

Each factor grants scopes:

- The shared token grants `discus.read` and `discus.write`.
- A client certificate grants the scopes of its first matching `[[client_cert]]` rule. Without a
  match it grants `discus.read` and `discus.write`.
- A token grants the scopes in its claims, or the scopes of its `sub` rule.

With an `all` policy, the client gets only the scopes that every identity factor grants. With an
`any` policy, the client gets the scopes of every factor it has.

Client certificate rules go in the `--auth-config` file. A trailing `*` matches any suffix:

```toml
[[client_cert]]
identity = "spiffe://example.org/ns/reporting/*"
scopes = ["discus.read"]
```

## Commands from other nodes

The node that a client connects to is the entry node. The entry node checks every command before
it routes the command:

1. It looks up the command and checks the number of arguments.
2. It checks that the connection meets the authentication policy.
3. It checks the scopes of the connection for the command.

Then the entry node runs the command or forwards it to the owner of the key. The owner does not
check the authentication or the scopes again. The cluster mTLS proves that the command comes from
a member of the cluster, and the entry node already checked the client. Thus a peer certificate
gives full data access. Keep the cluster CA and the node keys as safe as the admin credentials.

## Failed attempts

The node limits failed `AUTH` attempts in two ways:

- After 5 failed attempts, the node closes the connection. Set the number with
  `--auth-max-failures-per-connection`.
- After 20 failed attempts from one source address within 60 seconds, the node refuses every
  `AUTH` from that address until the window ends. It does not check the token. Set the numbers
  with `--auth-max-failures-per-source` and `--auth-failure-window-secs`.

Successful attempts never count.

## OAuth 2.0

Discus is an OAuth 2.0 resource server. It does not issue tokens. It checks each JSON Web Token
(JWT) locally with the public keys of the issuer.

### Trusted issuers

Each `[[issuer]]` section in the `--auth-config` file adds one trusted issuer:

```toml
[[issuer]]
issuer = "https://login.example.com"
audience = "discus"
# All settings below are optional. The values shown are the defaults.
discovery_url = "https://login.example.com/.well-known/openid-configuration"
algorithms = ["RS256", "ES256"]
scope_prefix = ""
jwks_refresh_secs = 300
jwks_min_refetch_secs = 30
jwks_max_stale_secs = 3600
# ca_file = "/etc/discus/idp-ca.pem"
```

The node checks each token against the issuer that its `iss` claim names. A key of one issuer never
verifies a token that names another issuer.

The node checks these items:

- The signature, with an algorithm from `algorithms`. `HS256`, `HS384`, `HS512`, and `none` are
  never allowed.
- `iss` equals `issuer`.
- `aud` contains `audience`.
- `exp` is present and not past. `nbf`, when present, is not in the future. `iat`, when present,
  is not in the future.
- The token grants at least one Discus scope.

All time checks allow 60 seconds of clock skew. Set it with `--auth-clock-skew-secs`.

The node reads scopes from the `scope` claim (a space-separated string) or the `scp` claim (a
string or a list). When the identity provider adds a prefix, set `scope_prefix`. For example, with
`scope_prefix = "api://discus/"`, the scope `api://discus/discus.read` grants `discus.read`.

An issuer can map the `sub` claim to scopes instead. With at least one `[[issuer.subject]]` rule,
the node ignores the `scope` claim and refuses a token whose `sub` matches no rule:

```toml
[[issuer.subject]]
match = "service-a"
scopes = ["discus.read", "discus.write"]
```

### Keys and readiness

The node fetches the discovery document of each issuer at startup. Then it fetches the key set
(JWKS) from the `jwks_uri` of the document. The discovery document must name the same issuer.

- The node is not ready until every issuer has its keys. `/startupz` and `/readyz` answer 503
  until then.
- The node fetches the keys again every `jwks_refresh_secs`.
- A token with an unknown key ID (`kid`) makes the node fetch the keys at once. It does this at
  most once per `jwks_min_refetch_secs` for each issuer.
- After a fetch, the node checks the token of each open connection again on its next command. A
  token whose key is gone gets `-NOAUTH`.
- If the issuer is unreachable, the node keeps the keys it has. After `jwks_max_stale_secs`
  without a successful fetch, the node refuses every token from that issuer. The node stays
  ready.

The node uses the trust root build feature for HTTPS to the issuer. Set `ca_file` to trust only
one CA instead.

### Expiry

The node checks the full token at `AUTH` and after a key refresh. For each other command, it
checks only the `exp` and `nbf` values that it saved. When the token expires, the next command
returns `-NOAUTH`. The client can send `AUTH` with a new token on the same connection.

### TLS requirement

OAuth 2.0 needs TLS. With `--tls-mode off`, the node refuses to start with an `oauth2` policy.
For development, set `--dev-insecure-oauth2`. This flag also allows `http` issuer addresses.

The node never logs a token. The debug log shows the `sub` and `jti` claims only.

### Introspection

Some identity providers issue opaque tokens instead of JWTs. A build with the
`oauth2-introspection` feature checks them with RFC 7662 introspection. Add a client ID and a
secret file to one issuer:

```toml
[[issuer]]
issuer = "https://login.example.com"
audience = "discus"
client_id = "discus"
client_secret_file = "/etc/discus/introspection-secret"
# introspection_url = "https://login.example.com/oauth2/introspect"
```

Only one issuer can turn on introspection. The node sends a token with no JWT shape to that
issuer only. Without `introspection_url`, the node uses the `introspection_endpoint` of the
discovery document.

The node accepts an answer only when `active` is true and `exp` is present and not past. When the
answer has `iss` or `aud`, they must match. The node keeps an answer until the token expires, or
for at most 300 seconds.

## Kubernetes service accounts

A workload in the cluster can use its own service account token. It needs no client secret.

Add a `[kubernetes]` section to the `--auth-config` file:

```toml
[kubernetes]
# The values shown are the defaults.
api_server = "https://kubernetes.default.svc"
ca_file = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
audience = "discus"

[[kubernetes.account]]
match = "system:serviceaccount:web:frontend"
scopes = ["discus.read", "discus.write"]

[[kubernetes.account]]
match = "system:serviceaccount:batch:*"
scopes = ["discus.read"]
```

The node fetches the discovery document and the keys from the API server with the cluster CA. It
takes the expected `iss` from the discovery document. Set `issuer` to fix it instead.

Each `match` names one account, `system:serviceaccount:<namespace>:<name>`, or every account in a
namespace, `system:serviceaccount:<namespace>:*`. The node refuses an account that matches no
rule.

The node refuses legacy tokens from Secrets, because they have no `exp` and no audience.

The workload mounts a projected token with the audience `discus`:

```yaml
volumes:
  - name: discus-token
    projected:
      sources:
        - serviceAccountToken:
            audience: discus
            expirationSeconds: 3600
            path: token
```

The kubelet replaces the token before it expires. The client must read the file again before
each new connection.

Discus does not call TokenReview. A deleted pod keeps access until its token expires, so keep the
lifetime short. The Kubernetes minimum is 10 minutes.

If the API server blocks anonymous discovery, the node sends its own token from `token_file`.
Bind the ServiceAccount of Discus to the `system:service-account-issuer-discovery` ClusterRole:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: discus-issuer-discovery
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:service-account-issuer-discovery
subjects:
  - kind: ServiceAccount
    name: discus
    namespace: cache
```

## Admin port

The admin port (`--admin-listen`) has two kinds of routes:

- The probes `/healthz`, `/startupz`, and `/readyz` are open. The kubelet calls them without
  credentials. They show only the phase of the node.
- Every route under `/admin/` needs admin credentials. `GET /admin/security` shows the TLS mode,
  the policy, readiness, and the key status of each issuer. `POST /admin/drain` starts a drain
  (see `docs/cluster.md`). `GET /admin/counters` shows the role of the node in each counter
  group. `POST /admin/raft/force-reconfigure` is `RAFT_FORCE_RECONFIGURE`. It makes the node the
  only voter of each Raft group in which it is a voter, and it can lose counter writes. Only the
  admin port has it.

An admin request passes in one of two ways:

- The client presents a certificate whose identity matches an `[admin]` pattern. This needs
  `--admin-tls`.
- The client sends `Authorization: Bearer <token>` with a token that has `discus.admin`. Over
  plaintext, this needs `--dev-insecure-oauth2`.

The shared token never opens the admin routes. Without admin credentials, the node answers 401.

With `--admin-tls`, the admin port uses TLS with `--tls-cert`, `--tls-key`, and
`--tls-client-ca`. Admin certificate identities go in the `--auth-config` file:

```toml
[admin]
cert_identities = ["spiffe://example.org/ns/ops/*"]
```

## Build features

Each part of this page needs a Cargo feature. If the configuration uses a part that the binary
lacks, the node refuses to start and names the missing feature.

| Part | Feature |
| --- | --- |
| TLS modes, client certificates, shared token, admin certificates | Always present |
| OAuth 2.0 tokens and `[[issuer]]` | `oauth2` |
| Introspection | `oauth2-introspection` |
| `[kubernetes]` | `k8s` and `oauth2` |

The TLS backend feature (`tls-aws-lc`, `tls-ring`, or `tls-openssl`) supplies the cryptography
for the client listener, the admin port, HTTPS to identity providers, the OTLP exporter, and the
node-to-node QUIC transport. The build has one provider for all of them. QUIC uses only its
TLS 1.3 cipher suites. Token signatures use
`aws-lc-rs` with `tls-aws-lc`. With `tls-ring` or `tls-openssl`, token signatures use the RustCrypto
crates, because the JWT library has no `ring` or OpenSSL backend.

The trust root feature (`roots-system` or `roots-webpki`) supplies the roots for HTTPS to an
identity provider without `ca_file`.

## Flag reference

| Flag | Default | Effect |
| --- | --- | --- |
| `--tls-mode` | `off` | The client listener TLS mode: `off`, `node`, or `lb`. |
| `--tls-cert` | none | The PEM certificate chain of the client listener and the admin port. |
| `--tls-key` | none | The PEM private key that matches `--tls-cert`. |
| `--tls-client-ca` | none | The CA for client certificates, or for the load balancer in `lb` mode. |
| `--tls-client-auth` | `optional` | `required` refuses clients without a certificate in `node` mode. |
| `--lb-trusted-source` | none | A network that the load balancer connects from. Repeat it for more. |
| `--lb-identity-tlv` | none | A PROXY v2 TLV type from `0xE0` to `0xEF` that holds the client identity. |
| `--auth-policy` | see above | The client authentication policy. |
| `--auth-config` | none | The TOML file with issuers, rules, and admin identities. |
| `--shared-token` | none | The shared token for the `shared_token` factor. |
| `--dev-insecure-oauth2` | off | Allows OAuth 2.0 over plaintext and `http` issuers. Development only. |
| `--auth-clock-skew-secs` | 60 | The clock skew allowed on token times. |
| `--auth-max-failures-per-connection` | 5 | Failed attempts that close a connection. |
| `--auth-max-failures-per-source` | 20 | Failed attempts per source address in one window. |
| `--auth-failure-window-secs` | 60 | The window of the per-source limit. |
| `--admin-tls` | off | Serves the admin port over TLS. |

The node reads certificates, keys, and the `--auth-config` file once at startup. To change them,
restart the node.
