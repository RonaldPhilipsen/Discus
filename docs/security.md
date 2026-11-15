# Security

This page tells operators how to secure the client side of a Discus node. It covers TLS on the
client listener, client authentication, scopes, OAuth 2.0 tokens, Kubernetes service account
tokens, and the admin port.

Every client connection uses TLS. A node without `--tls-cert` and `--tls-key` refuses to start.
Mutual TLS (mTLS), where the client also presents a certificate, is a choice. Node-to-node traffic
always uses QUIC with mTLS. See [Running a cluster](cluster.md) for its certificates. The section
[Commands from other nodes](#commands-from-other-nodes) tells how the two sides meet.

## Quick start

A node needs an authentication policy. Without one, it refuses to start. With the policy `none`,
the node asks only for TLS. Each client connects with TLS and needs no credentials:

```bash
discus --tls-cert /etc/discus/tls.crt --tls-key /etc/discus/tls.key --auth-policy none
```

To also ask each client for an OAuth 2.0 token, set the policy `all:oauth2`:

```bash
discus \
  --tls-cert /etc/discus/tls.crt \
  --tls-key /etc/discus/tls.key \
  --auth-policy all:oauth2 \
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
| `node` | The node terminates TLS. It can also verify client certificates (mTLS). This is the default. |
| `lb` | The load balancer terminates TLS. It forwards the client identity to the node. |

There is no plaintext mode. Each TLS mode needs `--tls-cert` and `--tls-key`. Both are PEM files.
The certificate file can hold a chain.

### Mode `node`

Set `--tls-client-ca` to ask clients for a certificate. The node verifies a client certificate
against this CA only. It never uses the system trust store for client certificates.

Use a client CA that is not the cluster CA. With an empty `--cluster-san`, every certificate from
the cluster CA is a member of the cluster, and a member needs no client authorization. A client
certificate from that CA then gives access to the cluster ports. If one CA must serve both, set
`--cluster-san`. The Helm chart refuses an install that has the same CA for both and no URI SAN
test. See [Install with Helm](cluster.md#install-with-helm).

By default a client certificate is optional. Set `--tls-client-auth required` to refuse every
client without one.

The node reads the identity of a client certificate in this order:

1. The first URI SAN, for example `spiffe://example.org/ns/web/sa/frontend`.
2. The first DNS SAN.
3. The subject CN.

A certificate without any of these has no identity. It does not meet the `mtls` factor.

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
  the load balancer says that it verified the client certificate. If the header has a
  `PP2_TYPE_SSL` TLV, the custom TLV also counts only when the load balancer verified the client
  certificate. An empty identity is no identity.
- Whether the client used TLS. If the `PP2_TYPE_SSL` TLV says that the client did not use TLS, the
  node closes the connection. Configure the load balancer to accept TLS clients only.

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
  certificate serves every pod. The Helm chart sets this template by default. CoreDNS answers
  these names for the pods of a headless Service.

Every connection must authenticate. A cluster client opens one connection to each node and
authenticates on each one. The limit on failed attempts counts for each node.

If an authentication policy is on, the `CLUSTER` subcommands, `READONLY`, `READWRITE`, and `ASKING`
need an authenticated connection. The replies show the internal address of every node. A command
that the scopes deny returns `-NOPERM`, never `-MOVED`.

## Authentication policy

The flag `--auth-policy` sets which factors a client needs. There are three factors:

| Factor | The client has it when |
| --- | --- |
| `mtls` | It presents a verified client certificate. |
| `oauth2` | It sends a valid OAuth 2.0 token with `AUTH`. |
| `password` | It sends the name and the password of a user with `AUTH`. |

The policy has one of these forms:

- `all:<factor>,<factor>`. The client needs every factor.
- `any:<factor>,<factor>`. The client needs one factor.
- `none`. Every client is in, with read and write access. Every client still uses TLS.

Every client uses TLS, so TLS is not a factor. For TLS without credentials, use `none`.

Without `--auth-policy`, the result depends on the ACL file:

| Setting | Result |
| --- | --- |
| `--acl-file` set | The policy is `all:password`. |
| No ACL file | The node refuses to start. The error names `--auth-policy`. |

The node never opens the client port without credentials unless you ask for it. Set
`--auth-policy none` to make that choice.

With `none`, every client that can reach the port and complete the TLS handshake gets read and
write access. The node logs a warning about this at startup. To limit access, set a policy with
`mtls`, `oauth2`, or `password`.

Examples:

| Goal | Policy |
| --- | --- |
| A token from an identity provider | `all:oauth2` |
| A client certificate or a token | `any:mtls,oauth2` |
| A client certificate and a token | `all:mtls,oauth2` |
| One password for every client, or named users with rules | `all:password` |
| A client certificate or a user with a password | `any:mtls,password` |

### Users and passwords

Users give each client its own password and its own rules. The rules say which commands the user
can run and which keys the user can read or write. Put the users in an ACL file and set
`--acl-file`:

```bash
discus \
  --tls-cert /etc/discus/tls.crt \
  --tls-key /etc/discus/tls.key \
  --acl-file /etc/discus/users.acl
```

Without `--auth-policy`, the node uses the policy `all:password`. The format is the aclfile
format of Valkey, with a few limits. See [Differences from Valkey](#differences-from-valkey).

#### One password for every client

For a simple setup, give the user `default` one password and every right. This file has one line:

```text
user default on allkeys +@all #<64 hex digits>
```

A client then sends `AUTH <password>`, without a name. To make the hash of a password, run this
command:

```bash
printf '%s' "$PASSWORD" | sha256sum
```

Older versions had the flags `--shared-token` and `--shared-token-file` and the factor
`shared_token` for this setup. They are gone. Put the token in an ACL file as the password of the
user `default`, and use the factor `password`.

One thing changes for the clients. The shared token accepted every username. The password
belongs to the user `default`, so a client must send no username or the username `default`.

#### The file

Each line that is not empty has the form `user <name> <rule> <rule> ...`. A line that starts with
`#` is a comment. A new user starts as `off`, with no password, no keys, and no commands. The node
applies the rules from left to right. This is an example file:

```text
# An administrator. It can run every command on every key.
user admin on allkeys +@all #f8d0ecc0b1332a8d24b9aedd2b08a1de79116e43eb661409bc1bc07bab948ec5

# A read-only user. It can read every key and run every command of @read.
user reporting on allkeys +@read +@connection #c44b2dc134935203670c2f44343df9b18a97e59a460919753d1d010b1f4e3552

# A service. It reads the keys "config:*" and writes the keys "queue:*".
# It cannot run dangerous commands, such as FLUSHALL.
# It can list the names of all keys with SCAN.
user app on %R~config:* %W~queue:* +@read +@write -@dangerous +@connection #da5aef018d0ebf8aa8b9ce37720c50fd9aae477c00a7cae02fba69a6aeb80093
```

The hashes in this example are for the passwords `admin-secret-p4ss`, `reader-secret-p4ss`, and
`app-secret-p4ss`. Never use them. Make a new password and a new hash for each user.

The `+@read` rule of the user `app` also allows `SCAN`, `RANDOMKEY`, and `DBSIZE`. These commands
have no key argument. Thus `app` can see the names and the count of all keys, also outside
`config:*` and `queue:*`.

These are the rules:

| Rule | Effect |
| --- | --- |
| `on`, `off` | The user can log in, or cannot. |
| `>password` | Adds a password. The node keeps only its SHA-256 hash. |
| `#<64 hex digits>` | Adds a password as a SHA-256 hash. |
| `nopass` | Every password matches. Removes the stored passwords. |
| `resetpass` | Removes the stored passwords and `nopass`. |
| `~pattern` | Read and write access to the keys that match the glob pattern. |
| `%R~pattern`, `%W~pattern`, `%RW~pattern` | Read access, write access, or both. |
| `allkeys` | The same as `~*`. |
| `resetkeys` | Removes every key pattern. |
| `+command`, `-command` | Allows or denies one command, with all of its subcommands. |
| `+acl\|subcommand`, `-acl\|subcommand` | Allows or denies one subcommand of `ACL`, such as `+acl\|list`. |
| `+@category`, `-@category` | Allows or denies every command of a category. `@all` is every command. |
| `allcommands`, `nocommands` | The same as `+@all` and `-@all`. |
| `&pattern`, `allchannels`, `resetchannels` | Accepted, with no effect. Discus has no pub/sub. |
| `reset` | Puts the user back in the state of a new user. |

A password cannot hold whitespace. A user can have more than one password.

The hash is SHA-256 with no salt. Thus a stolen file lets an attacker test guesses at high speed.
Use long random passwords. To make a hash, run this command:

```bash
printf %s "$PASSWORD" | sha256sum
```

Use the 64 hex digits of the output. The file is a secret, so give it the same care as a private
key.

#### Errors that stop the node

The node refuses to start with a file that has an error. Most errors name the line. These are the
causes:

- A line does not start with `user`, or it has no name.
- Two lines name the same user.
- The name is `OAUTH2` in any case. `AUTH OAUTH2 <token>` selects OAuth 2.0.
- A rule is not in the table. This includes `<password`, `!hash`, selectors, and `+command|sub`.
- A `+` or `-` rule names a command that Discus does not have.
- A password hash after `#` does not have exactly 64 hex digits.
- A category is not one of the 21 categories of Valkey or `all`.
- The file has no user. This error names no line.
- The file is missing or the node cannot read it. The error has the form `--acl-file <path>: ...`.

An error for a rule gives the position of the word and never its text. A mistyped rule can hold a
password, and the error goes to the log. These are the forms:

- `line 3: unknown rule at word 5`
- `line 3: unknown command at word 5`
- `line 3: unknown category at word 5`
- `line 3: this command has no rules for one subcommand (word 5)`

The first word of a line is `user`, and the second word is the name. Thus the first rule is word 3.
An error for the name shows the name.

A `-` rule for a command that Discus lacks stops the node, because a typing error in a `-` rule
otherwise leaves a command open. A category without Discus commands, such as `@pubsub`, is valid.

The node also refuses to start in these cases:

- The policy has `password`, and `--acl-file` is not set.
- `--acl-file` is set, and the policy has no `password`.

#### Categories and key access

Each command has the categories of Valkey:

- One data category: `@string`, `@hash`, `@set`, `@sortedset`, `@keyspace`, or `@connection`.
- `@read` or `@write`, for a data command.
- `@fast` or `@slow`.
- `@dangerous` or `@admin`, where Valkey sets them.

Category rules are the portable form, because they work in Valkey and in Discus. To list the
commands of a category, run `ACL CAT <category>`. `CLUSTER` is in `@connection`, so `+@connection`
is enough for a cluster client. `INFO` and `CONFIG` are in `@dangerous`, not in `@read`.

`@write` includes `FLUSHDB`, `FLUSHALL`, and `SWAPDB`. They have no key argument, so a user with
`+@write` can run them on any key. To remove them, add `-@dangerous` after `+@write`.

A key that a command reads needs read access. A key that a command changes needs write access. A
command that changes a key and uses its old value needs both. These commands are:

- `GETDEL` and `GETEX`.
- `INCR`, `DECR`, `INCRBY`, `DECRBY`, and `INCRBYFLOAT`.
- `HINCRBY`, `ZINCRBY`, `SPOP`, and `MOVE`.
- `SET` with the `GET` option.

`RENAME` needs read and write on the source and write on the destination. `COPY` needs read on the
source and write on the destination.

Each key needs one pattern that grants the whole access. Two patterns do not add up. For example,
`%R~x` and `%W~x` together do not allow `INCR x`. A command that names more than one key and
meets a denied key changes nothing.

A command without key arguments needs only its command rule. Thus `SCAN` and `RANDOMKEY` show key
names that the user cannot read, as in Valkey.

#### Login

- If the file names the username, and the name is not `default`, the value is the password of that
  user. A wrong password fails. The node does not try the value as an OAuth 2.0 token.
- If the file does not name the username, the username is only a hint. The value is a token.
- `AUTH <value>` tries the user `default` first. If that fails and the policy has `oauth2`, the
  node tries the value as a token.
- `AUTH default <value>` works like `AUTH <value>`. Many client libraries send this form for a
  credential without a username. Only the exact name `default` counts, because names are
  case-sensitive.
- If the command names a user of the file, the node never sends the value to an identity
  provider. This also holds for a wrong password.
- If the user `default` accepts the value, the node does not send the value to an identity
  provider either.
- In every other case, the node treats the value as a token. With introspection, the value then
  goes to the identity provider. Thus a client that sends a password without its username, or
  with a mistyped username, sends that password to the identity provider.
- A user that is `off` cannot log in.
- A second successful `AUTH` on a connection replaces the user.
- With an `any` policy, a successful `AUTH` replaces the earlier login of the connection. A login
  with a password removes an earlier token, and a login with a token removes an earlier user. A
  client certificate stays. With an `all` policy, the connection keeps both, because it needs
  both.

Every failure returns the same `-WRONGPASS` error and counts for the limits in
[Failed attempts](#failed-attempts).

#### Rules and scopes

A user has no scopes. Its rules decide, and they cover the connection commands too. `AUTH`,
`HELLO`, and `QUIT` always run. `+@connection` covers `PING`, `ECHO`, `SELECT`, `COMMAND`,
`CLIENT`, `CLUSTER`, `ACL`, `READONLY`, `READWRITE`, and `ASKING`. `INFO` and `CONFIG` are in
`@dangerous`, so a user needs a rule for them.

A client certificate or a token keeps its scopes. The policy says how the scopes and the rules
combine:

- With an `all` policy, the scopes and the rules must both allow the command.
- With an `any` policy, one of them is enough.

With an `any` policy, such as `any:mtls,password`, take care with client certificates. A
certificate that matches no `[[client_cert]]` rule grants read and write. That client can run
every data command, whatever the rules of its user say.

#### Denied commands

A command that the rules deny returns this error:

```text
-NOPERM this user has no permissions to run the '<command>' command
```

A key that the rules deny returns this error. The text never names the key:

```text
-NOPERM this user has no permissions to access one of the keys used as arguments
```

#### Differences from Valkey

The file is the only source of users. If you copy a Valkey ACL file, change these items:

- `ACL SETUSER`, `DELUSER`, `LOAD`, `SAVE`, `LOG`, `GENPASS`, and `DRYRUN` return an error.
- Selectors, such as `(~temp:* +get)`, stop the node.
- Only `ACL` has rules for one subcommand. A rule for a subcommand of a different command, such as
  `+config|get`, stops the node.
- Discus has no rule for each database.
- In Valkey, `CLIENT`, `CLUSTER`, and `CONFIG` have only `@slow`, and each subcommand has its own
  categories. In Discus, each of these commands has one set of categories.
- `ACL`, `ACL WHOAMI`, `ACL CAT`, and `CLUSTER` are in `@connection`. In Valkey, they are not.
- A category rule sets each subcommand of `ACL` by the categories of the subcommand. In Valkey,
  `+@slow` allows every subcommand of `ACL`, because `ACL` itself is in `@slow`.
- `ACL LIST` and `ACL GETUSER` do not show the flag `sanitize-payload` and show no channel rules
  other than `resetchannels`.
- `ACL LIST` and `ACL USERS` show the user `default` only when the file names it.
- `CLIENT` has the categories of `CLIENT SETNAME`. `CONFIG` has the categories of `CONFIG GET`.
- A `+` or `-` rule for a command that Discus lacks stops the node. Use category rules.
- The rule words, such as `on` and `nopass`, are case-sensitive. Command names and category names
  ignore case.
- A byte order mark at the start of the file is an error.
- If the policy needs the `password` factor, a client that sends no `AUTH` gets `-NOAUTH`, also
  with `user default on nopass`. In Valkey, that client is the user `default`.
- A user never opens the admin port.
- Discus does not copy users between nodes. Each node reads its own file.

#### The ACL command

`ACL WHOAMI` returns the name of the user of the connection. `ACL CAT` lists the categories, and
`ACL CAT <category>` lists the commands of one category. These two subcommands are in
`@connection`.

Three subcommands show the users of the node:

| Subcommand | Reply |
| --- | --- |
| `ACL USERS` | The names of the users, in sorted order. |
| `ACL LIST` | One line for each user, in the format of the ACL file, with the password hashes. |
| `ACL GETUSER <name>` | The flags, the password hashes, the command rules, and the key rules of one user. |

> [!WARNING]
> Give these three subcommands only to an administrator. The password hashes have no salt, so a
> person who reads a hash can find a weak password.

The three subcommands are in `@admin` and `@dangerous`, as in Valkey. These rules decide who can
run them:

- A user with `+@all` can run them. Add `-@admin` or `-@dangerous` after `+@all` to deny them.
- `+@connection` does not allow them.
- A rule for one subcommand allows or denies one of them, for example `+acl|users` or
  `-acl|getuser`.
- `+acl` allows every subcommand of `ACL`, and `-acl` denies every subcommand.
- A connection without a user of the file, such as a login with a certificate or a token, needs
  the scope `discus.admin`.

A denied subcommand returns this error:

```text
-NOPERM this user has no permissions to run the 'acl|list' command
```

The node prints the command rules of a user in a normal form. It starts with `-@all` or `+@all`,
and a later rule replaces an earlier rule for the same command or category. Each node shows the
users of its own file. During a change of the Secret, two nodes can thus give different replies.

#### In Kubernetes

In the Helm chart, put the file in a Secret and set `aclFile.secretName`. The chart mounts the
Secret read-only and sets `--acl-file`. The value `aclFile.key` names the key of the Secret that
holds the file. The default key is `users.acl`.

```bash
kubectl create secret generic discus-acl --from-file=users.acl
helm upgrade discus charts/discus --reuse-values --set aclFile.secretName=discus-acl
```

To change the file while the node runs, see [ACL file rotation](#acl-file-rotation).

### Commands

A client sends its credential with one of these commands:

- `AUTH <token>`
- `AUTH <username> <token>`. If the ACL file does not name the username, the username is only a
  hint.
- `AUTH OAUTH2 <token>`. This form uses OAuth 2.0 only.
- `AUTH <username> <password>`. This form logs in a user of the ACL file.
- `HELLO 3 AUTH <username> <token>`

With a policy that has both `password` and `oauth2`, `AUTH <token>` tries the user `default`
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

The scopes do not apply to a user of the ACL file. The rules of the user decide. See
[Rules and scopes](#rules-and-scopes).

A read-only client that runs a write command gets
`-NOPERM this user has no permissions to run the '<command>' command`.

Connection commands need a login but no scope. They are `PING`, `ECHO`, `QUIT`, `AUTH`, `HELLO`,
`SELECT`, `COMMAND`, `CLIENT`, and `CLUSTER`. `INFO` and `CONFIG GET` count as reads. A user of
the ACL file needs a rule for each of them, except `AUTH`, `HELLO`, and `QUIT`.

Each factor grants scopes:

- A client certificate grants `discus.read` and `discus.write` when the `--auth-config` file has
  no `[[client_cert]]` rule. With at least one rule, it grants the scopes of its first matching
  rule. A certificate that matches no rule gives no `mtls` factor.
- A token grants the scopes in its claims, or the scopes of its `sub` rule.

With an `all` policy, the client gets only the scopes that every identity factor grants. With an
`any` policy, the client gets the scopes of every factor it has.

Client certificate rules go in the `--auth-config` file. A trailing `*` matches any suffix:

```toml
[[client_cert]]
identity = "spiffe://example.org/ns/reporting/*"
scopes = ["discus.read"]
```

With this file, a client whose certificate identity does not start with
`spiffe://example.org/ns/reporting/` does not have the `mtls` factor. Discus keeps the TLS
connection open, so the client can still log in with another factor that the policy accepts. Add
a rule for each identity that needs access.

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
- After 20 failed attempts from one source within 60 seconds, the node refuses every `AUTH` from
  that source until the window ends. It does not check the token. Set the numbers with
  `--auth-max-failures-per-source` and `--auth-failure-window-secs`.

A source is one IPv4 address or one IPv6 /64 network. Thus all addresses in the same /64 share
one count. An IPv4-mapped IPv6 address such as `::ffff:192.0.2.1` counts as its IPv4 address.

Successful attempts never count.

The node tracks at most 65,536 sources at the same time. When this table is full, failed attempts
from all other sources go to one shared count. When the shared count reaches the limit, the node
refuses every `AUTH` from a source that is not in the table until the window ends. This also
refuses a new client that has the correct token. Connections that are already authenticated
continue to work.

The limit per source also covers bearer tokens on the admin port. See
[Admin port](#admin-port).

## Limits before authentication

With a policy other than `none`, a connection that has not met the policy yet gets small limits.
A client without a credential then cannot use much memory on the node.

- One argument and one buffered request are at most 64 KiB, and one command has at most 64
  arguments. This is enough for `HELLO` and `AUTH` with the largest token. A larger request gets
  a protocol error, and the node closes the connection.
- The connection has 30 seconds from the accept to meet the policy. After that the node closes it.
  Set the time with `--auth-timeout-secs`. The value `0` turns the deadline off. A new value
  from the settings file applies to new connections.

When the connection meets the policy, the normal limits apply from the next command. So a client
can send `AUTH` and a large `SET` in one pipeline. A client certificate that meets the policy
alone gives the normal limits from the start. The normal limits then stay for the life of the
connection.

The node also limits the number of open client connections, with every policy. The default is
10000. Set it with `--max-clients`. The value `0` turns the limit off. The count includes
connections in the TLS handshake, and the listener and the redirect listener share it. At the
limit, the node accepts a new TCP connection and closes it at once, before the TLS handshake. The
metric `discus.client.rejected_connections` counts these connections.

The settings file sets both values too, with the keys `[auth] timeout-secs` and `[clients] max`.
The chart values are `settings.auth.timeoutSecs` and `settings.clients.max`. The node applies a
change without a restart. A lower connection limit closes no open connection. The node refuses new
connections until the count is below the new limit.

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
  token whose key is gone gets `-NOAUTH`, and the node closes the connection.
- If the issuer is unreachable, the node keeps the keys it has. After `jwks_max_stale_secs`
  without a successful fetch, the node refuses every token from that issuer. The node stays
  ready.

The node uses the trust root build feature for HTTPS to the issuer. Set `ca_file` to trust only
one CA instead.

### Expiry

The node checks the full token at `AUTH` and after a key refresh. For each other command, it
checks only the `exp` and `nbf` values that it saved. When the token expires, the next command
returns `-NOAUTH`, and the node closes the connection. The client opens a new connection and sends
`AUTH` with a new token. To keep a connection, send `AUTH` with a new token before the old one
expires.

A connection that used an opaque token also has a recheck time. See
[Introspection](#introspection).

### Issuer addresses

The node fetches keys from an issuer over HTTPS. For development, `--dev-insecure-oauth2` also
allows `http` issuer addresses.

The node never logs a token. The debug log shows the `sub` and `jti` claims only. The node refuses
a token that is longer than 16 KiB, before it decodes the token or sends it to an identity
provider.

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
# require_aud = false
```

Only one issuer can turn on introspection. The node sends a token with no JWT shape to that
issuer only. Without `introspection_url`, the node uses the `introspection_endpoint` of the
discovery document.

The node accepts an answer only when `active` is true and `exp` is present and not past. When the
answer has `iss` or `aud`, they must match. With `require_aud = true`, the node also refuses an
answer without `aud`. Set it when the identity provider always sends `aud`, so a token for another
service never gets access.

When the answer has `token_type`, it must name an access token: `access_token`, `Bearer`,
`bearer`, or `urn:ietf:params:oauth:token-type:access_token`. The node refuses every other token
type, for example a refresh token. The node keeps an answer until the token expires, or
for at most 300 seconds.

A connection keeps its access only as long as the answer lives. When the answer ends, the node
asks the identity provider again before it runs the next command of that connection. Thus the
node sees a revoked token after at most 300 seconds, also on an open connection. Connections
that use the same token share one request.

- If the token is still active, the connection keeps its access. The node uses the scopes and
  the `exp` value of the new answer.
- If the token is not active, the connection loses its access. The command returns `-NOAUTH`,
  and the node closes the connection.
- If the identity provider gives no answer, the connection also loses its access, and the node
  closes it. The node cannot know that the token is still valid, so it fails closed. An outage of
  the identity provider thus ends the access of every connection with an opaque token within 300
  seconds. When the identity provider is back, the client can connect and send `AUTH` again.

The node also keeps a refused answer, for 30 seconds. During that time it refuses the same token
without a request to the identity provider. Thus a token that becomes active later can stay
refused for up to 30 seconds. The node does not keep a refused token that is longer than 1024
bytes.

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

A user of the ACL file never opens the admin routes.
Without admin credentials, the node answers 401.

Each refused bearer token counts as a failed attempt for its source address, together with the
failed `AUTH` attempts from that address (see [Failed attempts](#failed-attempts)). When an
address is over the limit, the node answers 401 to every bearer token from it until the window
ends. It does not check the token, and it sends no request to the identity provider. An admin
certificate still passes. The admin port takes the source address from the TCP connection, so
every client behind one proxy shares one limit.

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
| TLS modes, client certificates, ACL users, admin certificates | Always present |
| OAuth 2.0 tokens and `[[issuer]]` | `oauth2` |
| Introspection | `oauth2-introspection` |
| `[kubernetes]` | `k8s` and `oauth2` |

The TLS backend feature (`tls-aws-lc`, `tls-ring`, or `tls-openssl`) supplies the cryptography for
all TLS in the node. This includes the client listener, the admin port, HTTPS to identity providers,
the OTLP exporter, and the node-to-node QUIC transport. The build has one provider for all of them.
QUIC uses only its TLS 1.3 cipher suites. Token signatures use `aws-lc-rs` with `tls-aws-lc`. With
`tls-ring` or `tls-openssl`, token signatures use the RustCrypto crates, because the JWT library has
no `ring` or OpenSSL backend.

The trust root feature (`roots-system` or `roots-webpki`) supplies the roots for HTTPS to an
identity provider without `ca_file`.

## Flag reference

| Flag | Default | Effect |
| --- | --- | --- |
| `--tls-mode` | `node` | The client listener TLS mode: `node` or `lb`. |
| `--tls-cert` | required | The PEM certificate chain of the client listener and the admin port. |
| `--tls-key` | required | The PEM private key that matches `--tls-cert`. |
| `--tls-client-ca` | none | The CA for client certificates, or for the load balancer in `lb` mode. |
| `--tls-client-auth` | `optional` | `required` refuses clients without a certificate in `node` mode. |
| `--lb-trusted-source` | none | A network that the load balancer connects from. Repeat it for more. |
| `--lb-identity-tlv` | none | A PROXY v2 TLV type from `0xE0` to `0xEF` that holds the client identity. |
| `--auth-policy` | required without `--acl-file` | The client authentication policy. See [Authentication policy](#authentication-policy). |
| `--auth-config` | none | The TOML file with issuers, rules, and admin identities. |
| `--acl-file` | none | A file with users, passwords, and rules, for the `password` factor. The node reloads it after a change. |
| `--dev-insecure-oauth2` | off | Allows `http` issuers, and admin bearer tokens on a plaintext admin port. Development only. |
| `--auth-clock-skew-secs` | 60 | The clock skew allowed on token times. |
| `--auth-max-failures-per-connection` | 5 | Failed attempts that close a connection. |
| `--auth-max-failures-per-source` | 20 | Failed attempts per source in one window. An IPv6 source is a /64 network. |
| `--auth-failure-window-secs` | 60 | The window of the per-source limit. |
| `--auth-timeout-secs` | 30 | Seconds that a new connection gets to meet the policy. `0` turns the deadline off. The node applies a change from the settings file. |
| `--max-clients` | 10000 | Open client connections on both client listeners. `0` means no limit. The node applies a change from the settings file. |
| `--admin-tls` | off | Serves the admin port over TLS. |
| `--tls-reload-secs` | 10 | Seconds between checks of the certificate files and the ACL file. `0` turns the reload off. |

### Certificate rotation

The node checks the files of `--tls-cert`, `--tls-key`, and `--tls-client-ca` every
`--tls-reload-secs` seconds. If their content changes, the node loads the new files. New
connections on the client, redirect, and admin ports then use them. Open connections keep the
certificate of their handshake. You do not have to restart the node.

If the new files do not load, the node logs a warning and keeps its current certificates. For
example, this happens when the key does not match the certificate. The node tries the files again
at each check, until they load. In Kubernetes, the files of a
mounted Secret change after the Secret changes, usually within a minute or two.

The node reads the `--auth-config` file once at startup. To change it, restart the node.

### ACL file rotation

The node checks the file of `--acl-file` every `--tls-reload-secs` seconds. If its content
changes, the node reads the whole file. You do not have to restart the node.

The node loads the same content that it compared. If the file has an error, the node logs a
warning and keeps its users. The node tries the file again at each check, until it loads. After a
good reload, these rules apply:

- If the user of a connection is gone or `off`, the connection loses its login. Its next command
  returns `-NOAUTH`, and the node closes the connection. Valkey also closes the connections of a
  user that `ACL LOAD` removes.
- The new rules apply to the next command of each connection.
- A changed password keeps the connections that are logged in.

With an `any` policy, another factor can still authenticate the connection, for example a client
certificate. Then the connection stays open and keeps the access of that factor after the user is
gone.

Outside Kubernetes, write the new file next to the old file, and then rename it over the old file.
A file that you write in place can load half written. Then a rule at the end of a line can be
missing.

In Kubernetes, change the Secret of `aclFile.secretName`. The mounted file changes after the Secret
changes, usually within a minute or two. Each node reads its own copy, so for a short time some
nodes have the old file and some have the new file.

To change a password without a failed login, give the user two passwords for a while:

1. Add the new hash to the line of the user, next to the old hash.
2. Change the Secret, and wait until every node has the new file.
3. Give each client the new password.
4. Remove the old hash from the line.
5. Change the Secret again.
