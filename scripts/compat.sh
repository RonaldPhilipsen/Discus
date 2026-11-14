#!/usr/bin/env bash
# Drive redis-cli through every supported and every unsupported command once.
#
# usage: scripts/compat.sh [host:port]
#
# With no argument the script builds the node, starts it on COMPAT_PORT (default 16379) with a
# test certificate, runs the checks, and stops the node. With an argument it checks that node and
# leaves it running. Every client uses TLS, so then set COMPAT_CA to the CA file of that node.
# The node of the script also has a redirect listener on COMPAT_REDIRECT_PORT (default 16380) for
# the cluster client check. For a node that you name, set COMPAT_REDIRECT=host:port to run it.
# FLUSHDB and FLUSHALL run only when COMPAT_ALLOW_FLUSH=1, because they delete data.
# The database checks need a node with 3 databases whose database 2 holds no other keys.
set -u
cd "$(dirname "$0")/.."

node_pid=""
certs=""
cleanup() {
  [ -n "$node_pid" ] && kill "$node_pid" 2>/dev/null
  [ -n "$certs" ] && rm -rf "$certs"
}
trap cleanup EXIT

if [ $# -ge 1 ]; then
  target=$1
  ca=${COMPAT_CA:?set COMPAT_CA to the CA file of the node}
else
  port=${COMPAT_PORT:-16379}
  redirect_port=${COMPAT_REDIRECT_PORT:-16380}
  certs=$(mktemp -d)
  scripts/dev-cert.sh "$certs"
  ca="$certs/ca.pem"
  cargo build -q -p discus || exit 1
  "${CARGO_TARGET_DIR:-target}/debug/discus" --listen "127.0.0.1:$port" --redirect-listen "127.0.0.1:$redirect_port" \
    --admin-listen "127.0.0.1:$((port + 1000))" --tls-cert "$certs/cert.pem" \
    --tls-key "$certs/key.pem" --auth-policy none --databases 3 >/dev/null 2>&1 &
  node_pid=$!
  target="127.0.0.1:$port"
  export COMPAT_ALLOW_FLUSH=1
  COMPAT_REDIRECT="127.0.0.1:$redirect_port"
  for _ in $(seq 1 50); do
    redis-cli --tls --cacert "$ca" -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 && break
    sleep 0.1
  done
fi
host=${target%:*}
port=${target##*:}
p="compat:$$"
fail=0

cli() { timeout 5 redis-cli --tls --cacert "$ca" -h "$host" -p "$port" "$@" 2>&1; }

# ok NAME EXTENDED-REGEX ARGS...: the reply must match the regex.
ok() {
  local name=$1 expect=$2 out
  shift 2
  out=$(cli "$@")
  if grep -q -E -- "$expect" <<<"$out"; then
    echo "ok   $name"
  else
    echo "FAIL $name: expected /$expect/, got: $out"
    fail=1
  fi
}

# absent NAME TEXT ARGS...: the reply must not contain the text.
absent() {
  local name=$1 text=$2 out
  shift 2
  out=$(cli "$@")
  if grep -q -F -- "$text" <<<"$out"; then
    echo "FAIL $name: $text in: $out"
    fail=1
  else
    echo "ok   $name"
  fi
}

# unsupported NAME ARGS...: the reply must be the Discus error.
unsupported() {
  local name=$1 out
  out=$(cli "$@")
  if grep -q -F -- "command not supported by Discus" <<<"$out"; then
    echo "ok   unsupported $name"
  else
    echo "FAIL unsupported $name: got: $out"
    fail=1
  fi
}

# Connection
ok PING '^PONG$' PING
ok ECHO '^hi$' ECHO hi
ok HELLO 'proto' HELLO 3
ok "CLIENT ID" '^[0-9]+$' CLIENT ID
ok "CLIENT SETNAME" '^OK$' CLIENT SETNAME compat
ok "CLIENT GETNAME" '.*' CLIENT GETNAME
ok INFO 'redis_version:7\.2\.0' INFO
ok "CONFIG GET" 'maxmemory' CONFIG GET maxmemory
ok "COMMAND COUNT" '^[0-9]+$' COMMAND COUNT
ok AUTH 'called without any password|^OK$' AUTH x

# Strings
ok SET '^OK$' SET "$p:s" hello
ok GET '^hello$' GET "$p:s"
ok APPEND '^11$' APPEND "$p:s" " world"
ok STRLEN '^11$' STRLEN "$p:s"
ok GETEX 'hello world' GETEX "$p:s" PERSIST
ok "SET NX" '^$|nil' SET "$p:s" other NX
ok "SET GET" 'hello world' SET "$p:s" "hello world" GET
ok SET '^OK$' SET "$p:n" 10
ok INCR '^11$' INCR "$p:n"
ok DECR '^10$' DECR "$p:n"
ok INCRBY '^15$' INCRBY "$p:n" 5
ok DECRBY '^12$' DECRBY "$p:n" 3
ok INCRBYFLOAT '^12\.5$' INCRBYFLOAT "$p:n" 0.5
ok "SET float" '^OK$' SET "$p:f" 0.1
ok "INCRBYFLOAT 0.1 + 0.2" '^0\.3$' INCRBYFLOAT "$p:f" 0.2
ok "INCRBYFLOAT 1e30" '^1000000000000000000000000000000\.300048828125$' INCRBYFLOAT "$p:f" 1e30
ok MSET '^OK$' MSET "$p:a" 1 "$p:b" 2
ok MGET '1' MGET "$p:a" "$p:b"
ok TYPE '^string$' TYPE "$p:s"
ok EXISTS '^1$' EXISTS "$p:s"
ok EXPIRE '^1$' EXPIRE "$p:s" 100
ok TTL '^(100|99)$' TTL "$p:s"
ok PTTL '^[0-9]+$' PTTL "$p:s"
ok PERSIST '^1$' PERSIST "$p:s"
ok PEXPIRE '^1$' PEXPIRE "$p:s" 100000
ok EXPIREAT '^1$' EXPIREAT "$p:s" 4102444800
ok PEXPIREAT '^1$' PEXPIREAT "$p:s" 4102444800000
cli SET "$p:rn1" 1 >/dev/null
ok RENAME '^OK$' RENAME "$p:rn1" "$p:rn2"
ok GETDEL '^1$' GETDEL "$p:a"
ok DEL '^1$' DEL "$p:b"
cli DEL "$p:rn2" >/dev/null
ok "RENAME missing source" 'no such key' RENAME "$p:rnmissing" "$p:rnx"
cli SET "$p:rnself" 1 >/dev/null
ok "RENAME same key" '^OK$' RENAME "$p:rnself" "$p:rnself"
cli SET "$p:rnsrc" new >/dev/null
cli SET "$p:rndst" old >/dev/null
ok "RENAME overwrite" '^OK$' RENAME "$p:rnsrc" "$p:rndst"
ok "RENAME overwrite GET" '^new$' GET "$p:rndst"
cli SET "$p:rnttl" v EX 100 >/dev/null
ok "RENAME with TTL" '^OK$' RENAME "$p:rnttl" "$p:rnttl2"
ok "RENAME keeps TTL" '^(100|99)$' TTL "$p:rnttl2"
cli DEL "$p:rnself" "$p:rndst" "$p:rnttl2" >/dev/null

# Hashes
ok HSET '^1$' HSET "$p:h" f 1
ok HSETNX '^1$' HSETNX "$p:h" g 2
ok HGET '^1$' HGET "$p:h" f
ok HMGET '1' HMGET "$p:h" f g
ok HGETALL 'f' HGETALL "$p:h"
ok HDEL '^1$' HDEL "$p:h" g
ok HEXISTS '^1$' HEXISTS "$p:h" f
ok HLEN '^1$' HLEN "$p:h"
ok HINCRBY '^5$' HINCRBY "$p:h" f 4
ok HKEYS '^f$' HKEYS "$p:h"
ok HVALS '^5$' HVALS "$p:h"

# Sets
ok SADD '^2$' SADD "$p:set" a b
ok SISMEMBER '^1$' SISMEMBER "$p:set" a
ok SMISMEMBER '1' SMISMEMBER "$p:set" a z
ok SCARD '^2$' SCARD "$p:set"
ok SMEMBERS 'a' SMEMBERS "$p:set"
ok SRANDMEMBER '^[ab]$' SRANDMEMBER "$p:set"
ok SPOP '^[ab]$' SPOP "$p:set"
ok SREM '^1$' SREM "$p:set" a b

# Sorted sets
ok ZADD '^2$' ZADD "$p:z" 1 a 2 b
ok ZSCORE '^1$' ZSCORE "$p:z" a
ok ZMSCORE '1' ZMSCORE "$p:z" a b
ok ZCARD '^2$' ZCARD "$p:z"
ok ZRANGE 'a' ZRANGE "$p:z" 0 -1
ok ZREVRANGE 'b' ZREVRANGE "$p:z" 0 -1
ok ZRANGEBYSCORE 'a' ZRANGEBYSCORE "$p:z" -inf +inf
ok ZINCRBY '^2$' ZINCRBY "$p:z" 1 a
ok ZCOUNT '^2$' ZCOUNT "$p:z" 0 5
ok ZREM '^1$' ZREM "$p:z" a

# Counter keys
c="cnt:$p"
ok "INCR counter" '^1$' INCR "$c:n"
ok "INCRBY counter" '^5$' INCRBY "$c:n" 4
ok "INCR EX counter" '^1$' INCR "$c:w" EX 100
ok "TTL counter" '^(100|99)$' TTL "$c:w"
ok "INCR EX keeps the window" '^2$' INCR "$c:w" EX 5
ok "TTL window kept" '^(100|99)$' TTL "$c:w"
ok "GET counter" '^5$' GET "$c:n"
ok "TYPE counter" '^string$' TYPE "$c:n"
absent "SCAN skips counters" "$c" SCAN 0 MATCH "$c:*" COUNT 1000
ok "HSET counter" 'only string and expiry' HSET "$c:h" f v
ok "MGET mixed" 'cannot be mixed' MGET "$c:n" "$p:s"
ok "DEL counter" '^2$' DEL "$c:n" "$c:w"

# Databases
d="$p:db"
ok "SELECT 2" '^OK$' SELECT 2
ok "SELECT 3" 'out of range' SELECT 3
ok "SET in database 1" '^OK$' -n 1 SET "$d" one
ok "GET in database 0" '^$|nil' -n 0 GET "$d"
ok "MOVE to database 2" '^1$' -n 1 MOVE "$d" 2
ok "COPY to database 1" '^1$' -n 2 COPY "$d" "$d" DB 1
ok "SWAPDB" 'not allowed in cluster mode' SWAPDB 0 1
ok "DBSIZE in database 2" '^1$' -n 2 DBSIZE
ok "SCAN in database 2" "^$d\$" -n 2 SCAN 0 MATCH "$p:*" COUNT 1000
absent "SCAN in database 2 skips database 0" "$p:s" -n 2 SCAN 0 MATCH "$p:*" COUNT 1000
ok "RANDOMKEY in database 2" "^$d\$" -n 2 RANDOMKEY
ok "INFO keyspace" 'db2:keys=' INFO keyspace
if [ "${COMPAT_ALLOW_FLUSH:-0}" = 1 ]; then
  ok "FLUSHDB in database 2" '^OK$' -n 2 FLUSHDB
  ok "FLUSHDB keeps database 1" '^one$' -n 1 GET "$d"
  ok "DBSIZE after FLUSHDB" '^0$' -n 2 DBSIZE
fi
cli -n 1 DEL "$d" >/dev/null
cli -n 2 DEL "$d" >/dev/null

# Keys
ok SCAN "$p" SCAN 0 MATCH "$p:*" COUNT 1000
ok RANDOMKEY '.' RANDOMKEY
ok DBSIZE '^[0-9]+$' DBSIZE
if [ "${COMPAT_ALLOW_FLUSH:-0}" = 1 ]; then
  ok FLUSHDB '^OK$' FLUSHDB
  ok FLUSHALL '^OK$' FLUSHALL
else
  echo "skip FLUSHDB and FLUSHALL (set COMPAT_ALLOW_FLUSH=1 to run them)"
  cli DEL "$p:s" "$p:n" "$p:h" "$p:set" "$p:z" >/dev/null
fi

# Cluster
ok "CLUSTER INFO" 'cluster_state:ok' CLUSTER INFO
ok "CLUSTER SLOTS" '16383' CLUSTER SLOTS
ok "CLUSTER KEYSLOT" '^12182$' CLUSTER KEYSLOT foo
ok READONLY '^OK$' READONLY
ok READWRITE '^OK$' READWRITE
ok ASKING '^OK$' ASKING
ok "COMMAND INFO" '^get$' COMMAND INFO get
if [ -n "${COMPAT_REDIRECT:-}" ]; then
  rhost=${COMPAT_REDIRECT%:*}
  rport=${COMPAT_REDIRECT##*:}
  rcli() { timeout 5 redis-cli --tls --cacert "$ca" -c -h "$rhost" -p "$rport" "$@" 2>&1; }
  rcli SET "$p:cc" cluster-client >/dev/null
  out=$(rcli GET "$p:cc")
  if [ "$out" = cluster-client ]; then
    echo "ok   redis-cli -c SET and GET"
  else
    echo "FAIL redis-cli -c SET and GET: got: $out"
    fail=1
  fi
  rcli DEL "$p:cc" >/dev/null
else
  echo "skip redis-cli -c (set COMPAT_REDIRECT=host:port to run it)"
fi

# Unsupported
for name in MULTI EXEC DISCARD WATCH UNWATCH EVAL EVALSHA SCRIPT FUNCTION PUBLISH PUBSUB \
  BLPOP BRPOP BLMOVE BRPOPLPUSH XADD SORT RENAMENX SUNIONSTORE SINTERSTORE \
  SDIFFSTORE BITOP BITFIELD PFADD PFCOUNT GEOADD MIGRATE WAIT DEBUG; do
  unsupported "$name" "$name" x
done
unsupported SUBSCRIBE SUBSCRIBE ch
unsupported PSUBSCRIBE PSUBSCRIBE 'ch*'

if [ "$fail" = 0 ]; then echo "compat: all checks passed"; else echo "compat: FAILED"; fi
exit "$fail"
