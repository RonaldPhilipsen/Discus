#!/usr/bin/env bash
# Drive redis-cli through every supported and every unsupported command once.
#
# usage: scripts/compat.sh [host:port]
#
# With no argument the script builds the node, starts it on COMPAT_PORT (default 16379), runs the
# checks, and stops the node. With an argument it checks that node and leaves it running.
# FLUSHDB and FLUSHALL run only when COMPAT_ALLOW_FLUSH=1, because they delete data.
set -u
cd "$(dirname "$0")/.."

node_pid=""
cleanup() { [ -n "$node_pid" ] && kill "$node_pid" 2>/dev/null; }
trap cleanup EXIT

if [ $# -ge 1 ]; then
  target=$1
else
  port=${COMPAT_PORT:-16379}
  cargo build -q -p discus || exit 1
  target/debug/discus --listen "127.0.0.1:$port" --admin-listen "127.0.0.1:$((port + 1000))" \
    >/dev/null 2>&1 &
  node_pid=$!
  target="127.0.0.1:$port"
  export COMPAT_ALLOW_FLUSH=1
  for _ in $(seq 1 50); do
    redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 && break
    sleep 0.1
  done
fi
host=${target%:*}
port=${target##*:}
p="compat:$$"
fail=0

cli() { timeout 5 redis-cli -h "$host" -p "$port" "$@" 2>&1; }

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
ok SELECT '^OK$' SELECT 0
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
ok "INCRBYFLOAT 1e30" '^1000000000000000000024696061952$' INCRBYFLOAT "$p:f" 1e30
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
ok GETDEL '^1$' GETDEL "$p:a"
ok DEL '^1$' DEL "$p:b"

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
ok CLUSTER 'not a Redis Cluster' CLUSTER INFO

# Unsupported
for name in MULTI EXEC DISCARD WATCH UNWATCH EVAL EVALSHA SCRIPT FUNCTION PUBLISH PUBSUB \
  BLPOP BRPOP BLMOVE BRPOPLPUSH XADD SORT RENAME RENAMENX SUNIONSTORE SINTERSTORE \
  SDIFFSTORE BITOP BITFIELD PFADD PFCOUNT GEOADD MIGRATE WAIT DEBUG; do
  unsupported "$name" "$name" x
done
unsupported SUBSCRIBE SUBSCRIBE ch
unsupported PSUBSCRIBE PSUBSCRIBE 'ch*'

if [ "$fail" = 0 ]; then echo "compat: all checks passed"; else echo "compat: FAILED"; fi
exit "$fail"
