#!/usr/bin/env bash
# Benchmark smoke: redis-benchmark against a release build of one node. Not a gate.
set -eu
cd "$(dirname "$0")/.."
port=${BENCH_PORT:-16380}
certs=$(mktemp -d)
scripts/dev-cert.sh "$certs"
cargo build -q --release -p discus
target/release/discus --listen "127.0.0.1:$port" --admin-listen "127.0.0.1:$((port + 1000))" \
  --tls-cert "$certs/cert.pem" --tls-key "$certs/key.pem" --auth-policy none >/dev/null 2>&1 &
pid=$!
trap 'kill $pid 2>/dev/null; rm -rf "$certs"' EXIT
tls=(--tls --cacert "$certs/ca.pem")
for _ in $(seq 1 50); do
  redis-cli "${tls[@]}" -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 && break
  sleep 0.1
done
redis-benchmark "${tls[@]}" -h 127.0.0.1 -p "$port" -t set,get -n 200000 -P 16 -q
