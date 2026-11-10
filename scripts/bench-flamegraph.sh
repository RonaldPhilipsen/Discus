#!/usr/bin/env bash
# Records a CPU flamegraph of one Discus node under redis-benchmark load.
#
# Needs perf and inferno (cargo install inferno) on PATH. With kernel.perf_event_paranoid at 2 or
# higher, perf records user-space samples only, so the graph leaves out the time in the kernel.
# bench-compare.sh reports that time.
set -eu
cd "$(dirname "$0")/.."

server_cpus=${SERVER_CPUS:-0-7}
client_cpus=${CLIENT_CPUS:-8-15}
client_threads=${CLIENT_THREADS:-8}
port=${BENCH_PORT:-16390}
seconds=${SECONDS_TO_RECORD:-20}
out=${OUT:-target/bench-compare}
target=target/bench-fp

mkdir -p "$out"
RUSTFLAGS="-C force-frame-pointers=yes" CARGO_PROFILE_RELEASE_DEBUG=line-tables-only \
  CARGO_TARGET_DIR=$target cargo build -q --release -p discus

taskset -c "$server_cpus" $target/release/discus \
  --listen "127.0.0.1:$port" \
  --admin-listen "127.0.0.1:$((port + 1000))" \
  --cluster-listen "127.0.0.1:$((port + 2000))" \
  --lease-provider none --bootstrap-deadline-ms 500 >"$out/flamegraph-discus.log" 2>&1 &
pid=$!
load=
trap 'kill $pid $load 2>/dev/null || true' EXIT

for _ in $(seq 1 300); do
  [ "$(redis-cli -h 127.0.0.1 -p "$port" SET bench:ready 1 2>/dev/null)" = OK ] && break
  sleep 0.1
done

record() {
  local label=$1
  shift
  taskset -c "$client_cpus" redis-benchmark -h 127.0.0.1 -p "$port" --threads "$client_threads" \
    -q -l -t set,get -d 256 -r 100000 "$@" >/dev/null &
  load=$!
  sleep 3
  # The default event and plain -g both need more privileges than user-space profiling has.
  perf record -q -e cycles:u --user-callchains -F 997 -g -p "$pid" -o "$out/$label.perf" -- sleep "$seconds"
  kill "$load" 2>/dev/null || true
  wait "$load" 2>/dev/null || true
  load=
  perf script -i "$out/$label.perf" 2>/dev/null | inferno-collapse-perf >"$out/$label.folded"
  inferno-flamegraph --title "discus $label" <"$out/$label.folded" >"$out/$label.svg"
  echo "$out/$label.svg"
}

record c250 -c 250
record c50-p16 -c 50 -P 16
