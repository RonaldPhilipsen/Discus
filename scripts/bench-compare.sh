#!/usr/bin/env bash
# Compares one Discus node with Redis and Valkey under the same redis-benchmark load.
#
# Servers run on SERVER_CPUS and clients on CLIENT_CPUS. The defaults fit a 16-core, two-CCD
# machine: one CCD for the servers and the other for the clients. Results go to OUT/results.csv.
# Needs: podman, redis-benchmark, redis-cli, python3.
set -eu
cd "$(dirname "$0")/.."

server_cpus=${SERVER_CPUS:-0-7}
client_cpus=${CLIENT_CPUS:-8-15}
client_threads=${CLIENT_THREADS:-8}
io_threads=${IO_THREADS:-8}
port=${BENCH_PORT:-16390}
# redis-benchmark measures time in 250 ms steps, so a run must last several seconds.
requests=${REQUESTS:-3000000}
reps=${REPS:-3}
out=${OUT:-target/bench-compare}
redis_image=${REDIS_IMAGE:-docker.io/library/redis:8}
valkey_image=${VALKEY_IMAGE:-docker.io/valkey/valkey:9}
servers=${SERVERS:-discus redis redis-io valkey valkey-io}
persistence=(--save "" --appendonly no)

name=discus-bench-$$
pid=
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  podman rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$out"
results=$out/results.csv
echo server,stage,test,rep,rps,p50_ms,p99_ms,user_s,sys_s >"$results"

cargo build -q --release -p discus

container() {
  local image=$1 binary=$2
  shift 2
  taskset -c "$server_cpus" podman run -d --name "$name" --network host "$image" \
    "$binary" --port "$port" --bind 127.0.0.1 "${persistence[@]}" "$@" >/dev/null
  pid=
  for _ in $(seq 1 100); do
    pid=$(podman inspect --format '{{.State.Pid}}' "$name" 2>/dev/null || true)
    [ -n "$pid" ] && [ "$pid" != 0 ] && break
    sleep 0.1
  done
}

start() {
  case $1 in
    discus)
      taskset -c "$server_cpus" target/release/discus \
        --listen "127.0.0.1:$port" \
        --admin-listen "127.0.0.1:$((port + 1000))" \
        --cluster-listen "127.0.0.1:$((port + 2000))" \
        --lease-provider none --bootstrap-deadline-ms 500 >"$out/discus.log" 2>&1 &
      pid=$!
      ;;
    redis) container "$redis_image" redis-server ;;
    redis-io) container "$redis_image" redis-server --io-threads "$io_threads" ;;
    valkey) container "$valkey_image" valkey-server ;;
    valkey-io) container "$valkey_image" valkey-server --io-threads "$io_threads" ;;
  esac
  for _ in $(seq 1 300); do
    if [ "$(redis-cli -h 127.0.0.1 -p "$port" SET bench:ready 1 2>/dev/null)" = OK ]; then
      # A container's main process can be tini, with the server as its child.
      child=$(pgrep -P "$pid" -f -- '-server' | head -1 || true)
      [ -n "$child" ] && pid=$child
      return 0
    fi
    sleep 0.1
  done
  echo "$1 did not accept writes" >&2
  exit 1
}

stop() {
  kill "$pid" 2>/dev/null || true
  [ "$1" = discus ] && { wait "$pid" 2>/dev/null || true; }
  podman rm -f "$name" >/dev/null 2>&1 || true
  pid=
}

# Prints the user and system CPU seconds of the server, for all of its threads.
cpu() {
  python3 -I - "$pid" <<'EOF'
import os, sys
fields = open(f"/proc/{sys.argv[1]}/stat").read().rsplit(")", 1)[1].split()
tick = os.sysconf("SC_CLK_TCK")
print(int(fields[11]) / tick, int(fields[12]) / tick)
EOF
}

run_stage() {
  local server=$1 stage=$2 n=$3 test rep before after line
  shift 3
  for test in set get; do
    for rep in $(seq 1 "$reps"); do
      before=$(cpu)
      line=$(taskset -c "$client_cpus" redis-benchmark -h 127.0.0.1 -p "$port" \
        --threads "$client_threads" --csv -t "$test" -d 256 -r 100000 -n "$n" "$@" | tail -1)
      after=$(cpu)
      python3 -I - "$server" "$stage" "$test" "$rep" "$line" $before $after >>"$results" <<'EOF'
import csv, sys
server, stage, test, rep, line, u0, s0, u1, s1 = sys.argv[1:]
row = next(csv.reader([line]))
print(",".join([server, stage, test, rep, row[1], row[4], row[6],
                f"{float(u1) - float(u0):.2f}", f"{float(s1) - float(s0):.2f}"]))
EOF
    done
  done
}

for server in $servers; do
  echo "== $server"
  start "$server"
  run_stage "$server" c50 "$requests" -c 50
  run_stage "$server" c250 "$requests" -c 250
  run_stage "$server" c1000 "$requests" -c 1000
  run_stage "$server" c50-p16 "$((requests * 5))" -c 50 -P 16
  stop "$server"
done

python3 -I - "$results" <<'EOF' | tee "$(dirname "$results")/summary.txt"
import collections, csv, statistics, sys

runs = collections.defaultdict(list)
for row in csv.DictReader(open(sys.argv[1])):
    runs[row["stage"], row["test"], row["server"]].append(row)

def median(rows, field):
    return statistics.median(float(r[field]) for r in rows)

print(f"{'stage':8} {'test':4} {'server':10} {'rps':>10} {'p50 ms':>8} {'p99 ms':>8} "
      f"{'user s':>7} {'sys s':>7} {'sys %':>6} {'rps range':>9}")
for (stage, test, server), rows in runs.items():
    rps = [float(r["rps"]) for r in rows]
    user, system = median(rows, "user_s"), median(rows, "sys_s")
    share = 100 * system / (user + system) if user + system else 0
    spread = 100 * (max(rps) - min(rps)) / statistics.median(rps)
    print(f"{stage:8} {test:4} {server:10} {statistics.median(rps):10.0f} "
          f"{median(rows, 'p50_ms'):8.3f} {median(rows, 'p99_ms'):8.3f} "
          f"{user:7.2f} {system:7.2f} {share:6.1f} {spread:8.1f}%")
EOF
