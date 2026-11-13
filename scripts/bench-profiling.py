#!/usr/bin/env python3
"""One Discus node pinned to one core, profiling on or off. Prints ops/s and CPU us per op.

usage: bench-profiling.py <binary|docker:IMAGE> <on|off> <label> [extra discus args...]

Environment:
  OUT          append one JSON line per workload to this file
  WORK_DIR     directory for the node log, its TMPDIR and perf data (default: a new temp dir)
  NODE_CPU     the core that the node runs on (default 4)
  CLIENT_CPUS  the cores that redis-benchmark runs on (default 16-23)
  PERF=1       record `perf record` of the node during the GET run (PERFF, PERFG set options)

Profiling on sends the profiles to a local sink that discards them.
Needs: redis-benchmark, redis-cli, taskset, and docker for a docker: image.
"""
import json
import http.server
import os
import re
import socketserver
import subprocess
import sys
import tempfile
import threading
import time

WORK = os.environ.get("WORK_DIR") or tempfile.mkdtemp(prefix="bench-profiling-")
os.makedirs(WORK, exist_ok=True)
print(f"node log and perf data: {WORK}", file=sys.stderr)
binary, mode, label, *extra = sys.argv[1:]
PORT, ADMIN, PYRO = 16390, 17390, 14040
NODE_CPU = os.environ.get("NODE_CPU", "4")
CLIENT_CPUS = os.environ.get("CLIENT_CPUS", "16-23")


class Sink(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


socketserver.TCPServer.allow_reuse_address = True
srv = socketserver.ThreadingTCPServer(("127.0.0.1", PYRO), Sink)
threading.Thread(target=srv.serve_forever, daemon=True).start()

image = binary[len("docker:"):] if binary.startswith("docker:") else None
args = ["docker", "run", "--rm", "--name", "bench-profiling", "--network", "host",
        "--tmpfs", "/tmp", image] if image else ["taskset", "-c", NODE_CPU, binary]
if image and image.startswith("docker.io/valkey/"):
    args += ["valkey-server", "--port", str(PORT), "--bind", "127.0.0.1", "--save", "",
             "--appendonly", "no", *extra]
else:
    args += ["--listen", f"127.0.0.1:{PORT}", "--admin-listen", f"127.0.0.1:{ADMIN}", *extra]
if mode == "on":
    args += ["--pyroscope-url", f"http://127.0.0.1:{PYRO}"]
env = dict(os.environ, TMPDIR=tempfile.mkdtemp(dir=WORK))
node = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(f"{WORK}/{label}.log", "w"), env=env)
try:
    for _ in range(100):
        if subprocess.run(["redis-cli", "-p", str(PORT), "PING"], capture_output=True).stdout.strip() == b"PONG":
            break
        time.sleep(0.1)
    else:
        sys.exit(f"node did not start, see {WORK}/{label}.log")
    pid = node.pid
    if image:
        pid = int(subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", "bench-profiling"],
                                 capture_output=True, text=True).stdout)
        if open(f"/proc/{pid}/comm").read().strip() == "tini":
            pid = int(open(f"/proc/{pid}/task/{pid}/children").read().split()[0])
        subprocess.run(["taskset", "-a", "-cp", NODE_CPU, str(pid)], check=True, capture_output=True)
    subprocess.run(["redis-benchmark", "-p", str(PORT), "-t", "set", "-n", "500000", "-r", "100000",
                    "-q"], capture_output=True)  # prefill and warm up

    def cpu():
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")

    for test in ["get", "set"]:
        perf = None
        if os.environ.get("PERF") == "1" and test == "get":
            perf = subprocess.Popen(["perf", "record", "-F", os.environ.get("PERFF", "199"), *os.environ.get("PERFG", "-g").split(), "-p", str(pid),
                                     "-o", f"{WORK}/{label}.perf.data"],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(0.5)
        c0, t0 = cpu(), time.time()
        out = subprocess.run(["taskset", "-c", CLIENT_CPUS, "redis-benchmark", "-p", str(PORT), "-t", test,
                              "-n", "1500000", "-r", "100000", "-c", "64", "--threads", "4", "-q"],
                             capture_output=True, text=True).stdout
        c1, t1 = cpu(), time.time()
        if perf:
            perf.send_signal(2)
            perf.wait()
        ops = float(re.findall(r"([\d.]+) requests per second", out)[-1])
        p50 = re.findall(r"p50=([\d.]+)", out)
        n = 1500000
        print(f"{label:20s} {mode:3s} {test:3s} ops/s={ops:9.0f} cpu_us/op={(c1 - c0) / n * 1e6:6.2f} "
              f"node_cores={(c1 - c0) / (t1 - t0):4.2f} p50={p50[-1] if p50 else '?'}", flush=True)
        if os.environ.get("OUT"):
            with open(os.environ["OUT"], "a") as f:
                f.write(json.dumps({"build": label, "profiling": mode, "workload": test, "ops": ops,
                                    "cpu_us_per_op": (c1 - c0) / n * 1e6, "node_cores": (c1 - c0) / (t1 - t0),
                                    "p50_ms": float(p50[-1]) if p50 else None, "requests": n,
                                    "start": t0, "end": t1}) + "\n")
finally:
    if image:
        subprocess.run(["docker", "rm", "-f", "bench-profiling"], capture_output=True)
    node.terminate()
    node.wait()
    srv.shutdown()
