#!/usr/bin/env python3
"""Compare Discus, Redis, and Valkey on Kubernetes with 1 CPU and 2 GiB per node."""
import argparse
import json
import os
import pathlib
import subprocess
import tempfile
import threading
import time

CTX = "home-ops"
NS = "discus-bench"
NODE = "k8s-neo-alpha"
DISCUS_TAG = "0.2.2"
# The published chart of the same version as the image.
CHART = "oci://ghcr.io/ronaldphilipsen/charts/discus"
IMAGES = {"redis": "docker.io/library/redis:8", "valkey": "docker.io/valkey/valkey:9"}
MEMTIER = "docker.io/redislabs/memtier_benchmark:2.2.1"
KEYS = 1_000_000
VALUE = 256

WORKLOADS = {
    "get": ("0:1", 1),
    "set": ("1:0", 1),
    "mixed-1w9r": ("1:9", 1),
    "mixed-1w9r-p16": ("1:9", 16),
}

# name -> (kind, engine, nodes). A Discus setup can name its image tag as a fourth item.
SYSTEMS = {
    "discus-6": ("discus", "discus", 6, "0.2.3-rc.1"),
    "discus-6-rc2": ("discus", "discus", 6, "0.2.3-rc.2"),
    "discus-6-0.2.2": ("discus", "discus", 6, "0.2.2"),
    "discus-6-0.3.0-rc.2": ("discus", "discus", 6, "0.3.0-rc.2"),
    "discus-3-0.3.0-rc.2": ("discus", "discus", 3, "0.3.0-rc.2"),
    "discus-1-0.3.0-rc.2": ("discus", "discus", 1, "0.3.0-rc.2"),
    "discus-6-0.4.1-rc.1": ("discus", "discus", 6, "0.4.1-rc.1"),
    "discus-3-0.4.1-rc.1": ("discus", "discus", 3, "0.4.1-rc.1"),
    "discus-1-0.4.1-rc.1": ("discus", "discus", 1, "0.4.1-rc.1"),
    "discus-6-0.5.0-rc.1": ("discus", "discus", 6, "0.5.0-rc.1"),
    "discus-3-0.5.0-rc.1": ("discus", "discus", 3, "0.5.0-rc.1"),
    "discus-1-0.5.0-rc.1": ("discus", "discus", 1, "0.5.0-rc.1"),
    "discus-cluster-6-0.5.0-rc.1": ("discus-cluster", "discus", 6, "0.5.0-rc.1"),
    "discus-cluster-3-0.5.0-rc.1": ("discus-cluster", "discus", 3, "0.5.0-rc.1"),
    "discus-cluster-1-0.5.0-rc.1": ("discus-cluster", "discus", 1, "0.5.0-rc.1"),
    "discus-6-0.5.0-rc.2": ("discus", "discus", 6, "0.5.0-rc.2"),
    "discus-3-0.5.0-rc.2": ("discus", "discus", 3, "0.5.0-rc.2"),
    "discus-1-0.5.0-rc.2": ("discus", "discus", 1, "0.5.0-rc.2"),
    "discus-cluster-6-0.5.0-rc.2": ("discus-cluster", "discus", 6, "0.5.0-rc.2"),
    "discus-cluster-3-0.5.0-rc.2": ("discus-cluster", "discus", 3, "0.5.0-rc.2"),
    "discus-cluster-1-0.5.0-rc.2": ("discus-cluster", "discus", 1, "0.5.0-rc.2"),
    "discus-6-1.0.2-rc.1": ("discus", "discus", 6, "1.0.2-rc.1"),
    "discus-3-1.0.2-rc.1": ("discus", "discus", 3, "1.0.2-rc.1"),
    "discus-1-1.0.2-rc.1": ("discus", "discus", 1, "1.0.2-rc.1"),
    "discus-cluster-6-1.0.2-rc.1": ("discus-cluster", "discus", 6, "1.0.2-rc.1"),
    "discus-cluster-3-1.0.2-rc.1": ("discus-cluster", "discus", 3, "1.0.2-rc.1"),
    "discus-cluster-1-1.0.2-rc.1": ("discus-cluster", "discus", 1, "1.0.2-rc.1"),
    "redis-sentinel-6": ("sentinel", "redis", 6),
    "valkey-sentinel-6": ("sentinel", "valkey", 6),
    "redis-cluster-noreplica-6": ("cluster0", "redis", 6),
    "valkey-cluster-noreplica-6": ("cluster0", "valkey", 6),
    "redis-cluster-replica-6": ("cluster1", "redis", 6),
    "valkey-cluster-replica-6": ("cluster1", "valkey", 6),
    "redis-cluster-replica2-6": ("cluster2", "redis", 6),
    "valkey-cluster-replica2-6": ("cluster2", "valkey", 6),
    "discus-3": ("discus", "discus", 3),
    "redis-sentinel-3": ("sentinel", "redis", 3),
    "valkey-sentinel-3": ("sentinel", "valkey", 3),
    "redis-cluster-noreplica-3": ("cluster0", "redis", 3),
    "valkey-cluster-noreplica-3": ("cluster0", "valkey", 3),
    "redis-cluster-replica-3": ("cluster1", "redis", 3),
    "valkey-cluster-replica-3": ("cluster1", "valkey", 3),
    "redis-cluster-replica2-3": ("cluster2", "redis", 3),
    "valkey-cluster-replica2-3": ("cluster2", "valkey", 3),
    "discus-1": ("discus", "discus", 1),
    "redis-1": ("standalone", "redis", 1),
    "valkey-1": ("standalone", "valkey", 1),
}


def run(args, check=True, input_=None, timeout=900):
    p = subprocess.run(args, capture_output=True, text=True, input=input_, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"{args}\nstdout: {p.stdout}\nstderr: {p.stderr}")
    return p.stdout


def kc(*a, **k):
    return run(["kubectl", "--context", CTX, "-n", NS, *a], **k)


def apply(obj):
    kc("apply", "-f", "-", input_=json.dumps(obj))


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def until(fn, what, limit=240):
    end = time.time() + limit
    last = None
    while time.time() < end:
        try:
            r = fn()
            if r:
                return r
        except (RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError) as e:
            last = e
        time.sleep(2)
    raise RuntimeError(f"timeout waiting for {what}: {last}")


def tool_pod(name, image, cpu, mem):
    return {
        "apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "labels": {"bench": "tool"}},
        "spec": {
            "nodeSelector": {"kubernetes.io/hostname": NODE},
            "terminationGracePeriodSeconds": 1,
            "containers": [{
                "name": "c", "image": image, "command": ["sleep", "infinity"],
                "resources": {"requests": {"cpu": cpu, "memory": mem}, "limits": {"cpu": cpu, "memory": mem}},
            }],
        },
    }


def ensure_tools():
    run(["kubectl", "--context", CTX, "create", "ns", NS], check=False)
    apply(tool_pod("client", MEMTIER, "8", "4Gi"))
    apply(tool_pod("toolbox", IMAGES["redis"], "200m", "128Mi"))
    kc("wait", "--for=condition=Ready", "pod/client", "pod/toolbox", "--timeout=300s")


def cli(ip, *a, port=6379, check=True):
    return kc("exec", "toolbox", "--", "redis-cli", "--raw", "-h", ip, "-p", str(port), *a, check=check, timeout=60)


def server_sts(name, engine, n, script):
    return [
        {"apiVersion": "v1", "kind": "Service",
         "metadata": {"name": name, "labels": {"bench": "server"}},
         "spec": {"clusterIP": "None", "publishNotReadyAddresses": True, "selector": {"app": name},
                  "ports": [{"port": 6379}]}},
        {"apiVersion": "apps/v1", "kind": "StatefulSet",
         "metadata": {"name": name, "labels": {"bench": "server"}},
         "spec": {
             "serviceName": name, "replicas": n, "podManagementPolicy": "Parallel",
             "selector": {"matchLabels": {"app": name}},
             "template": {"metadata": {"labels": {"app": name, "bench": "server"}}, "spec": {
                 "nodeSelector": {"kubernetes.io/hostname": NODE},
                 "terminationGracePeriodSeconds": 1,
                 "containers": [{
                     "name": "s", "image": IMAGES[engine], "command": ["sh", "-c", script],
                     "env": [{"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}],
                     "resources": {"requests": {"cpu": "1", "memory": "2Gi"},
                                   "limits": {"cpu": "1", "memory": "2Gi"}},
                 }],
             }},
         }},
    ]


def common(mem):
    return (f"--bind 0.0.0.0 --protected-mode no --save '' --appendonly no "
            f"--maxmemory {mem} --maxmemory-policy allkeys-lru")


def pods(name):
    out = json.loads(kc("get", "pods", "-l", f"app={name}", "-o", "json"))
    return [(p["metadata"]["name"], p["status"].get("podIP")) for p in
            sorted(out["items"], key=lambda p: p["metadata"]["name"])]


def wait_pods(name, n):
    until(lambda: len(pods(name)) == n and all(ip for _, ip in pods(name)), f"{name} pods")
    kc("wait", "--for=condition=Ready", "pod", "-l", f"app={name}", "--timeout=300s")
    return pods(name)


def deploy_standalone(name, engine):
    s = f"exec {engine}-server {common('1536mb')}"
    for o in server_sts(name, engine, 1, s):
        apply(o)
    ip = wait_pods(name, 1)[0][1]
    until(lambda: cli(ip, "PING").strip() == "PONG", "ping")
    return {"targets": [(ip, 6379)], "cluster": False, "ips": [ip]}


def deploy_sentinel(name, engine, n):
    host0 = f"{name}-0.{name}.{NS}.svc.cluster.local"
    s = f"""
ORD=${{HOSTNAME##*-}}
ROLE=""
[ "$ORD" != 0 ] && ROLE="--replicaof {host0} 6379"
{engine}-server {common('1536mb')} --replica-announce-ip $POD_IP $ROLE &
until {engine}-cli -h {host0} ping | grep -q PONG; do sleep 1; done
cat > /tmp/s.conf <<EOF
port 26379
bind 0.0.0.0
protected-mode no
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip $POD_IP
sentinel monitor m {host0} 6379 2
sentinel down-after-milliseconds m 5000
sentinel failover-timeout m 30000
EOF
{engine}-server /tmp/s.conf --sentinel &
wait
"""
    for o in server_sts(name, engine, n, s):
        apply(o)
    ps = wait_pods(name, n)
    ips = [ip for _, ip in ps]

    def healthy():
        info = cli(ips[0], "INFO", "replication")
        if f"connected_slaves:{n - 1}" not in info or info.count("state=online") != n - 1:
            return False
        out = cli(ips[0], "SENTINEL", "master", "m", port=26379)
        f = out.split()
        d = dict(zip(f[::2], f[1::2]))
        return d.get("num-slaves") == str(n - 1) and d.get("num-other-sentinels") == str(n - 1)
    until(healthy, "sentinel healthy")
    return {"targets": [(ips[0], 6379)], "cluster": False, "ips": ips,
            "replica_targets": [(ip, 6379) for ip in ips]}


def deploy_cluster(name, engine, replicas, n):
    """Each pod runs one primary on 6379 and `replicas` replica processes on 6380 and up. Replica k
    of the primary on pod i runs on pod i + k + 1, so the copies of a slot are on different pods."""
    mem = {0: "1536mb", 1: "700mb", 2: "500mb"}[replicas]
    srv = (f"{engine}-server {common(mem)} --cluster-enabled yes --cluster-node-timeout 5000 "
           f"--cluster-announce-ip $POD_IP")
    if replicas:
        s = " & ".join(f"{srv} --port {6379 + k} --cluster-config-file /tmp/n{k}.conf"
                       for k in range(replicas + 1)) + " & wait"
    else:
        s = f"exec {srv} --port 6379 --cluster-config-file /tmp/n0.conf"
    for o in server_sts(name, engine, n, s):
        apply(o)
    ps = wait_pods(name, n)
    ips = [ip for _, ip in ps]
    pod0 = ps[0][0]
    ex = lambda *a: kc("exec", pod0, "--", f"{engine}-cli", *a, timeout=120)
    until(lambda: all(cli(ip, "PING").strip() == "PONG" for ip in ips), "cluster nodes up")
    ex("--cluster", "create", *[f"{ip}:6379" for ip in ips], "--cluster-yes")
    until(lambda: "cluster_state:ok" in cli(ips[0], "CLUSTER", "INFO"), "cluster ok")
    if replicas:
        ids = [cli(ip, "CLUSTER", "MYID").strip() for ip in ips]
        for k in range(1, replicas + 1):
            for i, ip in enumerate(ips):
                host = ips[(i + k) % n]
                ex("--cluster", "add-node", f"{host}:{6379 + k}", f"{ip}:6379",
                   "--cluster-slave", "--cluster-master-id", ids[i])

        def linked():
            for ip in ips:
                info = cli(ip, "INFO", "replication")
                online = info.count("state=online")
                if f"connected_slaves:{replicas}" not in info or online < replicas:
                    return False
            return "cluster_state:ok" in cli(ips[0], "CLUSTER", "INFO")
        until(linked, "cluster replicas online")
    return {"targets": [(ips[0], 6379)], "cluster": True, "ips": ips}


def deploy_discus(name, n, tag, tmp, cluster_mode=False, pyroscope_url=None):
    """With cluster_mode, memtier runs as a Redis Cluster client against the redirect listener.
    With pyroscope_url, the nodes push CPU profiles. The image needs the pyroscope feature."""
    release = "discus"
    run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
         "-days", "2", "-subj", "/CN=bench CA", "-keyout", f"{tmp}/ca.key", "-out", f"{tmp}/ca.crt"])
    run(["openssl", "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
         "-subj", "/CN=bench", "-keyout", f"{tmp}/tls.key", "-out", f"{tmp}/n.csr"])
    pathlib.Path(f"{tmp}/ext").write_text("extendedKeyUsage=serverAuth,clientAuth\n")
    run(["openssl", "x509", "-req", "-in", f"{tmp}/n.csr", "-CA", f"{tmp}/ca.crt", "-CAkey", f"{tmp}/ca.key",
         "-CAcreateserial", "-days", "2", "-extfile", f"{tmp}/ext", "-out", f"{tmp}/tls.crt"])
    kc("delete", "secret", "discus-tls", "--ignore-not-found")
    kc("create", "secret", "generic", "discus-tls", f"--from-file=ca.crt={tmp}/ca.crt",
       f"--from-file=tls.crt={tmp}/tls.crt", f"--from-file=tls.key={tmp}/tls.key")
    run(["helm", "--kube-context", CTX, "-n", NS, "upgrade", "--install", release, CHART, "--version", tag,
         "--wait", "--timeout", "5m",
         "--set", f"replicaCount={n}", "--set", f"image.tag={tag}",
         "--set", "cluster.tls.secretName=discus-tls",
         "--set", "resources.requests.cpu=1", "--set", "resources.limits.cpu=1",
         "--set", "resources.requests.memory=2Gi", "--set", "resources.limits.memory=2Gi",
         "--set-string", f"nodeSelector.kubernetes\\.io/hostname={NODE}",
         "--set", "podDisruptionBudget.enabled=false",
         "--set", f"clusterClients.enabled={'true' if cluster_mode else 'false'}",
         *(["--set-string", f"extraArgs[0]=--pyroscope-url={pyroscope_url}"] if pyroscope_url else [])],
        timeout=400)
    out = json.loads(kc("get", "pods", "-l", "app.kubernetes.io/name=discus", "-o", "json"))
    ips = [p["status"]["podIP"] for p in out["items"] if p["metadata"].get("deletionTimestamp") is None]

    def members():
        for ip in ips:
            info = cli(ip, "INFO", "discus")
            d = dict(l.split(":", 1) for l in info.splitlines() if ":" in l)
            if d.get("discus_members", "").strip() != str(n):
                return False
        return True
    until(members, "discus members")
    svc = kc("get", "svc", release, "-o", "jsonpath={.spec.clusterIP}").strip()
    # Counters switch to Raft after the 30 s dwell. Waiting avoids that change during a run.
    if n >= 3:
        time.sleep(40)
    if cluster_mode:
        until(lambda: cli(ips[0], "CLUSTER", "SLOTS", port=6380).strip(), "cluster slots")
        return {"targets": [(ips[0], 6380)], "cluster": True, "ips": ips, "service": (svc, 6379)}
    # The client connects to every pod, so each node gets the same share of the connections. Through
    # the Service, the connections spread at random over the pods.
    return {"targets": [(ip, 6379) for ip in ips], "cluster": False, "ips": ips, "service": (svc, 6379)}


def teardown():
    run(["helm", "--kube-context", CTX, "-n", NS, "uninstall", "discus", "--wait"], check=False, timeout=200)
    kc("delete", "statefulset,service", "-l", "bench=server", "--ignore-not-found", "--wait=true", timeout=200)
    kc("delete", "pod", "-l", "bench=server", "--ignore-not-found", "--wait=true", timeout=200)


def memtier(target, cluster, extra):
    a = ["memtier_benchmark", "-s", target[0], "-p", str(target[1]), "--hide-histogram",
         "--key-prefix", "key:", "--key-minimum", "1", "--key-maximum", str(KEYS), "-d", str(VALUE),
         "--distinct-client-seed"]
    if cluster:
        a.append("--cluster-mode")
    return a + extra


def prefill(info):
    log("prefill")
    args = memtier(info["targets"][0], info["cluster"],
                   ["-t", "1", "-c", "1", "--pipeline", "64", "--ratio", "1:0",
                    "--key-pattern", "P:P", "-n", "allkeys"])
    kc("exec", "client", "--", *args, timeout=600)
    time.sleep(15)


def one(info, targets, ratio, pipeline, duration, threads, clients):
    pct = "50,99,99.9"
    cmds = []
    for i, t in enumerate(targets):
        args = memtier(t, info["cluster"], [
            "-t", str(threads), "-c", str(clients), "--pipeline", str(pipeline), "--ratio", ratio,
            "--key-pattern", "R:R", "--test-time", str(duration), "--print-percentiles", pct,
            "--json-out-file", f"/tmp/o{i}.json"])
        cmds.append(" ".join(args) + " >/dev/null 2>/tmp/e" + str(i))
    script = " & ".join(cmds) + " & wait; " + "; ".join(
        f"echo @@@; cat /tmp/o{i}.json" for i in range(len(targets)))
    out = kc("exec", "client", "--", "sh", "-c", script, timeout=duration + 120)
    parts = [json.loads(p) for p in out.split("@@@\n")[1:]]
    return parts


def summarize(parts):
    ops = lat = 0.0
    p50 = p99 = p999 = 0.0
    hits = misses = sets = gets = moved = ask = 0.0
    for j in parts:
        t = j["ALL STATS"]["Totals"]
        o = t["Ops/sec"]
        ops += o
        lat += t["Average Latency"] * o
        pc = t["Percentile Latencies"]
        p50 = max(p50, pc["p50.00"])
        p99 = max(p99, pc["p99.00"])
        p999 = max(p999, pc["p99.90"])
        g = j["ALL STATS"].get("Gets", {})
        hits += g.get("Hits/sec", 0)
        misses += g.get("Misses/sec", 0)
        sets += j["ALL STATS"].get("Sets", {}).get("Ops/sec", 0)
        gets += g.get("Ops/sec", 0)
        moved += t.get("MOVED/sec", 0)
        ask += t.get("ASK/sec", 0)
    return {"ops": ops, "avg_ms": lat / ops if ops else None, "p50_ms": p50, "p99_ms": p99,
            "p999_ms": p999, "hits": hits, "misses": misses, "sets_s": sets, "gets_s": gets,
            "moved_s": moved, "ask_s": ask}


def verify(name, kind, info, phase, out):
    rec = {"system": name, "verify": phase, "nodes": []}
    ports = [6379 + k for k in range({"cluster1": 2, "cluster2": 3}.get(kind, 1))]
    for ip in info["ips"]:
        for port in ports:
            r = {"ip": ip, "port": port, "dbsize": cli(ip, "DBSIZE", port=port, check=False).strip()}
            for sec in ("memory", "replication"):
                txt = cli(ip, "INFO", sec, port=port, check=False)
                for k in ("used_memory", "role", "master_repl_offset", "slave_repl_offset", "connected_slaves"):
                    m = [l.split(":", 1)[1].strip() for l in txt.splitlines() if l.startswith(k + ":")]
                    if m:
                        r[k] = m[0]
            rec["nodes"].append(r)
    throttles = {}
    if not kind.startswith("discus"):
        for pod_name, _ in pods(rec["system"].replace("-noreplica", "n").replace("-replica", "r").replace("-sentinel", "s")[:40]):
            throttles[pod_name] = kc("exec", pod_name, "--", "cat", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu.max", check=False, timeout=30)
    rec["cpu_stat"] = throttles
    with open(out, "a") as f:
        f.write(json.dumps(rec) + "\n")
    log(f"verify {phase}: " + json.dumps(rec["nodes"]))


CFS = ("container_cpu_cfs_periods_total", "container_cpu_cfs_throttled_periods_total",
       "container_cpu_cfs_throttled_seconds_total", "container_cpu_usage_seconds_total")


def cfs():
    """The CFS counters of each pod in the namespace, from the kubelet cAdvisor metrics.
    Some images have no shell, so the pod cgroup cannot be read with exec."""
    txt = run(["kubectl", "--context", CTX, "get", "--raw",
               f"/api/v1/nodes/{NODE}/proxy/metrics/cadvisor"], check=False, timeout=60)
    out = {}
    for line in txt.splitlines():
        name = line.split("{", 1)[0]
        # The pod cgroup has an empty image. The pause container has container="" too.
        if (name not in CFS or f'namespace="{NS}"' not in line or 'container=""' not in line
                or 'image=""' not in line):
            continue
        pod = line.split('pod="', 1)[1].split('"', 1)[0]
        out.setdefault(pod, {})[name.removeprefix("container_cpu_").removesuffix("_total")] = \
            float(line.split("} ", 1)[1].split()[0])
    return out


def cfs_delta(before, after):
    return {pod: {k: round(v - before.get(pod, {}).get(k, 0.0), 3) for k, v in m.items()}
            for pod, m in after.items() if pod != "toolbox"}


def top_sample(delay, into):
    def w():
        time.sleep(delay)
        into.append(kc("top", "pods", "--no-headers", check=False, timeout=30))
    th = threading.Thread(target=w, daemon=True)
    th.start()
    return th


def run_system(name, args, tmp, out):
    kind, engine, n, *rest = SYSTEMS[name]
    log(f"=== {name}: deploy")
    teardown()
    short = name.replace("-noreplica", "n").replace("-replica", "r").replace("-sentinel", "s")[:40]
    if kind == "standalone":
        info = deploy_standalone(short, engine)
    elif kind == "sentinel":
        info = deploy_sentinel(short, engine, n)
    elif kind in ("cluster0", "cluster1", "cluster2"):
        info = deploy_cluster(short, engine, int(kind[-1]), n)
    else:
        info = deploy_discus(short, n, rest[0] if rest else DISCUS_TAG, tmp, kind == "discus-cluster",
                             args.pyroscope_url)
    prefill(info)
    verify(name, kind, info, "after-prefill", out)
    for rep in range(args.reps):
        for w, (ratio, pipe) in WORKLOADS.items():
            # Every setup gets args.connections per node, so all setups have the same total.
            # memtier in Cluster mode opens its connections to every primary. A Discus setup has
            # one memtier process per pod. A Sentinel primary takes the total of all nodes.
            targets = info["targets"]
            if info["cluster"]:
                t, c = args.threads, args.connections / args.threads
            elif len(targets) > 1:
                t = max(1, args.threads // len(targets))
                c = args.connections / t
            else:
                t, c = args.threads, args.connections * n / args.threads
            variants = [("primary", targets, t, max(1, round(c)))]
            if w == "get" and info.get("replica_targets"):
                spread = info["replica_targets"]
                t = max(1, args.threads // len(spread))
                variants.append(("all-nodes", spread, t, max(1, round(args.connections / t))))
            for variant, targets, t, c in variants:
                samples = []
                th = top_sample(args.duration * 0.6, samples)
                before = cfs()
                # The time window selects this run in Pyroscope.
                start_ms = time.time_ns() // 1_000_000
                parts = one(info, targets, ratio, pipe, args.duration, t, c)
                end_ms = time.time_ns() // 1_000_000
                throttle = cfs_delta(before, cfs())
                th.join()
                rec = {"system": name, "workload": w, "variant": variant, "rep": rep, "threads": t,
                       "clients": c, "start_ms": start_ms, "end_ms": end_ms,
                       "duration": args.duration, "top": samples[0] if samples else "",
                       "cfs": throttle, **summarize(parts)}
                with open(out, "a") as f:
                    f.write(json.dumps(rec) + "\n")
                log(f"{name} {w}/{variant} rep{rep}: {rec['ops']:.0f} ops/s "
                    f"p50={rec['p50_ms']:.2f} p99={rec['p99_ms']:.2f} misses={rec['misses']:.1f}")
                time.sleep(3)
    verify(name, kind, info, "after-runs", out)
    teardown()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--systems", default=",".join(SYSTEMS))
    ap.add_argument("--out", required=True)
    ap.add_argument("--threads", type=int, default=8,
                    help="memtier threads in all. With several targets, each target gets an equal part.")
    ap.add_argument("--connections", type=int, default=96,
                    help="client connections per node. Every setup gets this number times its node count.")
    ap.add_argument("--pyroscope-url",
                    help="Discus nodes push CPU profiles here, for example "
                         "http://pyroscope.observability.svc.cluster.local:4040. "
                         "The image must be built with --build-arg FEATURES=pyroscope.")
    args = ap.parse_args()
    ensure_tools()
    with tempfile.TemporaryDirectory() as tmp:
        for name in args.systems.split(","):
            try:
                run_system(name, args, tmp, args.out)
            except Exception as e:
                log(f"FAILED {name}: {e}")
                with open(args.out, "a") as f:
                    f.write(json.dumps({"system": name, "error": str(e)}) + "\n")
    log("done")


if __name__ == "__main__":
    main()
