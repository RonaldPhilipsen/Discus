#!/usr/bin/env python3
"""Summarize bench-k8s.py result files with the CPU counters of the server pods.

usage: bench-k8s-cpu.py RESULTS.jsonl [RESULTS.jsonl ...]

Prints one line for each system, workload and variant, as medians over the runs: throughput,
the spread of the throughput from the slowest to the fastest run, p50, p99 and p99.9 in ms, the
cores that all server pods used together, the server CPU per operation in microseconds, the
throttled periods of all server pods, and the client cores. Then it prints where the keys were
after the runs, from the "after-runs" check of bench-k8s.py.

CPU per operation is the median server cores divided by the median throughput. The cgroup
counters leave out softirq time, so the real CPU use is higher.
"""
import collections
import json
import statistics
import sys

NOT_SERVER = ("client", "toolbox")


def server_cores(r):
    return sum(v["usage_seconds"] for p, v in r["cfs"].items() if p not in NOT_SERVER) / r["duration"]


def throttled(r):
    return sum(v["cfs_throttled_periods"] for p, v in r["cfs"].items() if p not in NOT_SERVER)


def client_cores(r):
    return r["cfs"].get("client", {}).get("usage_seconds", 0) / r["duration"]


def main(paths):
    rows = collections.defaultdict(list)
    placement = {}
    for path in paths:
        for line in open(path):
            r = json.loads(line)
            if "error" in r:
                print("ERROR", r.get("system"), str(r["error"])[:300])
            elif r.get("verify") == "after-runs":
                placement[r["system"]] = r["nodes"]
            elif "workload" in r:
                rows[(r["system"], r["workload"], r["variant"])].append(r)
    for key in sorted(rows):
        rs = rows[key]

        def med(f):
            return statistics.median(f(r) for r in rs)

        ops, cores = med(lambda r: r["ops"]), med(server_cores)
        lo, hi = min(r["ops"] for r in rs), max(r["ops"] for r in rs)
        print(f"{key[0]:34} {key[1]:16} {key[2]:9} runs={len(rs)} ops={ops:>9.0f} "
              f"spread={lo:.0f}-{hi:.0f} "
              f"p50={med(lambda r: r['p50_ms']):6.2f} p99={med(lambda r: r['p99_ms']):6.2f} "
              f"p999={med(lambda r: r.get('p999_ms', float('nan'))):6.2f} "
              f"cores={cores:5.2f} us/op={cores / ops * 1e6:5.1f} throttled={med(throttled):5.0f} "
              f"client={med(client_cores):4.2f}")
    for system, nodes in sorted(placement.items()):
        print(f"\n{system}, after the runs:")
        for n in nodes:
            mib = int(n.get("used_memory", 0) or 0) // 2**20
            role = f" {n['role']}" if n.get("role") else ""
            print(f"  {n['ip']}:{n['port']}{role} keys={n['dbsize']} mem={mib}MiB")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
