import json, statistics, sys
order = ["discus-3","redis-sentinel-3","valkey-sentinel-3","redis-cluster-noreplica-3","valkey-cluster-noreplica-3",
         "redis-cluster-replica-3","valkey-cluster-replica-3","discus-1","redis-1","valkey-1"]
wl = ["get","set","mixed-1w9r","mixed-1w9r-p16"]
rows = {}
verify = {}
for l in open(sys.argv[1]):
    d = json.loads(l)
    if "error" in d:
        print("ERROR", d["system"], d["error"][:300]); continue
    if "verify" in d:
        verify[(d["system"], d["verify"])] = d; continue
    rows.setdefault((d["system"], d["workload"], d["variant"]), []).append(d)
for w in wl:
    print(f"\n### {w}\n\n| Setup | Ops/s (median) | Spread min to max | p50 ms | p99 ms | p99.9 ms | Runs |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for s in order:
        for v in ("primary","all-nodes"):
            r = rows.get((s, w, v))
            if not r: continue
            ops = [x["ops"] for x in r]
            med = lambda k: statistics.median(x[k] for x in r)
            name = s + (" (reads on all 3 nodes)" if v == "all-nodes" else "")
            print(f"| {name} | {statistics.median(ops):,.0f} | {min(ops):,.0f} to {max(ops):,.0f} | {med('p50_ms'):.2f} | {med('p99_ms'):.2f} | {med('p999_ms'):.2f} | {len(r)} |")
    miss = sum(x["misses"] for (s,ww,v),rr in rows.items() if ww==w for x in rr)
    print(f"\nGET misses per second summed over all runs: {miss:.1f}")
print("\n### Data placement after the runs\n")
for s in order:
    d = verify.get((s,"after-runs"))
    if not d: continue
    parts = [f"{n['ip']}:{n['port']} {n.get('role','?')} keys={n['dbsize']} mem={int(n.get('used_memory',0) or 0)//2**20}MiB" for n in d["nodes"]]
    print(f"- {s}: " + "; ".join(parts))
