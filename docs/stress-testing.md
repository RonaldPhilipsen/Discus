# Stress testing

This page describes how to stress test a Discus release candidate on a Kubernetes cluster. The
test has three parts: a smoke test, a load test with several clients, and a fault test. A fault
test restarts and kills pods while clients write.

## Prerequisites

You need these items:

- A machine with `kubectl` access to the test cluster. Run every command on this machine.
- A cluster with at least three schedulable nodes. You need them to test the loss of a node. With
  fewer nodes, two pods share a node, and the test does not cover the loss of a whole node.
- A Discus image that the cluster can pull.

Do not run this test on a cluster that serves production traffic without a limit on the client
pods. The clients and the Discus pods share the CPU of the nodes with other workloads.

## Deploy the candidate

1. Create the namespace `discus-rc`.
2. Apply the manifests from [Deploy on Kubernetes](cluster.md#deploy-on-kubernetes) in that
   namespace. Set the image to the candidate. Add the Role of the cluster lease.
3. Issue a node certificate with the URI SAN `spiffe://cluster.local/ns/discus-rc/sa/discus`.
4. Set `--cluster-san` to the same value.
5. Wait for the rollout:

   ```sh
   kubectl -n discus-rc rollout status deploy/discus --timeout=150s
   ```

## Start the client pods

Create four pods named `stress-0` to `stress-3`. Each pod uses the image `redis:8-alpine`, runs
`sleep 1800`, and has a CPU limit of 2 and a memory limit of 256Mi.

The clients use the Service `discus`, so Kubernetes spreads the connections over the pods. Each
connection stays open for the whole run, because Discus forwards a command to the owner of its key.

## Smoke test

Run a few commands of each family through one client, and compare each reply with Redis:

```sh
kubectl -n discus-rc exec stress-0 -- redis-cli -h discus SET a 1
kubectl -n discus-rc exec stress-0 -- redis-cli -h discus INCRBYFLOAT f 0.1
kubectl -n discus-rc exec stress-0 -- redis-cli -h discus HSET h a 1
```

[Compatibility](compatibility.md) lists the supported commands.

## Load test

Run each stage on all four clients at the same time. Replace `<args>` with the arguments of the
stage:

```sh
for i in 0 1 2 3; do
  kubectl -n discus-rc exec stress-$i -- redis-benchmark -h discus --csv <args> > out-$i.csv &
done
wait
```

| Stage | Arguments |
| --- | --- |
| 50 connections per client | `-t set,get -d 256 -r 100000 -n 200000 -c 50` |
| 250 connections per client | `-t set,get -d 256 -r 100000 -n 300000 -c 250` |
| 1000 connections per client | `-t set,get -d 256 -r 100000 -n 300000 -c 1000` |
| Pipeline of 16 commands | `-t set,get -d 256 -r 100000 -n 1000000 -c 50 -P 16` |
| Counters on 1000 keys | `-n 100000 -c 50 -r 1000 INCR cnt:s:__rand_int__` |
| One hot counter | `-n 50000 -c 50 INCR cnt:hot` |

Delete the key `cnt:hot` before the last stage. For each stage, add the requests per second of
the four clients, and take the highest p99 latency of the four clients.

During each stage, record the load with `kubectl -n discus-rc top pods -l app=discus`.

The load test passes when all of these conditions are true:

- No client reports an error.
- `cnt:hot` is exactly 200000 after the last stage.
- No Discus pod restarts.
- The logs of the Discus pods show no new `ERROR` line during the load test.

## Fault test

The fault test shows if Discus keeps acknowledged data through a rolling restart and a pod kill.
Run a script in a client pod that does these tasks until a stop file exists:

- It writes `SET k:<i> v<i>`, and it records each key that returned `OK`.
- It increments the counter `cnt:rc`, and it records each reply in one of three groups: success,
  refusal with `has no leader`, and any other reply.
- It reads random recorded keys, and it records each hit, miss, and wrong value.

If the script starts a new `redis-cli` for each command, add a pause of about 20 ms to each loop.
Without the pause, the client runs out of local ports and reports `Address not available`. These
errors come from the client, so do not count them against Discus.

Follow these steps:

1. Start the script, and wait 20 seconds.
2. Start a rolling restart:

   ```sh
   kubectl -n discus-rc rollout restart deploy/discus
   kubectl -n discus-rc rollout status deploy/discus --timeout=300s
   ```

3. Wait 20 seconds.
4. Kill one Discus pod:

   ```sh
   kubectl -n discus-rc delete pod <pod> --grace-period=0 --force
   kubectl -n discus-rc rollout status deploy/discus --timeout=180s
   ```

5. Wait 30 seconds, and then stop the script.
6. Read every recorded key, and read `cnt:rc`.

The fault test passes when all of these conditions are true:

- No read returns a wrong value.
- The counter is at least the number of successes. The counter is at most the number of successes
  plus the number of replies other than success and refusal.
- Every recorded key is still readable, apart from the misses that a forced kill can cause.
  [Drain](cluster.md#drain) describes when a key can become a miss.
- The logs do not contain the line `dropped from the epoch, joining again`.

A short burst of `Connection refused` during a rolling restart is expected. A draining pod stops
accepting connections before Kubernetes removes it from the Service.

## Clean up

When the test ends, delete the namespace:

```sh
kubectl delete namespace discus-rc
```
