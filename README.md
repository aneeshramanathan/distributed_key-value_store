# distributed_key-value_store

A linearizable, fault-tolerant key/value store replicated across 3–5 nodes with
the **Raft** consensus algorithm, written from scratch in Go. It is the same
basic design as etcd (which Kubernetes runs on), Consul and CockroachDB's
replication layer: the data stays correct and available as long as a majority
of nodes are up and can talk to each other.

The interesting part is the **test harness**. A simulated network drops,
delays, reorders and partitions messages while nodes are crashed and
restarted. Random concurrent workloads run on top of all that, and every
recorded history is checked for linearizability with
[Porcupine](https://github.com/anishathalye/porcupine). Because the checker
must also reject bad histories, there are tests for that too.

## What's implemented

| Requirement | Where |
|---|---|
| **Leader election.** Nodes vote for a leader and elect a new one automatically when it fails. Randomized timeouts, plus **PreVote** and **CheckQuorum** so partitions don't cause needless elections. | [`raft/raft.go`](raft/raft.go) |
| **Log replication.** The leader appends every write to its log and copies it to followers. An entry commits once a majority stores it. Uses fast conflict back-off and batching. | [`raft/raft.go`](raft/raft.go) |
| **Persistence and recovery.** Each node has a write-ahead log on disk with CRC-checked records, fsync with group commit, and torn-write repair. A restarted node rejoins with its state. | [`raft/filestorage.go`](raft/filestorage.go) |
| **Snapshotting.** The log is compacted once it reaches a size limit. A lagging follower is caught up with `InstallSnapshot`. | [`raft/raft.go`](raft/raft.go), [`kv/server.go`](kv/server.go) |
| **Client API.** `GET`, `PUT` and `DELETE` over HTTP, linearizable (every read sees the latest committed write), with exactly-once retried writes. | [`node/node.go`](node/node.go), [`client/client.go`](client/client.go) |
| **Adversarial test harness.** Simulated network with drop, delay, reorder, partition and crash. | [`simnet/simnet.go`](simnet/simnet.go) |
| **Linearizability checking** of random workloads under failures, using Porcupine. | [`kv/linearizability`](kv/linearizability), [`kv/kv_test.go`](kv/kv_test.go), [`node/node_test.go`](node/node_test.go) |
| **Benchmarks.** Throughput, p50/p99 latency and failover time on 3- and 5-node clusters. | [`cmd/kvbench`](cmd/kvbench/main.go) |

## Results

Measured with `go run ./cmd/kvbench -nodes 3,5 -duration 10s -trials 7`. All
nodes ran on one laptop (Intel i7-10750H, 6 cores / 12 threads, Windows 11,
NVMe SSD) over loopback HTTP, sharing a single disk. The workload was 32
concurrent clients, 1,000 keys, 100-byte values and 50% reads. **Every
operation, reads included, goes through the Raft log and is fsynced on a
majority before it is acknowledged.**

### Throughput and latency (fsync on)

| Nodes | Throughput | p50 | p99 | GET p50 / p99 | PUT p50 / p99 |
|---|---|---|---|---|---|
| 3 | **8,583 ops/s** | 3.1 ms | 12.6 ms | 3.1 / 12.8 ms | 3.1 / 12.4 ms |
| 5 | **7,908 ops/s** | 3.4 ms | 17.4 ms | 3.4 / 17.4 ms | 3.4 / 17.4 ms |

With `-nosync`, which isolates the cost of durability, the numbers are
14,180 / 13,347 ops/s and p50 1.0 ms. That makes fsync roughly 40% of the
throughput cost. Group commit is what keeps it that low: one fsync covers
every entry appended since the last one.

### Leader failover

The leader is killed while a probe client writes continuously.

| Nodes | New leader elected (median / max) | Writes unavailable (median / max) | Restarted node catches up (median) |
|---|---|---|---|
| 3 | 304 ms / 404 ms | 346 ms / 417 ms | 51 ms |
| 5 | 305 ms / 364 ms | 346 ms / 402 ms | 44 ms |

Failover is bounded below by the election timeout (250–500 ms, heartbeat
50 ms). Followers must first notice that the leader is silent, then win a
pre-vote and a vote. Write unavailability is that, plus the client finding the
new leader. "Catch-up" is the time for the killed node to restart from its
data directory, replay its WAL and receive what it missed.

## Correctness testing

```
go test ./...            # whole suite (~5 min)
go test -race ./...      # what CI runs, on Linux
go test ./raft -run Figure8 -v
KVTRACE=1 go test ./kv -run AllFaults -v   # watch partitions, terms and progress
```

### The simulated network ([`simnet`](simnet/simnet.go))

Nodes talk only through an in-memory network that serializes every message
with gob, so they share no memory. Per test, the network can:

- **drop** 10% of requests and 10% of replies,
- **delay** every message by 0–27 ms, which reorders concurrent messages,
- **reorder badly**: hold back 2/3 of replies for 200 ms – 1.7 s, so they
  arrive after much newer traffic,
- **partition** nodes, by enabling or disabling individual directed links,
- **crash** a node, which discards its in-flight replies even if the handler
  later returns. The node is restarted from a copy of its persisted state, so
  anything a "zombie" instance writes after the crash is lost.

### Raft tests ([`raft/raft_test.go`](raft/raft_test.go))

There are 24 scenario tests. Each node's applied entries are recorded, and the
harness fails the test the instant two nodes apply different commands at the
same index, or a node applies out of order.

- **Election:** initial election; re-election after leader loss; no leader
  without a quorum; 7-node cluster with random 3-node disconnections.
- **Replication:** agreement with follower and leader failures; nothing
  commits without a majority; concurrent `Start`s; a stale leader rejoining;
  50-entry divergent logs repaired by fast back-off; bounds on RPC counts.
- **Persistence:** whole-cluster and partial crash/restart loops.
- **Figure 8** (the Raft paper's counterexample): random leader crashes with
  persistence. The *unreliable* variant runs 1,000 rounds on a network that
  drops, delays and badly reorders messages.
- **Churn:** clients submit while nodes are randomly crashed, restarted and
  partitioned. Afterwards, every value a client saw committed must be in the
  final log.
- **Snapshots:** lagging followers caught up via `InstallSnapshot`, with
  disconnections, crashes and an unreliable network; whole-cluster restarts
  from snapshot + log tail.
- **Disk:** WAL round-trip, suffix overwrite, torn-tail and CRC-corruption
  recovery, compaction, and a real restart from disk.

### Linearizability tests ([`kv/kv_test.go`](kv/kv_test.go), [`node/node_test.go`](node/node_test.go))

Concurrent clients issue random `Get`/`Put`/`Delete` operations on a small key
space, so there is lots of contention. Every operation is recorded with
invocation and response timestamps, while a nemesis injects faults every 1–2
seconds. At the end the cluster is healed, the clients finish, and Porcupine
checks the entire history.

| Test | Faults | Typical ops checked |
|---|---|---|
| `TestKVConcurrent` | none | ~50,000 |
| `TestKVUnreliable` | drops, delays, reordering | ~650 |
| `TestKVPartitions` | random majority/minority partitions | ~85,000 |
| `TestKVPartitionsUnreliable` | partitions + unreliable network | ~500 |
| `TestKVCrashRestart` | random crash + restart from storage | ~90,000 |
| `TestKVAllFaults` | all of the above at once | ~1,000 |
| `TestKVSnapshots` / `…AllFaults` | the above with aggressive log compaction | ~40,000 / ~500 |
| `TestNodeLinearizableUnderCrashes` | **real HTTP and real disk**, nodes killed and restarted from their WAL | ~17,000 |

The checker itself is tested ([`model_test.go`](kv/linearizability/model_test.go)).
It must reject a stale read, a lost delete and a duplicated write. If a real
violation is ever found, the test writes an interactive HTML visualization of
the offending history.

`TestKVAllFaults` makes less progress than the others by design. With 5
nodes, a 3/2 partition plus a crash on the majority side leaves no reachable
majority. The cluster must then refuse writes rather than lose data (the
CAP theorem in action). `KVTRACE=1` shows it happening.

## Running a cluster

```sh
go build -o kvnode ./cmd/kvnode

PEERS=http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003
./kvnode -id 0 -peers $PEERS -data data/0 &
./kvnode -id 1 -peers $PEERS -data data/1 &
./kvnode -id 2 -peers $PEERS -data data/2 &

curl -L -X PUT --data-binary 'world' http://127.0.0.1:7001/kv/hello   # 204
curl -L http://127.0.0.1:7002/kv/hello                                # world
curl -L -X DELETE http://127.0.0.1:7003/kv/hello                      # 204
curl http://127.0.0.1:7001/status
```

| Endpoint | Result |
|---|---|
| `GET /kv/{key}` | `200` with the value, or `404` |
| `PUT /kv/{key}` (body = value) | `204` |
| `DELETE /kv/{key}` | `204` |
| `GET /status` | JSON: role, term, leader, commit/applied index, snapshot index, log size |

Any node accepts requests. Followers answer `307` with the leader's URL
(`curl -L` follows it), or `503` mid-election. Optional `X-Client-ID` and
`X-Request-Seq` headers make retried writes exactly-once. The Go client
([`client`](client/client.go)) sets them, tracks the leader and retries
automatically:

```go
c := client.New([]string{"http://127.0.0.1:7001", "http://127.0.0.1:7002", "http://127.0.0.1:7003"})
err := c.Put(ctx, "hello", "world")
v, err := c.Get(ctx, "hello")
```

`kvnode` flags: `-snapshot-bytes` (log size that triggers compaction, default
4 MiB), `-election-timeout`, `-heartbeat`, and `-nosync` (benchmarking only).

## Design

```
            HTTP clients (curl, client.Client)
                         │  GET/PUT/DELETE /kv/{key}
┌────────────────────────▼─────────────────────────┐
│ node      HTTP API, redirects to leader, /status │
├──────────────────────────────────────────────────┤
│ kv        state machine (map + dedup table)      │
│           Execute(op): Start → wait for apply    │
├──────────────────────────────────────────────────┤
│ raft      election, replication, commit, apply,  │ ◄──► other nodes
│           snapshots                              │      (transport)
├──────────────────────────────────────────────────┤
│ Storage   FileStorage (WAL + snapshot files)     │
│           MemoryStorage (simulated tests)        │
└──────────────────────────────────────────────────┘
transport.Caller: HTTPCaller in production, simnet.ClientEnd in tests
```

- **Every operation goes through the log**, reads included. A reply is sent
  only after the op is applied by the leader that accepted it. The handler
  confirms the applied entry carries the term it was proposed in (by Raft's
  log-matching property, that means it is the same entry). So each op takes
  effect at one instant between request and response, which is exactly
  linearizability.
- **Exactly-once writes.** Clients tag writes with `(clientID, seq)`. The
  state machine stores the highest applied `seq` per client, both in memory
  and in snapshots, and skips duplicates. Without this, a retried `PUT` could
  be applied after a newer write and silently revert it. The duplicate-write
  test shows Porcupine catching exactly that.
- **No-op on election.** A new leader commits an empty entry from its own
  term, which commits everything before it (Raft §5.4.2). It learns the
  commit point immediately instead of waiting for a client write.
- **Group commit.** The leader replicates new entries to followers while
  fsyncing them locally. It counts itself towards the majority only once its
  own copy is durable. Followers fsync before acknowledging.
- **WAL format.** Each record is `[len][crc32c][type][payload]`, with three
  record types: header, hard state (term and vote), and entry batches. On
  startup the WAL is replayed and a torn or corrupt tail is truncated.
  Compaction writes the snapshot file, then writes a new WAL and atomically
  renames it into place; the rename is the commit point.
- **PreVote + CheckQuorum.** A node only increments its term if a majority
  would vote for it and hasn't heard from a leader recently, so a node
  rejoining from a partition can't depose a healthy leader. A leader that
  can't reach a majority within an election timeout steps down, so clients
  stop waiting on it.

## Layout

```
raft/          Raft consensus, Storage interface, on-disk WAL, tests + harness
kv/            replicated state machine, Clerk, linearizability tests
kv/linearizability/  Porcupine model + history recorder
simnet/        adversarial in-memory network
transport/     RPC abstraction; gob-over-HTTP implementation
node/          networked replica: HTTP API, local cluster helper, integration tests
client/        Go client for the HTTP API
cmd/kvnode/    server binary
cmd/kvbench/   benchmark
```

## Limitations and next steps

- Reads go through the log, which is simple and obviously correct but costs a
  round of replication. *ReadIndex* or leader leases would make reads cheaper.
- Cluster membership is static. Adding or removing nodes would need joint
  consensus or single-server membership changes.
- Snapshots are sent in one message. Very large states would need chunked
  `InstallSnapshot`.
- The benchmark runs every node on one machine and one disk, so absolute
  numbers are conservative for real deployments, where disks and NICs aren't
  shared.
