# distributed_key-value_store


What it is: A database that keeps the same data on 3 to 5 machines, so it stays correct and available when some of them crash or lose network connectivity. Systems like etcd (which Kubernetes runs on), Consul, and CockroachDB are built on this idea.

What you'd build:

Leader election: Nodes vote for one leader. If it dies, a new one is elected automatically.
Log replication: The leader appends every write (SET x=5) to a log and copies it to followers. A write counts as committed once a majority has it.
Persistence and recovery: Each node writes its log to disk, so a restarted node rejoins with its state.
Snapshotting: Periodically compact the log so it doesn't grow forever.
Client API: GET, PUT, DELETE over gRPC or HTTP, with linearizable semantics (every read sees the latest committed write).

The part that makes it impressive: the test harness. Build a simulated network that drops, delays, and reorders messages and partitions nodes. Run random workloads while killing nodes, then verify the recorded history with a linearizability checker (Porcupine, for example). Most people only get the happy path working, so a correctness proof under failures stands out.

Numbers to report: throughput (ops/sec), p50/p99 latency, and leader failover time on a 3- and 5-node cluster.

Language: Go is faster to build in and has good tooling. C++ shows more raw systems skill but takes roughly twice as long. Since you already know Go and Kubernetes, I'd pick Go.

Timeline: about 6 to 8 weeks. MIT's 6.5840 (6.824) labs are public and are a good guide, but build your own API, tests, and benchmarks on top so it isn't a copy of the course.
