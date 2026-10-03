package kv

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/aneeshramanathan/distributed_key-value_store/kv/linearizability"
	"github.com/aneeshramanathan/distributed_key-value_store/raft"
	"github.com/aneeshramanathan/distributed_key-value_store/simnet"
	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

// kvCluster runs n replicas on a simulated network.
type kvCluster struct {
	t            *testing.T
	n            int
	net          *simnet.Network
	cfg          raft.Config
	maxRaftState int

	mu       sync.Mutex
	servers  []*Server
	storages []*raft.MemoryStorage
	ends     [][]string
	group    []int // partition each server is in
	gen      int
	clerks   int
}

func newKVCluster(t *testing.T, n int, reliable bool, maxRaftState int) *kvCluster {
	t.Helper()
	c := &kvCluster{
		t:            t,
		n:            n,
		net:          simnet.New(),
		cfg:          raft.DefaultConfig(),
		maxRaftState: maxRaftState,
		servers:      make([]*Server, n),
		storages:     make([]*raft.MemoryStorage, n),
		ends:         make([][]string, n),
		group:        make([]int, n),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			c.crash(i)
		}
		c.net.Close()
	})
	return c
}

func kvServerName(i int) string { return fmt.Sprintf("kv-%d", i) }

func (c *kvCluster) start(i int) {
	c.crash(i)
	c.mu.Lock()
	c.gen++
	peers := make([]transport.Caller, c.n)
	c.ends[i] = make([]string, c.n)
	for j := 0; j < c.n; j++ {
		name := fmt.Sprintf("kv%d->%d#%d", i, j, c.gen)
		c.ends[i][j] = name
		peers[j] = c.net.MakeEnd(name)
		c.net.Connect(name, kvServerName(j))
	}
	storage := c.storages[i]
	c.mu.Unlock()

	kv, err := NewServer(peers, i, storage, c.maxRaftState, c.cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	srv := transport.NewServer()
	Register(srv, kv)
	c.net.AddServer(kvServerName(i), srv)

	c.mu.Lock()
	c.servers[i] = kv
	c.mu.Unlock()
	c.applyPartition()
}

// crash kills server i; its storage survives (cloned, so writes from the
// dying instance are not seen by the next one).
func (c *kvCluster) crash(i int) {
	c.net.DeleteServer(kvServerName(i))
	c.mu.Lock()
	kv := c.servers[i]
	c.servers[i] = nil
	c.storages[i] = c.storages[i].Clone()
	c.mu.Unlock()
	c.applyPartition()
	if kv != nil {
		kv.Kill()
	}
}

func (c *kvCluster) alive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i] != nil
}

// partition assigns servers to groups; only servers in the same group can
// talk to each other. Clients can always reach every live server.
func (c *kvCluster) partition(group []int) {
	c.mu.Lock()
	copy(c.group, group)
	c.mu.Unlock()
	c.applyPartition()
}

func (c *kvCluster) heal() { c.partition(make([]int, c.n)) }

func (c *kvCluster) applyPartition() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.n; i++ {
		for j := 0; j < c.n; j++ {
			if c.ends[i] != nil {
				up := c.servers[i] != nil && c.servers[j] != nil && c.group[i] == c.group[j]
				c.net.Enable(c.ends[i][j], up)
			}
		}
	}
}

// randomPartition splits the servers into a random majority and minority.
func (c *kvCluster) randomPartition() {
	perm := rand.Perm(c.n)
	group := make([]int, c.n)
	for k, i := range perm {
		if k > c.n/2 {
			group[i] = 1
		}
	}
	c.partition(group)
}

func (c *kvCluster) makeClerk() *Clerk {
	c.mu.Lock()
	c.clerks++
	id := c.clerks
	c.mu.Unlock()
	ends := make([]transport.Caller, c.n)
	for j := 0; j < c.n; j++ {
		name := fmt.Sprintf("clerk%d->%d", id, j)
		ends[j] = c.net.MakeEnd(name)
		c.net.Connect(name, kvServerName(j))
		c.net.Enable(name, true)
	}
	return NewClerk(ends)
}

// trace, enabled with KVTRACE=1, logs progress and each server's partition
// group, term and role at every nemesis step.
var trace = os.Getenv("KVTRACE") != ""

func (c *kvCluster) describe() string {
	var parts []string
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		s, g := c.servers[i], c.group[i]
		c.mu.Unlock()
		if s == nil {
			parts = append(parts, fmt.Sprintf("%d:down", i))
			continue
		}
		st := s.Raft().Status()
		parts = append(parts, fmt.Sprintf("%d:g%d/t%d/%s", i, g, st.Term, st.Role))
	}
	return strings.Join(parts, " ")
}

func (c *kvCluster) raftStateSizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	sizes := make([]int, c.n)
	for i, s := range c.storages {
		sizes[i] = s.Size()
	}
	return sizes
}

// ---- tests ----

func TestKVBasic(t *testing.T) {
	c := newKVCluster(t, 3, true, -1)
	ck := c.makeClerk()

	if _, ok := ck.Get("missing"); ok {
		t.Fatal("Get of a missing key reported it present")
	}
	ck.Put("a", "1")
	ck.Put("b", "2")
	if v, ok := ck.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q, %v", v, ok)
	}
	ck.Put("a", "3")
	if v, _ := ck.Get("a"); v != "3" {
		t.Fatalf("Get(a) = %q after overwrite", v)
	}
	ck.Delete("a")
	if _, ok := ck.Get("a"); ok {
		t.Fatal("Get(a) found key after Delete")
	}
	// A second clerk sees the first one's writes.
	if v, _ := c.makeClerk().Get("b"); v != "2" {
		t.Fatalf("Get(b) from second clerk = %q", v)
	}
}

func TestKVDuplicateDetection(t *testing.T) {
	kv := &Server{data: map[string]string{}, lastSeq: map[int64]int64{}}
	kv.apply(Op{Kind: OpPut, Key: "x", Value: "old", ClientID: 7, Seq: 1})
	kv.apply(Op{Kind: OpPut, Key: "x", Value: "new", ClientID: 8, Seq: 1})
	// Client 7's retry of seq 1 must not clobber client 8's later write.
	kv.apply(Op{Kind: OpPut, Key: "x", Value: "old", ClientID: 7, Seq: 1})
	if kv.data["x"] != "new" {
		t.Fatalf("duplicate write was re-applied: x = %q", kv.data["x"])
	}
	kv.apply(Op{Kind: OpDelete, Key: "x", ClientID: 7, Seq: 2})
	kv.apply(Op{Kind: OpPut, Key: "x", Value: "again", ClientID: 8, Seq: 2})
	kv.apply(Op{Kind: OpDelete, Key: "x", ClientID: 7, Seq: 2})
	if kv.data["x"] != "again" {
		t.Fatalf("duplicate delete was re-applied")
	}
}

func TestKVOpEncoding(t *testing.T) {
	for _, op := range []Op{
		{Kind: OpGet, Key: "k"},
		{Kind: OpPut, Key: "key", Value: "value with spaces", ClientID: 1 << 60, Seq: 42},
		{Kind: OpDelete, Key: "", ClientID: -5, Seq: 0},
	} {
		got, err := decodeOp(encodeOp(op))
		if err != nil || got != op {
			t.Fatalf("round trip %+v -> %+v, %v", op, got, err)
		}
	}
	if _, err := decodeOp([]byte{1, 2}); err == nil {
		t.Fatal("decoded a truncated op")
	}
}

type workload struct {
	servers      int
	clients      int
	keys         int
	minOps       int // fail if fewer operations complete (default 20 per client)
	reliable     bool
	partitions   bool
	crashes      bool
	maxRaftState int
	duration     time.Duration
}

// run drives a random workload against the cluster while a nemesis injects
// faults, then checks the recorded history for linearizability.
func (w workload) run(t *testing.T) {
	c := newKVCluster(t, w.servers, w.reliable, w.maxRaftState)
	rec := linearizability.NewRecorder()

	var stopClients, stopNemesis atomic.Bool
	var clients sync.WaitGroup
	for cli := 0; cli < w.clients; cli++ {
		clients.Add(1)
		go func(cli int) {
			defer clients.Done()
			ck := c.makeClerk()
			for j := 0; !stopClients.Load(); j++ {
				key := fmt.Sprintf("k%d", rand.IntN(w.keys))
				var in linearizability.Input
				var out linearizability.Output
				call := rec.Now()
				switch r := rand.IntN(100); {
				case r < 45:
					in = linearizability.Input{Kind: linearizability.Put, Key: key, Value: fmt.Sprintf("%d.%d", cli, j)}
					ck.Put(key, in.Value)
				case r < 90:
					in = linearizability.Input{Kind: linearizability.Get, Key: key}
					out.Value, out.Found = ck.Get(key)
				default:
					in = linearizability.Input{Kind: linearizability.Delete, Key: key}
					ck.Delete(key)
				}
				rec.Record(cli, in, out, call, rec.Now())
			}
		}(cli)
	}

	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		last := 0
		for !stopNemesis.Load() {
			time.Sleep(time.Duration(1000+rand.IntN(1000)) * time.Millisecond)
			if trace {
				t.Logf("+%d ops  %s", rec.Len()-last, c.describe())
				last = rec.Len()
			}
			if w.partitions {
				if rand.IntN(4) == 0 {
					c.heal()
				} else {
					c.randomPartition()
				}
			}
			if w.crashes {
				// Restart anything that is down, then crash someone,
				// keeping a majority alive.
				down := 0
				for i := 0; i < w.servers; i++ {
					if !c.alive(i) {
						if rand.IntN(2) == 0 {
							c.start(i)
						} else {
							down++
						}
					}
				}
				if down < (w.servers-1)/2 {
					c.crash(rand.IntN(w.servers))
				}
			}
		}
	}()

	time.Sleep(w.duration)
	stopNemesis.Store(true)
	<-nemesisDone

	// Repair everything so that in-flight operations can finish.
	c.net.SetReliable(true)
	for i := 0; i < w.servers; i++ {
		if !c.alive(i) {
			c.start(i)
		}
	}
	c.heal()
	stopClients.Store(true)

	done := make(chan struct{})
	go func() { clients.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("clients did not finish after the cluster was healed")
	}

	n := rec.Len()
	if w.minOps == 0 {
		w.minOps = 20 * w.clients
	}
	if n < w.minOps {
		t.Fatalf("too little progress: only %d operations completed", n)
	}
	res := rec.Check(30*time.Second, t.TempDir())
	switch res.Outcome {
	case porcupine.Illegal:
		t.Fatalf("history of %d ops is NOT linearizable; visualization: %s", n, res.Visualization)
	case porcupine.Unknown:
		t.Logf("linearizability check timed out on %d ops (no violation found)", n)
	default:
		t.Logf("%d operations, linearizable", n)
	}

	if w.maxRaftState > 0 {
		// Allow for the entries appended between snapshots.
		for i, size := range c.raftStateSizes() {
			if size > 8*w.maxRaftState {
				t.Fatalf("server %d: raft state is %d bytes, limit %d; snapshots not compacting the log", i, size, w.maxRaftState)
			}
		}
	}
}

const testDuration = 5 * time.Second

// Porcupine checks each key independently, so spreading the load over more
// keys keeps checking fast even when the cluster completes many operations.

func TestKVConcurrent(t *testing.T) {
	workload{servers: 3, clients: 5, keys: 20, reliable: true, duration: testDuration}.run(t)
}

func TestKVUnreliable(t *testing.T) {
	workload{servers: 3, clients: 5, keys: 5, reliable: false, duration: testDuration}.run(t)
}

func TestKVPartitions(t *testing.T) {
	workload{servers: 5, clients: 5, keys: 20, reliable: true, partitions: true, duration: testDuration}.run(t)
}

func TestKVPartitionsUnreliable(t *testing.T) {
	workload{servers: 5, clients: 5, keys: 5, reliable: false, partitions: true, duration: testDuration}.run(t)
}

func TestKVCrashRestart(t *testing.T) {
	workload{servers: 5, clients: 5, keys: 20, reliable: true, crashes: true, duration: testDuration}.run(t)
}

// With partitions and crashes together, the nemesis regularly leaves no
// majority reachable, and the cluster must (correctly) refuse to make
// progress until one is restored. So the progress floor is lower.
func TestKVAllFaults(t *testing.T) {
	workload{servers: 5, clients: 5, keys: 5, minOps: 50, reliable: false, partitions: true, crashes: true, duration: 3 * testDuration}.run(t)
}

func TestKVSnapshots(t *testing.T) {
	workload{servers: 3, clients: 5, keys: 20, reliable: true, maxRaftState: 1000, duration: testDuration}.run(t)
}

func TestKVSnapshotsAllFaults(t *testing.T) {
	workload{servers: 5, clients: 5, keys: 5, minOps: 50, reliable: false, partitions: true, crashes: true, maxRaftState: 1000, duration: 3 * testDuration}.run(t)
}
