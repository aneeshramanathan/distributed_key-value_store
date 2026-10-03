package raft

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/simnet"
	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

// cluster is the test harness for Raft. It runs n nodes on a simulated
// network, records what each node applies, and fails the test as soon as two
// nodes apply different commands at the same index.
type cluster struct {
	t        *testing.T
	n        int
	net      *simnet.Network
	cfg      Config
	snapshot bool // whether nodes periodically snapshot their applied log

	mu        sync.Mutex
	rafts     []*Raft
	storages  []*MemoryStorage
	stops     []chan struct{}
	connected []bool
	ends      [][]string // ends[i][j]: name of i's end to j
	applied   [][][]byte // applied[i][k]: command node i applied at index k (index 0 unused)
	applyErr  string
	gen       int
	start     time.Time
}

const snapshotEvery = 10

func newCluster(t *testing.T, n int, reliable, snapshot bool) *cluster {
	t.Helper()
	c := &cluster{
		t:         t,
		n:         n,
		net:       simnet.New(),
		cfg:       DefaultConfig(),
		snapshot:  snapshot,
		rafts:     make([]*Raft, n),
		storages:  make([]*MemoryStorage, n),
		stops:     make([]chan struct{}, n),
		connected: make([]bool, n),
		ends:      make([][]string, n),
		applied:   make([][][]byte, n),
		start:     time.Now(),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.storages[i] = NewMemoryStorage()
		c.startNode(i)
	}
	for i := 0; i < n; i++ {
		c.connect(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
	c.net.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
}

func cmd(x int) []byte { return binary.BigEndian.AppendUint64(nil, uint64(x)) }

func cmdVal(b []byte) int {
	if len(b) != 8 {
		return -1 // no-op
	}
	return int(binary.BigEndian.Uint64(b))
}

func serverName(i int) string { return fmt.Sprintf("server-%d", i) }

// startNode (re)starts node i from its storage, as after a reboot.
func (c *cluster) startNode(i int) {
	c.crash(i)

	c.mu.Lock()
	c.gen++
	peers := make([]transport.Caller, c.n)
	c.ends[i] = make([]string, c.n)
	for j := 0; j < c.n; j++ {
		name := fmt.Sprintf("%d->%d#%d", i, j, c.gen)
		c.ends[i][j] = name
		peers[j] = c.net.MakeEnd(name)
		c.net.Connect(name, serverName(j))
	}
	// A restarted node starts with an empty state machine and rebuilds it
	// from its snapshot and log.
	c.applied[i] = [][]byte{nil}
	storage := c.storages[i]
	c.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	rf, err := New(peers, i, storage, applyCh, c.cfg)
	if err != nil {
		c.t.Fatalf("start node %d: %v", i, err)
	}
	stop := make(chan struct{})
	go c.applier(i, rf, applyCh, stop)

	srv := transport.NewServer()
	RegisterRPC(srv, rf)
	c.net.AddServer(serverName(i), srv)

	c.mu.Lock()
	c.rafts[i] = rf
	c.stops[i] = stop
	c.mu.Unlock()
}

// crash kills node i. Its storage is cloned so that anything the dying
// instance writes afterwards is not seen by the next incarnation.
func (c *cluster) crash(i int) {
	c.disconnect(i)
	c.net.DeleteServer(serverName(i))
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	if c.storages[i] != nil {
		c.storages[i] = c.storages[i].Clone()
	}
	if c.stops[i] != nil {
		close(c.stops[i])
		c.stops[i] = nil
	}
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

func (c *cluster) applier(i int, rf *Raft, applyCh chan ApplyMsg, stop chan struct{}) {
	for {
		var m ApplyMsg
		select {
		case m = <-applyCh:
		case <-stop:
			return
		}
		c.mu.Lock()
		if c.stops[i] != stop { // a newer incarnation exists
			c.mu.Unlock()
			return
		}
		switch {
		case m.SnapshotValid:
			var log [][]byte
			if err := transport.Decode(m.Snapshot, &log); err != nil {
				c.failf("node %d: bad snapshot: %v", i, err)
			} else if len(log) != m.SnapshotIndex+1 {
				c.failf("node %d: snapshot at %d holds %d entries", i, m.SnapshotIndex, len(log)-1)
			} else if m.SnapshotIndex >= len(c.applied[i]) {
				c.applied[i] = log
			}
		case m.CommandValid:
			if m.CommandIndex != len(c.applied[i]) {
				c.failf("node %d applied index %d out of order (expected %d)", i, m.CommandIndex, len(c.applied[i]))
				break
			}
			for j := 0; j < c.n; j++ {
				if j != i && m.CommandIndex < len(c.applied[j]) && !equal(c.applied[j][m.CommandIndex], m.Command) {
					c.failf("index %d: node %d applied %v but node %d applied %v",
						m.CommandIndex, i, cmdVal(m.Command), j, cmdVal(c.applied[j][m.CommandIndex]))
				}
			}
			c.applied[i] = append(c.applied[i], m.Command)
			if c.snapshot && m.CommandIndex%snapshotEvery == 0 {
				data, _ := transport.Encode(c.applied[i])
				c.mu.Unlock()
				rf.Snapshot(m.CommandIndex, data)
				continue
			}
		}
		c.mu.Unlock()
	}
}

func equal(a, b []byte) bool { return string(a) == string(b) }

// failf records the first apply error; the test fails at the next check.
func (c *cluster) failf(format string, args ...any) {
	if c.applyErr == "" {
		c.applyErr = fmt.Sprintf(format, args...)
	}
}

func (c *cluster) checkApplyErr() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
}

// connect attaches node i to the network.
func (c *cluster) connect(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[i] = true
	for j := 0; j < c.n; j++ {
		if c.connected[j] {
			c.net.Enable(c.ends[i][j], true)
			c.net.Enable(c.ends[j][i], true)
		}
	}
}

// disconnect cuts node i off from everyone.
func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[i] = false
	for j := 0; j < c.n; j++ {
		if c.ends[i] != nil {
			c.net.Enable(c.ends[i][j], false)
		}
		if c.ends[j] != nil {
			c.net.Enable(c.ends[j][i], false)
		}
	}
}

func (c *cluster) raft(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i]
}

// checkOneLeader waits for exactly one leader among connected nodes in the
// newest term and returns it.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for iter := 0; iter < 10; iter++ {
		time.Sleep(time.Duration(450+rand.IntN(100)) * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < c.n; i++ {
			if rf := c.raft(i); rf != nil && c.isConnected(i) {
				if term, isLeader := rf.GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		last := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			last = max(last, term)
		}
		if last != -1 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

// checkTerms checks that connected nodes agree on the term.
func (c *cluster) checkTerms() int {
	c.t.Helper()
	term := -1
	for i := 0; i < c.n; i++ {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			t, _ := rf.GetState()
			if term == -1 {
				term = t
			} else if term != t {
				c.t.Fatalf("servers disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := 0; i < c.n; i++ {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			if _, isLeader := rf.GetState(); isLeader {
				c.t.Fatalf("expected no leader among connected servers, but %d claims to be leader", i)
			}
		}
	}
}

// nCommitted returns how many nodes have applied index, and the command.
func (c *cluster) nCommitted(index int) (int, []byte) {
	c.t.Helper()
	c.checkApplyErr()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var command []byte
	for i := 0; i < c.n; i++ {
		if index < len(c.applied[i]) {
			if count > 0 && !equal(command, c.applied[i][index]) {
				c.t.Fatalf("committed values differ at index %d", index)
			}
			command = c.applied[i][index]
			count++
		}
	}
	return count, command
}

// one submits command and waits until at least expected nodes have applied
// it, retrying with whichever node is leader. It returns the log index.
func (c *cluster) one(command []byte, expected int, retry bool) int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			if rf := c.raft(starts); rf != nil && c.isConnected(starts) {
				if idx, _, ok := rf.Start(command); ok {
					index = idx
					break
				}
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if nd, cmd1 := c.nCommitted(index); nd > 0 && nd >= expected && equal(cmd1, command) {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%v) failed to reach agreement", cmdVal(command))
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement", cmdVal(command))
	return -1
}

// wait waits for at least n nodes to apply index. It gives up (returning
// nil) if the term moves past startTerm, since the entry may then be lost.
func (c *cluster) wait(index, n, startTerm int) []byte {
	c.t.Helper()
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for i := 0; i < c.n; i++ {
				if rf := c.raft(i); rf != nil {
					if t, _ := rf.GetState(); t > startTerm {
						return nil
					}
				}
			}
		}
	}
	nd, command := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d decided for index %d; wanted %d", nd, index, n)
	}
	return command
}

// rpcTotal returns the number of RPCs sent so far.
func (c *cluster) rpcTotal() int64 { return c.net.TotalCalls() }
