package raft

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- leader election ----

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.checkOneLeader()

	// With no failures, the leader and term should stay put.
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, but should be at least 1", term1)
	}
	time.Sleep(2 * c.cfg.ElectionTimeoutMax)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true, false)
	leader1 := c.checkOneLeader()

	// The leader disconnects: a new one should be elected.
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoins; it must not disturb the new leader.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Without a quorum, no leader can be elected.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(2 * c.cfg.ElectionTimeoutMax)
	c.checkNoLeader()

	// A quorum is restored.
	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()

	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	const n = 7
	c := newCluster(t, n, true, false)
	c.checkOneLeader()
	for iter := 0; iter < 10; iter++ {
		i1, i2, i3 := rand.IntN(n), rand.IntN(n), rand.IntN(n)
		c.disconnect(i1)
		c.disconnect(i2)
		c.disconnect(i3)
		// Either the current leader survives or a new one is elected.
		c.checkOneLeader()
		c.connect(i1)
		c.connect(i2)
		c.connect(i3)
	}
	c.checkOneLeader()
}

// ---- log replication ----

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, true, false)
	for i := 1; i <= 3; i++ {
		c.one(cmd(i*100), 3, false)
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(cmd(101), 3, false)

	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 1) % 3)
	// The remaining two still form a majority.
	c.one(cmd(102), 2, false)
	time.Sleep(c.cfg.ElectionTimeoutMax)
	c.one(cmd(103), 2, false)

	leader2 := c.checkOneLeader()
	c.disconnect((leader2 + 1) % 3)
	c.disconnect((leader2 + 2) % 3)

	// The lone leader cannot commit anything.
	index, _, ok := c.raft(leader2).Start(cmd(104))
	if !ok {
		t.Fatal("leader rejected Start()")
	}
	time.Sleep(2 * c.cfg.ElectionTimeoutMax)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}
}

func TestLeaderFailure(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(cmd(101), 3, false)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.one(cmd(102), 2, false)
	time.Sleep(c.cfg.ElectionTimeoutMax)
	c.one(cmd(103), 2, false)

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	// Only one server is left connected, so nothing can commit.
	var indices []int
	for i := 0; i < 3; i++ {
		if idx, _, ok := c.raft(i).Start(cmd(104)); ok {
			indices = append(indices, idx)
		}
	}
	time.Sleep(2 * c.cfg.ElectionTimeoutMax)
	for _, index := range indices {
		if _, command := c.nCommitted(index); cmdVal(command) == 104 {
			t.Fatalf("cmd 104 committed at %d but no majority", index)
		}
	}
}

func TestFailAgree(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(cmd(101), 3, false)

	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 3)
	c.one(cmd(102), 2, false)
	c.one(cmd(103), 2, false)
	time.Sleep(c.cfg.ElectionTimeoutMax)
	c.one(cmd(104), 2, false)
	c.one(cmd(105), 2, false)

	// The follower catches up after reconnecting.
	c.connect((leader + 1) % 3)
	c.one(cmd(106), 3, true)
	time.Sleep(c.cfg.ElectionTimeoutMax)
	c.one(cmd(107), 3, true)
}

func TestFailNoAgree(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true, false)
	c.one(cmd(10), n, false)

	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % n)
	c.disconnect((leader + 2) % n)
	c.disconnect((leader + 3) % n)

	index, _, ok := c.raft(leader).Start(cmd(20))
	if !ok {
		t.Fatal("leader rejected Start()")
	}
	time.Sleep(2 * c.cfg.ElectionTimeoutMax)
	if nd, _ := c.nCommitted(index); nd > 0 {
		t.Fatalf("%d committed but no majority", nd)
	}

	c.connect((leader + 1) % n)
	c.connect((leader + 2) % n)
	c.connect((leader + 3) % n)

	// The disconnected majority may have elected a leader and forgotten
	// index; either way, new commands must commit.
	leader2 := c.checkOneLeader()
	if _, _, ok := c.raft(leader2).Start(cmd(30)); !ok {
		t.Fatal("leader2 rejected Start()")
	}
	c.one(cmd(1000), n, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, true, false)
	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second) // give the cluster time to settle
		}
		leader := c.checkOneLeader()
		_, term, ok := c.raft(leader).Start(cmd(1))
		if !ok {
			continue // leader moved on quickly
		}

		const iters = 5
		var wg sync.WaitGroup
		indices := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, term1, ok := c.raft(leader).Start(cmd(100 + i))
				if ok && term1 == term {
					indices <- idx
				}
			}(ii)
		}
		wg.Wait()
		close(indices)

		if t1, _ := c.raft(leader).GetState(); t1 != term {
			continue // term changed; try again
		}
		failed := false
		got := map[int]bool{}
		for idx := range indices {
			command := c.wait(idx, 3, term)
			if command == nil {
				failed = true
				break
			}
			got[cmdVal(command)] = true
		}
		if failed {
			continue
		}
		for ii := 0; ii < iters; ii++ {
			if !got[100+ii] {
				t.Fatalf("cmd %d missing", 100+ii)
			}
		}
		return
	}
	t.Fatal("term changed too often")
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(cmd(101), 3, true)

	// Leader 1 is partitioned away and accepts entries that never commit.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.raft(leader1).Start(cmd(102))
	c.raft(leader1).Start(cmd(103))
	c.raft(leader1).Start(cmd(104))

	// The new leader commits at those indexes.
	c.one(cmd(103), 2, true)
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)

	// The old leader returns; its uncommitted entries must be overwritten.
	c.connect(leader1)
	c.one(cmd(104), 2, true)

	c.connect(leader2)
	c.one(cmd(105), 3, true)
}

// TestBackup forces followers with long divergent logs to be repaired,
// exercising the leader's accelerated back-off.
func TestBackup(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true, false)
	c.one(cmd(rand.Int()), n, true)

	// Put the leader and one follower in a partition and give them
	// entries that will never commit.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % n)
	c.disconnect((leader1 + 3) % n)
	c.disconnect((leader1 + 4) % n)
	for i := 0; i < 50; i++ {
		c.raft(leader1).Start(cmd(rand.Int()))
	}
	time.Sleep(c.cfg.ElectionTimeoutMax / 2)

	c.disconnect((leader1 + 0) % n)
	c.disconnect((leader1 + 1) % n)

	// The other three commit lots of entries.
	c.connect((leader1 + 2) % n)
	c.connect((leader1 + 3) % n)
	c.connect((leader1 + 4) % n)
	for i := 0; i < 50; i++ {
		c.one(cmd(rand.Int()), 3, true)
	}

	// Now a leader with one follower adds more uncommitted entries.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % n
	if leader2 == other {
		other = (leader2 + 1) % n
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ {
		c.raft(leader2).Start(cmd(rand.Int()))
	}
	time.Sleep(c.cfg.ElectionTimeoutMax / 2)

	// Bring the original leader back, with its divergent log.
	for i := 0; i < n; i++ {
		c.disconnect(i)
	}
	c.connect((leader1 + 0) % n)
	c.connect((leader1 + 1) % n)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(cmd(rand.Int()), 3, true)
	}

	// Everyone together.
	for i := 0; i < n; i++ {
		c.connect(i)
	}
	c.one(cmd(rand.Int()), n, true)
}

// TestRPCCount checks that an idle cluster and a simple workload do not send
// an excessive number of messages.
func TestRPCCount(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.checkOneLeader()
	total1 := c.rpcTotal()
	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%d) to elect initial leader", total1)
	}

	before := c.rpcTotal()
	for i := 1; i <= 10; i++ {
		c.one(cmd(i), 3, false)
	}
	if got := c.rpcTotal() - before; got > 120 {
		t.Fatalf("too many RPCs (%d) for 10 entries", got)
	}

	time.Sleep(time.Second)
	idle := c.rpcTotal()
	time.Sleep(time.Second)
	// Heartbeats only: roughly 2 peers * 10 per second.
	if got := c.rpcTotal() - idle; got > 3*20 {
		t.Fatalf("too many RPCs (%d) in 1 second of idleness", got)
	}
}

// ---- persistence ----

func TestPersist1(t *testing.T) {
	c := newCluster(t, 3, true, false)
	c.one(cmd(11), 3, true)

	// Crash and restart everyone.
	for i := 0; i < 3; i++ {
		c.startNode(i)
	}
	for i := 0; i < 3; i++ {
		c.connect(i)
	}
	c.one(cmd(12), 3, true)

	leader1 := c.checkOneLeader()
	c.startNode(leader1)
	c.connect(leader1)
	c.one(cmd(13), 3, true)

	leader2 := c.checkOneLeader()
	c.crash(leader2)
	index := c.one(cmd(14), 2, true)
	c.startNode(leader2)
	c.connect(leader2)

	c.wait(index, 3, -1) // the restarted node catches up

	i3 := (c.checkOneLeader() + 1) % 3
	c.crash(i3)
	c.one(cmd(15), 2, true)
	c.startNode(i3)
	c.connect(i3)
	c.one(cmd(16), 3, true)
}

func TestPersist2(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true, false)
	index := 1
	for iters := 0; iters < 5; iters++ {
		c.one(cmd(10+index), n, true)
		index++

		leader1 := c.checkOneLeader()
		c.crash((leader1 + 1) % n)
		c.crash((leader1 + 2) % n)
		c.one(cmd(10+index), n-2, true)
		index++

		c.crash((leader1 + 0) % n)
		c.crash((leader1 + 3) % n)
		c.crash((leader1 + 4) % n)

		c.startNode((leader1 + 1) % n)
		c.startNode((leader1 + 2) % n)
		c.connect((leader1 + 1) % n)
		c.connect((leader1 + 2) % n)

		time.Sleep(c.cfg.ElectionTimeoutMax)

		c.startNode((leader1 + 3) % n)
		c.connect((leader1 + 3) % n)
		c.one(cmd(10+index), n-2, true)
		index++

		c.startNode((leader1 + 4) % n)
		c.startNode((leader1 + 0) % n)
		c.connect((leader1 + 4) % n)
		c.connect((leader1 + 0) % n)
	}
	c.one(cmd(1000), n, true)
}

// TestFigure8 recreates the situation in Figure 8 of the Raft paper, where
// an entry stored on a majority can still be overwritten unless leaders only
// commit entries from their own term. Leaders crash at random times.
func TestFigure8(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true, false)
	c.one(cmd(rand.Int()), 1, true)

	up := n
	for iters := 0; iters < 300; iters++ {
		leader := -1
		for i := 0; i < n; i++ {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start(cmd(rand.Int())); ok {
					leader = i
				}
			}
		}
		if rand.IntN(1000) < 100 {
			time.Sleep(time.Duration(rand.Int64N(int64(c.cfg.ElectionTimeoutMax / 2))))
		} else {
			time.Sleep(time.Duration(rand.IntN(13)) * time.Millisecond)
		}
		if leader != -1 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			s := rand.IntN(n)
			if c.raft(s) == nil {
				c.startNode(s)
				c.connect(s)
				up++
			}
		}
	}
	for i := 0; i < n; i++ {
		if c.raft(i) == nil {
			c.startNode(i)
			c.connect(i)
		}
	}
	c.one(cmd(rand.Int()), n, true)
}

// TestFigure8Unreliable is Figure 8 again, on a network that drops,
// delays and badly reorders messages, with partitions instead of crashes.
func TestFigure8Unreliable(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false, false)
	c.one(cmd(rand.IntN(10000)), 1, true)

	nup := n
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			c.net.SetLongReordering(true)
		}
		leader := -1
		for i := 0; i < n; i++ {
			if _, _, ok := c.raft(i).Start(cmd(rand.IntN(10000))); ok && c.isConnected(i) {
				leader = i
			}
		}
		if rand.IntN(1000) < 100 {
			time.Sleep(time.Duration(rand.Int64N(int64(c.cfg.ElectionTimeoutMax / 2))))
		} else {
			time.Sleep(time.Duration(rand.IntN(13)) * time.Millisecond)
		}
		if leader != -1 && rand.IntN(1000) < int(c.cfg.ElectionTimeoutMax/time.Millisecond)/2 {
			c.disconnect(leader)
			nup--
		}
		if nup < 3 {
			s := rand.IntN(n)
			if !c.isConnected(s) {
				c.connect(s)
				nup++
			}
		}
	}
	for i := 0; i < n; i++ {
		c.connect(i)
	}
	c.net.SetLongReordering(false)
	c.net.SetReliable(true)
	c.one(cmd(rand.IntN(10000)), n, true)
}

func churn(t *testing.T, reliable bool) {
	const n = 5
	c := newCluster(t, n, reliable, false)

	var stop atomic.Bool
	var wg sync.WaitGroup
	results := make(chan []int, 3)
	for me := 0; me < 3; me++ {
		wg.Add(1)
		go func(me int) {
			defer wg.Done()
			var values []int
			defer func() { results <- values }()
			for !stop.Load() {
				x := rand.Int()
				index, ok := -1, false
				for i := 0; i < n; i++ {
					if rf := c.raft(i); rf != nil {
						if idx, _, isLeader := rf.Start(cmd(x)); isLeader {
							index, ok = idx, true
						}
					}
				}
				if ok {
					// Wait for it to commit, or give up after a while.
					for _, to := range []int{10, 20, 50, 100, 200} {
						if nd, command := c.nCommitted(index); nd > 0 {
							if cmdVal(command) == x {
								values = append(values, x)
							}
							break
						}
						time.Sleep(time.Duration(to) * time.Millisecond)
					}
				} else {
					time.Sleep(time.Duration(79+me*17) * time.Millisecond)
				}
			}
		}(me)
	}

	for iters := 0; iters < 20; iters++ {
		if rand.IntN(1000) < 200 {
			c.disconnect(rand.IntN(n))
		}
		if rand.IntN(1000) < 500 {
			i := rand.IntN(n)
			if c.raft(i) == nil {
				c.startNode(i)
			}
			c.connect(i)
		}
		if rand.IntN(1000) < 200 {
			if i := rand.IntN(n); c.raft(i) != nil {
				c.crash(i)
			}
		}
		// Keep the crash rate below the election timeout so the cluster
		// can make progress between failures.
		time.Sleep(c.cfg.ElectionTimeoutMax * 7 / 10)
	}
	time.Sleep(c.cfg.ElectionTimeoutMax)
	c.net.SetReliable(true)
	for i := 0; i < n; i++ {
		if c.raft(i) == nil {
			c.startNode(i)
		}
		c.connect(i)
	}
	stop.Store(true)
	wg.Wait()
	close(results)

	time.Sleep(c.cfg.ElectionTimeoutMax)
	last := c.one(cmd(rand.Int()), n, true)

	// Every value a client saw committed must really be in the log.
	var really []int
	for index := 1; index <= last; index++ {
		command := c.wait(index, n, -1)
		really = append(really, cmdVal(command))
	}
	present := map[int]bool{}
	for _, v := range really {
		present[v] = true
	}
	for values := range results {
		for _, v := range values {
			if !present[v] {
				t.Fatalf("didn't find a value %d committed by a client", v)
			}
		}
	}
}

func TestReliableChurn(t *testing.T)   { churn(t, true) }
func TestUnreliableChurn(t *testing.T) { churn(t, false) }

// ---- snapshots ----

func snapshotCommon(t *testing.T, disconnect, reliable, crash bool) {
	const n = 3
	c := newCluster(t, n, reliable, true)
	c.one(cmd(rand.Int()), n, true)
	leader1 := c.checkOneLeader()

	for i := 0; i < 15; i++ {
		victim := (leader1 + 1) % n
		sender := leader1
		if i%3 == 1 {
			sender, victim = (leader1+1)%n, leader1
		}
		if disconnect {
			c.disconnect(victim)
			c.one(cmd(rand.Int()), n-1, true)
		}
		if crash {
			c.crash(victim)
			c.one(cmd(rand.Int()), n-1, true)
		}

		// Enough entries for the others to snapshot past the victim's log,
		// so it can only be caught up with InstallSnapshot.
		for j := 0; j < snapshotEvery+1; j++ {
			if rf := c.raft(sender); rf != nil {
				rf.Start(cmd(rand.Int()))
			}
		}
		c.one(cmd(rand.Int()), n-1, true)

		if disconnect {
			c.connect(victim)
			c.one(cmd(rand.Int()), n, true)
			leader1 = c.checkOneLeader()
		}
		if crash {
			c.startNode(victim)
			c.connect(victim)
			c.one(cmd(rand.Int()), n, true)
			leader1 = c.checkOneLeader()
		}
	}
}

func TestSnapshotBasic(t *testing.T)                  { snapshotCommon(t, false, true, false) }
func TestSnapshotInstall(t *testing.T)                { snapshotCommon(t, true, true, false) }
func TestSnapshotInstallUnreliable(t *testing.T)      { snapshotCommon(t, true, false, false) }
func TestSnapshotInstallCrash(t *testing.T)           { snapshotCommon(t, false, true, true) }
func TestSnapshotInstallUnreliableCrash(t *testing.T) { snapshotCommon(t, false, false, true) }

// TestSnapshotAllCrash restarts the whole cluster repeatedly; every node must
// come back from its snapshot plus log tail.
func TestSnapshotAllCrash(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false, true)
	c.one(cmd(rand.Int()), n, true)
	for i := 0; i < 5; i++ {
		for j := 0; j < snapshotEvery+1; j++ {
			c.one(cmd(rand.Int()), n, true)
		}
		index1 := c.one(cmd(rand.Int()), n, true)

		for k := 0; k < n; k++ {
			c.crash(k)
		}
		for k := 0; k < n; k++ {
			c.startNode(k)
			c.connect(k)
		}
		index2 := c.one(cmd(rand.Int()), n, true)
		if index2 < index1+1 {
			t.Fatalf("index decreased from %d to %d", index1, index2)
		}
	}
}
