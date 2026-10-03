package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/aneeshramanathan/distributed_key-value_store/client"
	"github.com/aneeshramanathan/distributed_key-value_store/kv/linearizability"
)

// testCluster runs real nodes on localhost, each with its own data
// directory, talking real HTTP.
type testCluster struct {
	t            *testing.T
	addrs        []string
	urls         []string
	dirs         []string
	maxRaftState int

	mu    sync.Mutex
	nodes []*Node
}

func newTestCluster(t *testing.T, n, maxRaftState int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, maxRaftState: maxRaftState, nodes: make([]*Node, n)}
	listeners := make([]net.Listener, n)
	root := t.TempDir()
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		c.addrs = append(c.addrs, ln.Addr().String())
		c.urls = append(c.urls, "http://"+ln.Addr().String())
		c.dirs = append(c.dirs, filepath.Join(root, fmt.Sprintf("node%d", i)))
	}
	for i := 0; i < n; i++ {
		c.startWith(i, listeners[i])
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.kill(i)
		}
	})
	return c
}

func (c *testCluster) startWith(i int, ln net.Listener) {
	nd, err := Start(Config{ID: i, Peers: c.urls, DataDir: c.dirs[i], Listener: ln, MaxRaftState: c.maxRaftState})
	if err != nil {
		c.t.Fatalf("start node %d: %v", i, err)
	}
	c.mu.Lock()
	c.nodes[i] = nd
	c.mu.Unlock()
}

// restart starts node i again on its old address, from its data directory.
func (c *testCluster) restart(i int) {
	var ln net.Listener
	var err error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if ln, err = net.Listen("tcp", c.addrs[i]); err == nil {
			break
		}
	}
	if err != nil {
		c.t.Fatalf("relisten on %s: %v", c.addrs[i], err)
	}
	c.startWith(i, ln)
}

func (c *testCluster) kill(i int) {
	c.mu.Lock()
	nd := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if nd != nil {
		nd.Close()
	}
}

func (c *testCluster) node(i int) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *testCluster) waitLeader() int {
	c.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		for i := range c.nodes {
			if nd := c.node(i); nd != nil && nd.Status().Role.String() == "leader" {
				return i
			}
		}
	}
	c.t.Fatal("no leader elected")
	return -1
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNodeBasicAPI(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	leader := c.waitLeader()
	ctx := ctxT(t)
	cl := client.New(c.urls)

	if _, err := cl.Get(ctx, "nope"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get(missing) err = %v", err)
	}
	if err := cl.Put(ctx, "greeting", "hello world"); err != nil {
		t.Fatal(err)
	}
	if err := cl.Put(ctx, "path/with/slashes", "ok"); err != nil {
		t.Fatal(err)
	}
	if v, err := cl.Get(ctx, "greeting"); err != nil || v != "hello world" {
		t.Fatalf("Get = %q, %v", v, err)
	}
	if v, err := cl.Get(ctx, "path/with/slashes"); err != nil || v != "ok" {
		t.Fatalf("Get(path) = %q, %v", v, err)
	}
	if err := cl.Delete(ctx, "greeting"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Get(ctx, "greeting"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get after delete err = %v", err)
	}

	// A follower redirects plain HTTP clients to the leader.
	follower := (leader + 1) % 3
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noFollow.Get(c.urls[follower] + "/kv/anything")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect || resp.Header.Get("Location") != c.urls[leader]+"/kv/anything" {
		t.Fatalf("follower answered %d Location=%q", resp.StatusCode, resp.Header.Get("Location"))
	}

	// /status reports the leader.
	resp, err = http.Get(c.urls[follower] + "/status")
	if err != nil {
		t.Fatal(err)
	}
	var st StatusResponse
	json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if st.LeaderID != leader || st.Role != "follower" {
		t.Fatalf("status = %+v, leader is %d", st, leader)
	}
}

func TestNodeFailoverAndRecovery(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	ctx := ctxT(t)
	cl := client.New(c.urls)
	for i := 0; i < 20; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Kill the leader; the remaining two elect a new one and keep serving.
	old := c.waitLeader()
	c.kill(old)
	for i := 20; i < 40; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if c.waitLeader() == old {
		t.Fatal("dead node still leader")
	}

	// The old leader restarts from disk and catches up.
	c.restart(old)
	target := c.node(c.waitLeader()).Status().CommitIndex
	for deadline := time.Now().Add(10 * time.Second); c.node(old).Status().LastApplied < target; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("restarted node stuck at %d, want %d", c.node(old).Status().LastApplied, target)
		}
	}

	// Restart the entire cluster: everything must come back from disk.
	for i := 0; i < 3; i++ {
		c.kill(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	cl = client.New(c.urls)
	for i := 0; i < 40; i++ {
		v, err := cl.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("after full restart k%d = %q, %v", i, v, err)
		}
	}
}

func TestNodeSnapshotRestart(t *testing.T) {
	c := newTestCluster(t, 3, 4096)
	ctx := ctxT(t)
	cl := client.New(c.urls)
	for i := 0; i < 300; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i%50), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	leader := c.waitLeader()
	if st := c.node(leader).Status(); st.SnapshotIndex == 0 {
		t.Fatalf("no snapshot taken: %+v", st)
	}
	if size := c.node(leader).storage.Size(); size > 8*4096 {
		t.Fatalf("log not compacted: %d bytes", size)
	}

	for i := 0; i < 3; i++ {
		c.kill(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	cl = client.New(c.urls)
	for k := 0; k < 50; k++ {
		want := fmt.Sprintf("v%d", 250+k)
		if v, err := cl.Get(ctx, fmt.Sprintf("k%d", k)); err != nil || v != want {
			t.Fatalf("k%d = %q, %v; want %q", k, v, err, want)
		}
	}
}

// TestNodeLinearizableUnderCrashes runs concurrent clients against a real
// cluster while nodes are killed and restarted from disk, then checks the
// history with Porcupine.
func TestNodeLinearizableUnderCrashes(t *testing.T) {
	c := newTestCluster(t, 3, 64<<10)
	rec := linearizability.NewRecorder()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for cli := 0; cli < 4; cli++ {
		wg.Add(1)
		go func(cli int) {
			defer wg.Done()
			cl := client.New(c.urls)
			for j := 0; !stop.Load(); j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				key := fmt.Sprintf("k%d", rand.IntN(5))
				var in linearizability.Input
				var out linearizability.Output
				var err error
				call := rec.Now()
				switch r := rand.IntN(10); {
				case r < 5:
					in = linearizability.Input{Kind: linearizability.Put, Key: key, Value: fmt.Sprintf("%d.%d", cli, j)}
					err = cl.Put(ctx, key, in.Value)
				case r < 9:
					in = linearizability.Input{Kind: linearizability.Get, Key: key}
					out.Value, err = cl.Get(ctx, key)
					out.Found = err == nil
					if errors.Is(err, client.ErrNotFound) {
						err = nil
					}
				default:
					in = linearizability.Input{Kind: linearizability.Delete, Key: key}
					err = cl.Delete(ctx, key)
				}
				ret := rec.Now()
				cancel()
				switch {
				case err == nil:
					rec.Record(cli, in, out, call, ret)
				case in.Kind != linearizability.Get:
					// Outcome unknown: the write may take effect at any
					// later time, so leave the operation open-ended.
					rec.Record(cli, in, out, call, math.MaxInt64)
				}
			}
		}(cli)
	}

	for i := 0; i < 6; i++ {
		time.Sleep(time.Second)
		victim := rand.IntN(3)
		c.kill(victim)
		time.Sleep(time.Duration(200+rand.IntN(600)) * time.Millisecond)
		c.restart(victim)
	}
	stop.Store(true)
	wg.Wait()

	n := rec.Len()
	if n < 100 {
		t.Fatalf("too little progress: %d ops", n)
	}
	res := rec.Check(30*time.Second, t.TempDir())
	if res.Outcome == porcupine.Illegal {
		t.Fatalf("history of %d ops is not linearizable; see %s", n, res.Visualization)
	}
	t.Logf("%d operations over real HTTP with crashes: %s", n, res.Outcome)
}
