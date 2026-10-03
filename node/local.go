package node

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"
)

// LocalCluster runs a whole cluster inside one process, each node with its
// own data directory and loopback HTTP listener. Nodes talk to each other
// over real TCP, exactly as separate processes would. It is used by the
// benchmark and is handy for experiments.
type LocalCluster struct {
	URLs  []string
	addrs []string
	dirs  []string
	cfg   Config

	mu    sync.Mutex
	nodes []*Node
}

// StartLocalCluster starts n nodes with data under dir. Fields of tmpl other
// than ID, Peers, DataDir and Listener apply to every node.
func StartLocalCluster(n int, dir string, tmpl Config) (*LocalCluster, error) {
	c := &LocalCluster{cfg: tmpl, nodes: make([]*Node, n)}
	lns := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, l := range lns[:i] {
				l.Close()
			}
			return nil, err
		}
		lns[i] = ln
		c.addrs = append(c.addrs, ln.Addr().String())
		c.URLs = append(c.URLs, "http://"+ln.Addr().String())
		c.dirs = append(c.dirs, filepath.Join(dir, fmt.Sprintf("node%d", i)))
	}
	for i := 0; i < n; i++ {
		if err := c.start(i, lns[i]); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *LocalCluster) start(i int, ln net.Listener) error {
	cfg := c.cfg
	cfg.ID, cfg.Peers, cfg.DataDir, cfg.Listener = i, c.URLs, c.dirs[i], ln
	nd, err := Start(cfg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.nodes[i] = nd
	c.mu.Unlock()
	return nil
}

// Node returns node i, or nil if it is down.
func (c *LocalCluster) Node(i int) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

// Size returns the number of nodes.
func (c *LocalCluster) Size() int { return len(c.URLs) }

// Kill stops node i abruptly.
func (c *LocalCluster) Kill(i int) {
	c.mu.Lock()
	nd := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if nd != nil {
		nd.Close()
	}
}

// Restart brings node i back on its old address, recovering from disk.
func (c *LocalCluster) Restart(i int) error {
	var ln net.Listener
	var err error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if ln, err = net.Listen("tcp", c.addrs[i]); err == nil {
			return c.start(i, ln)
		}
	}
	return err
}

// Leader returns the index of a node that believes it leads, or -1.
func (c *LocalCluster) Leader() int {
	best, bestTerm := -1, -1
	for i := range c.URLs {
		if nd := c.Node(i); nd != nil {
			if st := nd.Status(); st.Role.String() == "leader" && st.Term > bestTerm {
				best, bestTerm = i, st.Term
			}
		}
	}
	return best
}

// WaitLeader waits up to timeout for a leader.
func (c *LocalCluster) WaitLeader(timeout time.Duration) (int, error) {
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if l := c.Leader(); l >= 0 {
			return l, nil
		}
	}
	return -1, fmt.Errorf("no leader after %v", timeout)
}

// Close stops every node.
func (c *LocalCluster) Close() {
	for i := range c.URLs {
		c.Kill(i)
	}
}
