// Command kvbench measures throughput, latency and leader failover time.
//
// For each cluster size it starts a local cluster (real HTTP over loopback,
// real fsync'd write-ahead logs), then:
//
//  1. Throughput and latency: -clients concurrent clients issue a mix of
//     GETs and PUTs over random keys for -duration. Every operation goes
//     through the Raft log, so reads are linearizable too.
//  2. Failover: a probe client writes continuously while the leader is
//     killed. Reported per trial:
//     - election: time until a new leader exists,
//     - unavailability: time until a write succeeds again,
//     - catch-up: time for the killed node, restarted from disk, to
//     replay and catch up with the leader.
//
// Usage:
//
//	kvbench -nodes 3,5 -clients 32 -duration 10s -trials 5
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/client"
	"github.com/aneeshramanathan/distributed_key-value_store/node"
)

type options struct {
	sizes     []int
	clients   int
	duration  time.Duration
	warmup    time.Duration
	keys      int
	valueSize int
	readRatio float64
	trials    int
	noSync    bool
}

func main() {
	var o options
	sizes := flag.String("nodes", "3,5", "comma-separated cluster sizes to benchmark")
	flag.IntVar(&o.clients, "clients", 32, "concurrent clients")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "measurement time per cluster size")
	flag.DurationVar(&o.warmup, "warmup", 2*time.Second, "warm-up time before measuring")
	flag.IntVar(&o.keys, "keys", 1000, "number of distinct keys")
	flag.IntVar(&o.valueSize, "value-size", 100, "value size in bytes")
	flag.Float64Var(&o.readRatio, "reads", 0.5, "fraction of operations that are GETs")
	flag.IntVar(&o.trials, "trials", 5, "leader failover trials per cluster size")
	flag.BoolVar(&o.noSync, "nosync", false, "disable fsync (shows the cost of durability; unsafe)")
	flag.Parse()
	for _, s := range strings.Split(*sizes, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			log.Fatalf("bad -nodes value %q", s)
		}
		o.sizes = append(o.sizes, n)
	}

	cfg := node.DefaultRaftConfig()
	fmt.Printf("kvbench: %d clients, %d keys, %d-byte values, %.0f%% reads, fsync=%v\n",
		o.clients, o.keys, o.valueSize, o.readRatio*100, !o.noSync)
	fmt.Printf("raft: heartbeat %v, election timeout %v-%v\n\n", cfg.HeartbeatInterval, cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax)

	var perf []perfResult
	var fail []failoverResult
	for _, n := range o.sizes {
		p, f, err := benchCluster(n, o)
		if err != nil {
			log.Fatalf("%d-node cluster: %v", n, err)
		}
		perf = append(perf, p)
		fail = append(fail, f)
	}

	fmt.Println("\n## Throughput and latency")
	fmt.Println()
	fmt.Println("| Nodes | Throughput (ops/s) | p50 | p99 | GET p50 / p99 | PUT p50 / p99 | Errors |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, p := range perf {
		fmt.Printf("| %d | %.0f | %s | %s | %s / %s | %s / %s | %d |\n", p.nodes, p.throughput,
			ms(p.all.p50), ms(p.all.p99), ms(p.get.p50), ms(p.get.p99), ms(p.put.p50), ms(p.put.p99), p.errors)
	}
	fmt.Println("\n## Leader failover")
	fmt.Println()
	fmt.Println("| Nodes | Election (median / max) | Write unavailability (median / max) | Restarted node catch-up (median) |")
	fmt.Println("|---|---|---|---|")
	for _, f := range fail {
		fmt.Printf("| %d | %s / %s | %s / %s | %s |\n", f.nodes,
			ms(median(f.election)), ms(slices.Max(f.election)),
			ms(median(f.unavailable)), ms(slices.Max(f.unavailable)), ms(median(f.catchUp)))
	}
}

type latencyStats struct{ p50, p99 time.Duration }

type perfResult struct {
	nodes         int
	throughput    float64
	all, get, put latencyStats
	errors        int64
}

type failoverResult struct {
	nodes                          int
	election, unavailable, catchUp []time.Duration
}

func benchCluster(n int, o options) (perfResult, failoverResult, error) {
	dir, err := os.MkdirTemp("", "kvbench-*")
	if err != nil {
		return perfResult{}, failoverResult{}, err
	}
	defer os.RemoveAll(dir)

	c, err := node.StartLocalCluster(n, dir, node.Config{NoSync: o.noSync})
	if err != nil {
		return perfResult{}, failoverResult{}, err
	}
	defer c.Close()
	if _, err := c.WaitLeader(10 * time.Second); err != nil {
		return perfResult{}, failoverResult{}, err
	}

	fmt.Printf("[%d nodes] warming up for %v\n", n, o.warmup)
	runLoad(c.URLs, o, o.warmup)
	fmt.Printf("[%d nodes] measuring for %v\n", n, o.duration)
	perf := runLoad(c.URLs, o, o.duration)
	perf.nodes = n
	fmt.Printf("[%d nodes] %.0f ops/s, p50 %s, p99 %s\n", n, perf.throughput, ms(perf.all.p50), ms(perf.all.p99))

	fail := failoverResult{nodes: n}
	for trial := 1; trial <= o.trials; trial++ {
		e, u, cu, err := failover(c)
		if err != nil {
			return perf, fail, fmt.Errorf("failover trial %d: %w", trial, err)
		}
		fmt.Printf("[%d nodes] failover %d: election %s, writes unavailable %s, catch-up %s\n", n, trial, ms(e), ms(u), ms(cu))
		fail.election = append(fail.election, e)
		fail.unavailable = append(fail.unavailable, u)
		fail.catchUp = append(fail.catchUp, cu)
	}
	return perf, fail, nil
}

// runLoad runs the mixed workload and returns throughput and latencies.
func runLoad(urls []string, o options, d time.Duration) perfResult {
	value := strings.Repeat("x", o.valueSize)
	var errs atomic.Int64
	var mu sync.Mutex
	var gets, puts []time.Duration
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	start := time.Now()
	for w := 0; w < o.clients; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := client.New(urls)
			var g, p []time.Duration
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("key-%06d", rand.IntN(o.keys))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t0 := time.Now()
				var err error
				isGet := rand.Float64() < o.readRatio
				if isGet {
					_, err = cl.Get(ctx, key)
					if err == client.ErrNotFound {
						err = nil
					}
				} else {
					err = cl.Put(ctx, key, value)
				}
				lat := time.Since(t0)
				cancel()
				switch {
				case err != nil:
					errs.Add(1)
				case isGet:
					g = append(g, lat)
				default:
					p = append(p, lat)
				}
			}
			mu.Lock()
			gets = append(gets, g...)
			puts = append(puts, p...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	all := append(append([]time.Duration(nil), gets...), puts...)
	return perfResult{
		throughput: float64(len(all)) / elapsed.Seconds(),
		all:        stats(all),
		get:        stats(gets),
		put:        stats(puts),
		errors:     errs.Load(),
	}
}

// failover kills the leader under a steady write load and measures how
// long the cluster takes to recover.
func failover(c *node.LocalCluster) (election, unavailable, catchUp time.Duration, err error) {
	old, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		return 0, 0, 0, err
	}
	oldTerm := c.Node(old).Status().Term

	// Probe: write continuously; record when each successful write
	// started and finished.
	type span struct{ start, end time.Time }
	var stop atomic.Bool
	successes := make(chan span, 1<<16)
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		cl := client.New(c.URLs)
		for i := 0; !stop.Load(); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			start := time.Now()
			if cl.Put(ctx, "failover-probe", strconv.Itoa(i)) == nil {
				successes <- span{start, time.Now()}
			}
			cancel()
		}
	}()
	time.Sleep(500 * time.Millisecond) // steady state

	t0 := time.Now()
	c.Kill(old)
	killed := time.Now()

	// Election: poll the survivors for a leader of a newer term.
	for {
		if l := c.Leader(); l >= 0 && c.Node(l) != nil && c.Node(l).Status().Term > oldTerm {
			election = time.Since(t0)
			break
		}
		if time.Since(t0) > 30*time.Second {
			stop.Store(true)
			return 0, 0, 0, fmt.Errorf("no new leader elected")
		}
		time.Sleep(time.Millisecond)
	}

	// Unavailability: until a write that was issued after the old leader
	// was gone succeeds. Such a write can only have been committed by the
	// new leader. (A write issued earlier may have been committed by the
	// old leader just before it died.)
	timeout := time.After(30 * time.Second)
wait:
	for {
		select {
		case s := <-successes:
			if !s.start.Before(killed) {
				unavailable = s.end.Sub(t0)
				break wait
			}
		case <-timeout:
			stop.Store(true)
			return 0, 0, 0, fmt.Errorf("writes never resumed")
		}
	}
	stop.Store(true)
	<-probeDone

	// Catch-up: restart the old leader from disk and wait until it has
	// applied everything the cluster had committed.
	newLeader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		return 0, 0, 0, err
	}
	target := c.Node(newLeader).Status().CommitIndex
	t1 := time.Now()
	if err := c.Restart(old); err != nil {
		return 0, 0, 0, err
	}
	for c.Node(old).Status().LastApplied < target {
		if time.Since(t1) > 30*time.Second {
			return 0, 0, 0, fmt.Errorf("restarted node did not catch up")
		}
		time.Sleep(time.Millisecond)
	}
	catchUp = time.Since(t1)
	time.Sleep(time.Second) // let the cluster settle before the next trial
	return election, unavailable, catchUp, nil
}

func stats(lat []time.Duration) latencyStats {
	if len(lat) == 0 {
		return latencyStats{}
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration { return lat[min(len(lat)-1, int(p*float64(len(lat))))] }
	return latencyStats{p50: pct(0.50), p99: pct(0.99)}
}

func median(d []time.Duration) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[len(s)/2]
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000)
}
