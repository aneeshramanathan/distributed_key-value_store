// Package simnet is an in-memory, adversarial network for testing
// distributed protocols.
//
// A Network connects named client endpoints (ClientEnd) to named servers
// (transport.Server). Every message is serialized with gob, so endpoints never
// share memory. The network can be configured to:
//
//   - drop requests and replies (unreliable mode),
//   - delay messages by a random amount, which also reorders them,
//   - hold back some replies for a long time (long reordering),
//   - make calls to unreachable servers hang for a long time before failing
//     (long delays), and
//   - partition the cluster by enabling or disabling individual endpoints.
//
// Crashing a node is modelled by deleting its server: calls that are in
// flight to a deleted server fail and their replies are discarded, even if
// the handler eventually returns.
package simnet

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

// Network is a simulated network. The zero value is not usable; call New.
type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool
	longReordering bool
	ends           map[string]*ClientEnd
	enabled        map[string]bool
	servers        map[string]*transport.Server
	connections    map[string]string // end name -> server name
	serverCounts   map[string]int
	done           chan struct{}
	closeOnce      sync.Once

	totalCalls atomic.Int64
	totalBytes atomic.Int64
}

// New returns a reliable network with no endpoints or servers.
func New() *Network {
	return &Network{
		reliable:     true,
		ends:         make(map[string]*ClientEnd),
		enabled:      make(map[string]bool),
		servers:      make(map[string]*transport.Server),
		connections:  make(map[string]string),
		serverCounts: make(map[string]int),
		done:         make(chan struct{}),
	}
}

// Close shuts the network down. All subsequent calls fail immediately.
func (n *Network) Close() {
	n.closeOnce.Do(func() { close(n.done) })
}

// SetReliable controls whether messages are dropped and delayed.
func (n *Network) SetReliable(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = yes
}

// SetLongReordering controls whether some replies are held back for a long
// time, so that they arrive after much newer messages.
func (n *Network) SetLongReordering(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longReordering = yes
}

// SetLongDelays controls whether calls to unreachable servers hang for a
// long time before failing, rather than failing quickly.
func (n *Network) SetLongDelays(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longDelays = yes
}

// MakeEnd creates a new client endpoint. It starts disabled and
// unconnected.
func (n *Network) MakeEnd(name string) *ClientEnd {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.ends[name]; ok {
		panic("simnet: duplicate end name " + name)
	}
	e := &ClientEnd{name: name, net: n}
	n.ends[name] = e
	n.enabled[name] = false
	return e
}

// DeleteEnd removes an endpoint. Calls on it fail from now on.
func (n *Network) DeleteEnd(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.ends, name)
	delete(n.enabled, name)
	delete(n.connections, name)
}

// AddServer registers srv under name, replacing any previous server with that
// name. Replies from the replaced server are discarded.
func (n *Network) AddServer(name string, srv *transport.Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[name] = srv
}

// DeleteServer removes the server called name, simulating a crash.
func (n *Network) DeleteServer(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.servers, name)
}

// Connect routes calls made on the end called endName to serverName.
func (n *Network) Connect(endName, serverName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connections[endName] = serverName
}

// Enable allows or blocks traffic on an endpoint.
func (n *Network) Enable(endName string, enabled bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.enabled[endName] = enabled
}

// ServerCount returns how many RPCs were delivered to serverName.
func (n *Network) ServerCount(serverName string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.serverCounts[serverName]
}

// TotalCalls returns the number of RPCs attempted on the network.
func (n *Network) TotalCalls() int64 { return n.totalCalls.Load() }

// TotalBytes returns the number of request bytes sent on the network.
func (n *Network) TotalBytes() int64 { return n.totalBytes.Load() }

func (n *Network) closed() bool {
	select {
	case <-n.done:
		return true
	default:
		return false
	}
}

// route reports where a call on endName should go right now.
func (n *Network) route(endName string) (srv *transport.Server, serverName string, enabled, reliable, longReordering, longDelays bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	serverName = n.connections[endName]
	srv = n.servers[serverName]
	enabled = n.enabled[endName]
	if enabled && srv != nil {
		n.serverCounts[serverName]++
	}
	return srv, serverName, enabled, n.reliable, n.longReordering, n.longDelays
}

// dead reports whether a call on endName to srv can no longer complete,
// because the endpoint was disabled or the server crashed or was replaced.
func (n *Network) dead(endName, serverName string, srv *transport.Server) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.enabled[endName] || n.servers[serverName] != srv
}

// ClientEnd is one side of a simulated connection. It implements
// transport.Caller.
type ClientEnd struct {
	name string
	net  *Network
}

type dispatchResult struct {
	resp []byte
	err  error
}

// Call implements transport.Caller.
func (e *ClientEnd) Call(method string, args any, reply any) bool {
	n := e.net
	if n.closed() {
		return false
	}
	req, err := transport.Encode(args)
	if err != nil {
		panic("simnet: cannot encode args for " + method + ": " + err.Error())
	}
	n.totalCalls.Add(1)
	n.totalBytes.Add(int64(len(req)))

	srv, serverName, enabled, reliable, longReordering, longDelays := n.route(e.name)
	if !enabled || srv == nil {
		// The server is unreachable. Simulate a timeout of random length.
		maxMs := 100
		if longDelays {
			maxMs = 2000
		}
		e.sleep(time.Duration(rand.IntN(maxMs)) * time.Millisecond)
		return false
	}

	if !reliable {
		// Short random delay; this alone reorders concurrent messages.
		e.sleep(time.Duration(rand.IntN(27)) * time.Millisecond)
		if rand.IntN(1000) < 100 {
			return false // request dropped
		}
	}

	// Run the handler in its own goroutine so that a crash of the server
	// (or a partition) while the request is executing is noticed promptly.
	ch := make(chan dispatchResult, 1)
	go func() {
		resp, err := srv.Dispatch(method, req)
		ch <- dispatchResult{resp, err}
	}()

	var res dispatchResult
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
wait:
	for {
		select {
		case res = <-ch:
			break wait
		case <-ticker.C:
			if n.dead(e.name, serverName, srv) {
				return false
			}
		case <-n.done:
			return false
		}
	}

	// A server that crashed or was partitioned away during the call must
	// not be able to deliver its reply.
	if n.dead(e.name, serverName, srv) || res.err != nil {
		return false
	}
	if !reliable && rand.IntN(1000) < 100 {
		return false // reply dropped
	}
	if longReordering && rand.IntN(900) < 600 {
		// Deliver the reply much later than usual, after newer traffic.
		e.sleep(time.Duration(200+rand.IntN(1+rand.IntN(1500))) * time.Millisecond)
	}
	return transport.Decode(res.resp, reply) == nil
}

func (e *ClientEnd) sleep(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-e.net.done:
	}
}
