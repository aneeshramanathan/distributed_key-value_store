// Package node runs one replica of the key/value store as a network service.
//
// A node serves, on a single HTTP listener:
//
//	GET    /kv/{key}   read a key      200 + value | 404
//	PUT    /kv/{key}   write a key     204 (request body is the value)
//	DELETE /kv/{key}   delete a key    204
//	GET    /status     JSON view of the node's Raft state
//	POST   /rpc/...    Raft and KV RPCs between nodes (gob over HTTP)
//
// Only the leader serves client requests. Any other node answers with a
// 307 redirect to the leader when it knows who that is (curl -L follows it),
// or 503 when there is no leader (for example, mid-election).
//
// Clients may send X-Client-ID and X-Request-Seq headers, which enable
// exactly-once execution of retried writes. The Go client in package client
// does this automatically.
package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/kv"
	"github.com/aneeshramanathan/distributed_key-value_store/raft"
	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

// Config configures a node.
type Config struct {
	ID      int      // index of this node in Peers
	Peers   []string // base URLs of every node, e.g. "http://10.0.0.1:7001"
	DataDir string   // where the Raft log and snapshots live

	// Listener, if set, is used instead of listening on Peers[ID]'s port.
	Listener net.Listener

	Raft         raft.Config
	MaxRaftState int           // snapshot once the log reaches this many bytes (<= 0: never)
	RPCTimeout   time.Duration // timeout for node-to-node RPCs
	NoSync       bool          // skip fsync (benchmarking only; unsafe)
	MaxValueSize int64
}

// DefaultRaftConfig is tuned for a LAN: heartbeats every 50 ms and election
// timeouts of 250-500 ms, comfortably above round-trip times.
func DefaultRaftConfig() raft.Config {
	return raft.Config{
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 500 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		MaxEntriesPerRPC:   1024,
	}
}

func (c *Config) setDefaults() {
	if c.Raft == (raft.Config{}) {
		c.Raft = DefaultRaftConfig()
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = time.Second
	}
	if c.MaxRaftState == 0 {
		c.MaxRaftState = 4 << 20
	}
	if c.MaxValueSize == 0 {
		c.MaxValueSize = 1 << 20
	}
}

// Node is a running replica.
type Node struct {
	cfg     Config
	kv      *kv.Server
	storage *raft.FileStorage
	callers []*transport.HTTPCaller
	srv     *http.Server
	ln      net.Listener
}

// Start opens the node's storage, recovers its state, and starts serving.
func Start(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if cfg.ID < 0 || cfg.ID >= len(cfg.Peers) {
		return nil, fmt.Errorf("node id %d out of range for %d peers", cfg.ID, len(cfg.Peers))
	}
	ln := cfg.Listener
	if ln == nil {
		u, err := url.Parse(cfg.Peers[cfg.ID])
		if err != nil {
			return nil, fmt.Errorf("bad peer URL %q: %w", cfg.Peers[cfg.ID], err)
		}
		if ln, err = net.Listen("tcp", ":"+u.Port()); err != nil {
			return nil, err
		}
	}

	storage, err := raft.OpenFileStorage(cfg.DataDir)
	if err != nil {
		ln.Close()
		return nil, err
	}
	storage.NoSync = cfg.NoSync

	n := &Node{cfg: cfg, storage: storage, ln: ln}
	peers := make([]transport.Caller, len(cfg.Peers))
	for i, p := range cfg.Peers {
		c := transport.NewHTTPCaller(p, cfg.RPCTimeout)
		n.callers = append(n.callers, c)
		peers[i] = c
	}
	n.kv, err = kv.NewServer(peers, cfg.ID, storage, cfg.MaxRaftState, cfg.Raft)
	if err != nil {
		ln.Close()
		storage.Close()
		return nil, err
	}

	rpc := transport.NewServer()
	kv.Register(rpc, n.kv)
	mux := http.NewServeMux()
	mux.Handle(transport.RPCPathPrefix, rpc)
	mux.HandleFunc("GET /kv/{key...}", n.handleKV(kv.OpGet))
	mux.HandleFunc("PUT /kv/{key...}", n.handleKV(kv.OpPut))
	mux.HandleFunc("DELETE /kv/{key...}", n.handleKV(kv.OpDelete))
	mux.HandleFunc("GET /status", n.handleStatus)
	n.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go n.srv.Serve(ln)
	return n, nil
}

// Addr returns the address the node is listening on.
func (n *Node) Addr() string { return n.ln.Addr().String() }

// Status returns the node's Raft status.
func (n *Node) Status() raft.Status { return n.kv.Raft().Status() }

// Close stops the node abruptly, as a crash would: connections are dropped
// and nothing is flushed beyond what Raft already made durable.
func (n *Node) Close() error {
	n.srv.Close()
	n.kv.Kill()
	for _, c := range n.callers {
		c.Close()
	}
	return n.storage.Close()
}

func (n *Node) handleKV(kind kv.OpKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		op := kv.Op{Kind: kind, Key: r.PathValue("key")}
		if op.Key == "" {
			http.Error(w, "empty key", http.StatusBadRequest)
			return
		}
		if kind == kv.OpPut {
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, n.cfg.MaxValueSize))
			if err != nil {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			op.Value = string(body)
		}
		if id := r.Header.Get("X-Client-ID"); id != "" {
			var err1, err2 error
			op.ClientID, err1 = strconv.ParseInt(id, 10, 64)
			op.Seq, err2 = strconv.ParseInt(r.Header.Get("X-Request-Seq"), 10, 64)
			if err := errors.Join(err1, err2); err != nil {
				http.Error(w, "bad X-Client-ID / X-Request-Seq: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		value, err := n.kv.Execute(op)
		switch err {
		case kv.OK:
			if kind == kv.OpGet {
				w.Header().Set("Content-Type", "application/octet-stream")
				io.WriteString(w, value)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case kv.ErrNoKey:
			http.Error(w, "key not found", http.StatusNotFound)
		case kv.ErrWrongLeader:
			n.redirectToLeader(w, r)
		default: // ErrTimeout, ErrShutdown
			w.Header().Set("Retry-After", "1")
			http.Error(w, string(err), http.StatusServiceUnavailable)
		}
	}
}

func (n *Node) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := n.Status().LeaderID
	if leader < 0 || leader == n.cfg.ID {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader", http.StatusServiceUnavailable)
		return
	}
	target := strings.TrimRight(n.cfg.Peers[leader], "/") + r.URL.RequestURI()
	w.Header().Set("X-Raft-Leader", strconv.Itoa(leader))
	http.Redirect(w, r, target, http.StatusTemporaryRedirect)
}

// StatusResponse is the body of GET /status.
type StatusResponse struct {
	ID            int    `json:"id"`
	Role          string `json:"role"`
	Term          int    `json:"term"`
	LeaderID      int    `json:"leader_id"`
	Leader        string `json:"leader,omitempty"`
	CommitIndex   int    `json:"commit_index"`
	LastApplied   int    `json:"last_applied"`
	LastLogIndex  int    `json:"last_log_index"`
	SnapshotIndex int    `json:"snapshot_index"`
	LogBytes      int    `json:"log_bytes"`
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := n.Status()
	resp := StatusResponse{
		ID:            st.ID,
		Role:          st.Role.String(),
		Term:          st.Term,
		LeaderID:      st.LeaderID,
		CommitIndex:   st.CommitIndex,
		LastApplied:   st.LastApplied,
		LastLogIndex:  st.LastLogIndex,
		SnapshotIndex: st.SnapshotIndex,
		LogBytes:      n.storage.Size(),
	}
	if st.LeaderID >= 0 {
		resp.Leader = n.cfg.Peers[st.LeaderID]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
