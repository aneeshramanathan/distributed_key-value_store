package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/raft"
	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

// Server is one replica of the key/value store.
type Server struct {
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxRaftState int // snapshot when the Raft log reaches this many bytes; <= 0 disables
	opTimeout    time.Duration
	dead         atomic.Bool
	stopCh       chan struct{}
	doneCh       chan struct{}

	mu          sync.Mutex
	data        map[string]string
	lastSeq     map[int64]int64 // client ID -> highest applied write sequence number
	lastApplied int
	waiters     map[int]chan applyResult // log index -> handler waiting for it
}

type applyResult struct {
	term  int
	value string
	found bool
}

// snapshotState is the state machine's serialized form.
type snapshotState struct {
	Data    map[string]string
	LastSeq map[int64]int64
}

// NewServer starts a replica. peers are transports to every server
// (including this one, which is ignored); storage holds this server's Raft
// state and snapshots.
func NewServer(peers []transport.Caller, me int, storage raft.Storage, maxRaftState int, cfg raft.Config) (*Server, error) {
	kv := &Server{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 64),
		maxRaftState: maxRaftState,
		opTimeout:    2 * time.Second,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		data:         make(map[string]string),
		lastSeq:      make(map[int64]int64),
		waiters:      make(map[int]chan applyResult),
	}
	rf, err := raft.New(peers, me, storage, kv.applyCh, cfg)
	if err != nil {
		return nil, err
	}
	kv.rf = rf
	go kv.applyLoop()
	return kv, nil
}

// Raft returns the underlying Raft node.
func (kv *Server) Raft() *raft.Raft { return kv.rf }

// Kill stops the server and its Raft node.
func (kv *Server) Kill() {
	if kv.dead.Swap(true) {
		return
	}
	kv.rf.Kill()
	close(kv.stopCh)
	<-kv.doneCh
}

// Register exposes the server's KV.Do RPC on srv, and its Raft RPCs.
func Register(srv *transport.Server, kv *Server) {
	raft.RegisterRPC(srv, kv.rf)
	transport.Register(srv, "KV.Do", kv.Do)
}

// Do is the RPC handler for client operations.
func (kv *Server) Do(args *Args, reply *Reply) {
	reply.Value, reply.Err = kv.Execute(args.Op)
	reply.LeaderHint = kv.rf.Status().LeaderID
}

// Execute runs op through the replicated log and returns its result once it
// has been applied. It returns ErrWrongLeader if this server is not the
// leader or loses leadership before the op commits; the caller should retry
// elsewhere. ErrTimeout means the outcome is unknown and the caller should
// retry (duplicate detection makes retried writes safe).
func (kv *Server) Execute(op Op) (string, Err) {
	if kv.dead.Load() {
		return "", ErrShutdown
	}
	if op.Kind != OpGet && op.ClientID != 0 {
		kv.mu.Lock()
		dup := kv.lastSeq[op.ClientID] >= op.Seq
		kv.mu.Unlock()
		if dup {
			return "", OK // already applied, so already committed
		}
	}

	index, term, isLeader := kv.rf.Start(encodeOp(op))
	if !isLeader {
		return "", ErrWrongLeader
	}

	ch := make(chan applyResult, 1)
	kv.mu.Lock()
	kv.waiters[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		if kv.waiters[index] == ch {
			delete(kv.waiters, index)
		}
		kv.mu.Unlock()
	}()

	timeout := time.NewTimer(kv.opTimeout)
	defer timeout.Stop()
	check := time.NewTicker(50 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case res := <-ch:
			// Log matching: the entry at index has our term iff it is our
			// entry. Otherwise a new leader overwrote it.
			if res.term != term {
				return "", ErrWrongLeader
			}
			if op.Kind == OpGet && !res.found {
				return "", ErrNoKey
			}
			return res.value, OK
		case <-check.C:
			// Lost leadership: the entry may or may not commit. Let the
			// client retry; duplicate detection makes that safe.
			if t, isLeader := kv.rf.GetState(); t != term || !isLeader {
				return "", ErrWrongLeader
			}
		case <-timeout.C:
			return "", ErrTimeout
		case <-kv.stopCh:
			return "", ErrShutdown
		}
	}
}

// applyLoop executes committed operations in log order.
func (kv *Server) applyLoop() {
	defer close(kv.doneCh)
	for {
		var msg raft.ApplyMsg
		select {
		case msg = <-kv.applyCh:
		case <-kv.stopCh:
			return
		}

		kv.mu.Lock()
		switch {
		case msg.SnapshotValid:
			if msg.SnapshotIndex > kv.lastApplied {
				kv.restore(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
		case msg.CommandValid:
			if msg.CommandIndex <= kv.lastApplied {
				break // already reflected in an installed snapshot
			}
			if msg.CommandIndex != kv.lastApplied+1 {
				panic(fmt.Sprintf("kv %d: applied index %d after %d", kv.me, msg.CommandIndex, kv.lastApplied))
			}
			kv.lastApplied = msg.CommandIndex
			res := applyResult{term: msg.CommandTerm}
			if len(msg.Command) > 0 { // empty: a leader's no-op
				op, err := decodeOp(msg.Command)
				if err != nil {
					panic(fmt.Sprintf("kv %d: index %d: %v", kv.me, msg.CommandIndex, err))
				}
				res.value, res.found = kv.apply(op)
			}
			if ch, ok := kv.waiters[msg.CommandIndex]; ok {
				ch <- res
				delete(kv.waiters, msg.CommandIndex)
			}
			if kv.maxRaftState > 0 && kv.rf.StateSize() >= kv.maxRaftState {
				kv.rf.Snapshot(msg.CommandIndex, kv.snapshot())
			}
		}
		kv.mu.Unlock()
	}
}

// apply executes op against the state machine. Caller holds mu.
func (kv *Server) apply(op Op) (string, bool) {
	switch op.Kind {
	case OpGet:
		v, ok := kv.data[op.Key]
		return v, ok
	case OpPut, OpDelete:
		if op.ClientID != 0 {
			if kv.lastSeq[op.ClientID] >= op.Seq {
				return "", true // duplicate of a write we already applied
			}
			kv.lastSeq[op.ClientID] = op.Seq
		}
		if op.Kind == OpPut {
			kv.data[op.Key] = op.Value
		} else {
			delete(kv.data, op.Key)
		}
		return "", true
	}
	panic(fmt.Sprintf("kv: unknown op kind %d", op.Kind))
}

func (kv *Server) snapshot() []byte {
	data, err := transport.Encode(snapshotState{Data: kv.data, LastSeq: kv.lastSeq})
	if err != nil {
		panic(err)
	}
	return data
}

func (kv *Server) restore(b []byte) {
	var s snapshotState
	if err := transport.Decode(b, &s); err != nil {
		panic(fmt.Sprintf("kv %d: bad snapshot: %v", kv.me, err))
	}
	if s.Data == nil {
		s.Data = make(map[string]string)
	}
	if s.LastSeq == nil {
		s.LastSeq = make(map[int64]int64)
	}
	kv.data, kv.lastSeq = s.Data, s.LastSeq
}
