// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014).
//
// A Raft instance replicates an opaque log of []byte commands across a
// cluster. The service built on top (see package kv) submits commands with
// Start and learns that they are committed, in log order, from the apply
// channel. Implemented features:
//
//   - leader election with randomized timeouts (§5.2)
//   - log replication with the accelerated conflict back-off (§5.3)
//   - commitment restricted to current-term entries (§5.4.2, Figure 8)
//   - durable state via a pluggable Storage, with group commit: the leader
//     replicates entries to followers while fsyncing them locally, and counts
//     itself towards a majority only once its own copy is durable
//   - log compaction via snapshots and InstallSnapshot (§7)
//   - a no-op entry at the start of each term, so a new leader learns which
//     entries are committed without waiting for a client write
//   - PreVote (thesis §9.6), so a node rejoining after a partition cannot
//     depose a healthy leader by inflating its term
//   - CheckQuorum, so a leader cut off from a majority steps down instead
//     of accepting requests it can never commit
package raft

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

// Role is a node's role in the current term.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// ApplyMsg is sent on the apply channel for every committed entry, and when
// the service must replace its state with a snapshot.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte // nil for a leader's no-op entry; the service skips it
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Config holds timing parameters.
type Config struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	// MaxEntriesPerRPC caps the size of one AppendEntries batch.
	MaxEntriesPerRPC int
}

// DefaultConfig suits the simulated network, whose message delays are up to
// tens of milliseconds.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		MaxEntriesPerRPC:   512,
	}
}

// Status is a point-in-time view of a node, for monitoring and tests.
type Status struct {
	ID            int
	Term          int
	Role          Role
	LeaderID      int // -1 if unknown
	CommitIndex   int
	LastApplied   int
	LastLogIndex  int
	SnapshotIndex int
}

// Raft is one member of a Raft cluster.
type Raft struct {
	mu      sync.Mutex
	peers   []transport.Caller
	me      int
	storage Storage
	cfg     Config
	applyCh chan<- ApplyMsg
	dead    atomic.Bool
	stopCh  chan struct{}

	// Persistent state (mirrored in storage).
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a sentinel holding the snapshot's index and term
	snapshot    []byte
	persistedHS HardState

	// Volatile state.
	role             Role
	leaderID         int
	commitIndex      int
	lastApplied      int
	syncedIndex      int // highest index known to be durable in our own storage
	electionDeadline time.Time
	heartbeatDue     time.Time
	lastLeaderMsg    time.Time // when we last heard from a current leader
	preVoteRound     int       // distinguishes replies to successive pre-votes
	pendingSnapshot  bool      // the applier must deliver snapshot before more entries

	// Leader state, reinitialized after each election.
	nextIndex  []int
	matchIndex []int
	inflight   []bool      // an entry-carrying RPC is outstanding to this peer
	pending    []bool      // new entries arrived while inflight was set
	lastAck    []time.Time // when each peer last answered us in this term

	applyCond *sync.Cond
	syncCh    chan struct{}
}

// New creates a Raft node and starts its background goroutines.
//
// peers[i] is the transport to node i; peers[me] is never used. State is
// recovered from storage. Committed entries, and snapshots, are delivered on
// applyCh in log order. If storage holds a snapshot, it is delivered first.
func New(peers []transport.Caller, me int, storage Storage, applyCh chan<- ApplyMsg, cfg Config) (*Raft, error) {
	if cfg.MaxEntriesPerRPC <= 0 {
		cfg.MaxEntriesPerRPC = 512
	}
	hs, meta, entries, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	rf := &Raft{
		peers:       peers,
		me:          me,
		storage:     storage,
		cfg:         cfg,
		applyCh:     applyCh,
		stopCh:      make(chan struct{}),
		currentTerm: hs.Term,
		votedFor:    hs.VotedFor,
		persistedHS: hs,
		log:         append([]LogEntry{{Index: meta.Index, Term: meta.Term}}, entries...),
		snapshot:    storage.Snapshot(),
		role:        Follower,
		leaderID:    -1,
		commitIndex: meta.Index,
		lastApplied: meta.Index,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		inflight:    make([]bool, len(peers)),
		pending:     make([]bool, len(peers)),
		lastAck:     make([]time.Time, len(peers)),
		syncCh:      make(chan struct{}, 1),
	}
	rf.syncedIndex = rf.lastIndex()
	rf.pendingSnapshot = meta.Index > 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.applier()
	go rf.syncer()
	return rf, nil
}

// Start proposes command for the log. If this node is not the leader it
// returns isLeader=false. Otherwise it returns the index the command will
// occupy if it is ever committed, and the current term. There is no
// guarantee it will be committed: leadership may be lost first.
func (rf *Raft) Start(command []byte) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	e := LogEntry{Index: rf.lastIndex() + 1, Term: rf.currentTerm, Command: command}
	rf.appendLocal(e)
	rf.broadcast(false)
	return e.Index, e.Term, true
}

// GetState returns the current term and whether this node believes it is
// the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Status returns a snapshot of the node's state.
func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		ID:            rf.me,
		Term:          rf.currentTerm,
		Role:          rf.role,
		LeaderID:      rf.leaderID,
		CommitIndex:   rf.commitIndex,
		LastApplied:   rf.lastApplied,
		LastLogIndex:  rf.lastIndex(),
		SnapshotIndex: rf.firstIndex(),
	}
}

// StateSize returns the size of the persisted log, so the service can
// decide when to snapshot.
func (rf *Raft) StateSize() int { return rf.storage.Size() }

// Snapshot tells Raft that the service has captured its state through
// index (inclusive) in data. Raft discards the log up to index.
func (rf *Raft) Snapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || index <= rf.firstIndex() || index > rf.commitIndex {
		return
	}
	term := rf.term(index)
	rest := rf.log[index-rf.firstIndex()+1:]
	rf.log = append([]LogEntry{{Index: index, Term: term}}, rest...)
	rf.snapshot = data
	rf.must(rf.storage.SaveSnapshot(SnapshotMeta{Index: index, Term: term}, data, rf.hardState(), rf.log[1:]))
	rf.persistedHS = rf.hardState()
}

// Kill stops the node. It stops sending messages and delivering entries;
// handlers invoked afterwards do nothing.
func (rf *Raft) Kill() {
	if rf.dead.Swap(true) {
		return
	}
	close(rf.stopCh)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return rf.dead.Load() }

// must panics on a storage error. A node that cannot persist state must not
// keep participating, because it could forget votes or acknowledged
// entries. Errors after Kill are expected (storage may already be closed).
func (rf *Raft) must(err error) {
	if err != nil && !rf.killed() {
		panic(fmt.Sprintf("raft %d: storage failure: %v", rf.me, err))
	}
}

// ---- log helpers (caller holds mu) ----

func (rf *Raft) firstIndex() int { return rf.log[0].Index }
func (rf *Raft) lastIndex() int  { return rf.log[len(rf.log)-1].Index }
func (rf *Raft) lastTerm() int   { return rf.log[len(rf.log)-1].Term }
func (rf *Raft) term(i int) int  { return rf.log[i-rf.firstIndex()].Term }

func (rf *Raft) hardState() HardState {
	return HardState{Term: rf.currentTerm, VotedFor: rf.votedFor}
}

// persistHardState durably records the term and vote if they changed. It
// must run before any message that depends on them is sent.
func (rf *Raft) persistHardState() {
	hs := rf.hardState()
	if hs == rf.persistedHS {
		return
	}
	rf.must(rf.storage.SetHardState(hs))
	rf.persistedHS = hs
}

// appendLocal appends a leader-created entry and asks the syncer to make it
// durable. The leader may replicate it before it is durable locally.
func (rf *Raft) appendLocal(e LogEntry) {
	rf.log = append(rf.log, e)
	rf.must(rf.storage.Append([]LogEntry{e}))
	select {
	case rf.syncCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) resetElectionTimer() {
	spread := rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin
	d := rf.cfg.ElectionTimeoutMin
	if spread > 0 {
		d += time.Duration(rand.Int64N(int64(spread)))
	}
	rf.electionDeadline = time.Now().Add(d)
}

// becomeFollower moves to term (if newer) as a follower.
func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.leaderID = -1
	}
	rf.role = Follower
	rf.persistHardState()
}

// ---- background goroutines ----

// ticker drives elections and heartbeats.
func (rf *Raft) ticker() {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-rf.stopCh:
			return
		case <-tick.C:
		}
		rf.mu.Lock()
		now := time.Now()
		switch {
		case rf.role == Leader && !rf.hasQuorum(now):
			// CheckQuorum: a leader cut off from a majority steps down
			// rather than accepting requests it can never commit.
			rf.role = Follower
			rf.leaderID = -1
			rf.resetElectionTimer()
		case rf.role == Leader && !now.Before(rf.heartbeatDue):
			rf.heartbeatDue = now.Add(rf.cfg.HeartbeatInterval)
			rf.broadcast(true)
		case rf.role != Leader && !now.Before(rf.electionDeadline):
			rf.startPreVote()
		}
		rf.mu.Unlock()
	}
}

// hasQuorum reports whether a majority (counting ourselves) has answered
// within the last maximum election timeout.
func (rf *Raft) hasQuorum(now time.Time) bool {
	count := 1
	for p := range rf.peers {
		if p != rf.me && now.Sub(rf.lastAck[p]) < rf.cfg.ElectionTimeoutMax {
			count++
		}
	}
	return count > len(rf.peers)/2
}

// syncer makes the leader's newly appended entries durable in batches
// (group commit) and then lets them count towards a majority.
func (rf *Raft) syncer() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.syncCh:
		}
		rf.mu.Lock()
		target, term, leader := rf.lastIndex(), rf.currentTerm, rf.role == Leader
		rf.mu.Unlock()
		if !leader {
			continue // followers sync inline before acknowledging
		}
		rf.must(rf.storage.Sync())
		rf.mu.Lock()
		// Within one term a leader's log only grows, so everything up to
		// target is still in the log and now durable.
		if rf.currentTerm == term && rf.role == Leader && target > rf.syncedIndex {
			rf.syncedIndex = target
			rf.advanceCommit()
		}
		rf.mu.Unlock()
	}
}

// applier delivers committed entries and snapshots to the service, in
// order, without holding the lock while blocked on the channel.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && !rf.pendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.pendingSnapshot {
			rf.pendingSnapshot = false
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: rf.firstIndex(),
				SnapshotTerm:  rf.log[0].Term,
			}
			rf.mu.Unlock()
			if !rf.deliver(msg) {
				return
			}
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, msg.SnapshotIndex)
			rf.mu.Unlock()
			continue
		}
		if rf.lastApplied < rf.firstIndex() {
			panic(fmt.Sprintf("raft %d: lastApplied %d behind snapshot %d", rf.me, rf.lastApplied, rf.firstIndex()))
		}
		start, end := rf.lastApplied+1, rf.commitIndex
		batch := slices.Clone(rf.log[start-rf.firstIndex() : end-rf.firstIndex()+1])
		rf.mu.Unlock()
		for _, e := range batch {
			if !rf.deliver(ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index, CommandTerm: e.Term}) {
				return
			}
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, end)
		rf.mu.Unlock()
	}
}

func (rf *Raft) deliver(msg ApplyMsg) bool {
	select {
	case rf.applyCh <- msg:
		return true
	case <-rf.stopCh:
		return false
	}
}

// ---- leader election ----

// startPreVote asks the other nodes whether they would vote for us, before
// we disturb the cluster by incrementing our term. A node that was
// partitioned away keeps failing pre-votes, so it does not inflate its term
// and force a healthy leader to step down when it reconnects.
func (rf *Raft) startPreVote() {
	rf.resetElectionTimer()
	if len(rf.peers) == 1 {
		rf.startElection()
		return
	}
	rf.preVoteRound++
	round, term := rf.preVoteRound, rf.currentTerm
	args := RequestVoteArgs{
		Term:         term + 1,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
		PreVote:      true,
	}
	grants := 1
	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if !rf.peers[p].Call("Raft.RequestVote", &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.role == Leader || rf.currentTerm != term || rf.preVoteRound != round || !reply.VoteGranted {
				return
			}
			grants++
			if grants == len(rf.peers)/2+1 {
				rf.startElection()
			}
		}(p)
	}
}

func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.persistHardState()
	rf.resetElectionTimer()

	if len(rf.peers) == 1 {
		rf.becomeLeader()
		return
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	votes := 1
	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if !rf.peers[p].Call("Raft.RequestVote", &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.role != Candidate || rf.currentTerm != args.Term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}(p)
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.leaderID = rf.me
	now := time.Now()
	for p := range rf.peers {
		rf.nextIndex[p] = rf.lastIndex() + 1
		rf.matchIndex[p] = 0
		rf.inflight[p] = false
		rf.pending[p] = false
		rf.lastAck[p] = now // grace period before CheckQuorum applies
	}
	// Committing a no-op from our own term also commits every earlier
	// entry (§5.4.2), so the leader knows the commit point promptly.
	rf.appendLocal(LogEntry{Index: rf.lastIndex() + 1, Term: rf.currentTerm})
	rf.heartbeatDue = time.Now().Add(rf.cfg.HeartbeatInterval)
	rf.broadcast(false)
}

// RequestVote handles a candidate's vote request.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	// §5.4.1: only vote for a candidate whose log is at least as
	// up-to-date as ours, so that any leader holds all committed entries.
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())

	if args.PreVote {
		// Answer hypothetically; change nothing. Refuse while we have a
		// live leader, so a reconnecting node cannot depose it.
		reply.Term = rf.currentTerm
		leaderAlive := rf.role == Leader || time.Since(rf.lastLeaderMsg) < rf.cfg.ElectionTimeoutMin
		reply.VoteGranted = args.Term >= rf.currentTerm && upToDate && !leaderAlive
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persistHardState()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
}

// ---- log replication ----

// broadcast sends to every peer. A heartbeat round always reaches every
// peer; otherwise, a peer that already has an entry-carrying RPC in flight
// is just marked as needing another one when that completes.
func (rf *Raft) broadcast(heartbeat bool) {
	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		if heartbeat && rf.inflight[p] {
			rf.sendTo(p, true)
		} else {
			rf.sendTo(p, false)
		}
	}
	if len(rf.peers) == 1 {
		rf.advanceCommit()
	}
}

// sendTo sends one AppendEntries or InstallSnapshot to peer. With
// heartbeatOnly it carries no entries and does not take the inflight slot.
func (rf *Raft) sendTo(p int, heartbeatOnly bool) {
	if !heartbeatOnly {
		if rf.inflight[p] {
			rf.pending[p] = true
			return
		}
		rf.inflight[p] = true
		rf.pending[p] = false
	}

	if !heartbeatOnly && rf.nextIndex[p] <= rf.firstIndex() {
		// The entries the follower needs were compacted away.
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.firstIndex(),
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.snapshot,
		}
		go rf.sendSnapshot(p, &args)
		return
	}

	prev := max(rf.nextIndex[p]-1, rf.firstIndex())
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.term(prev),
		LeaderCommit: rf.commitIndex,
	}
	if !heartbeatOnly {
		end := min(rf.lastIndex(), prev+rf.cfg.MaxEntriesPerRPC)
		// Copy: the log's backing array may be overwritten after a
		// truncation while this RPC is still being encoded.
		args.Entries = slices.Clone(rf.log[prev+1-rf.firstIndex() : end+1-rf.firstIndex()])
	}
	go rf.sendAppend(p, &args, heartbeatOnly)
}

func (rf *Raft) sendAppend(p int, args *AppendEntriesArgs, heartbeatOnly bool) {
	var reply AppendEntriesReply
	ok := rf.peers[p].Call("Raft.AppendEntries", args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !heartbeatOnly && rf.currentTerm == args.Term {
		rf.inflight[p] = false
	}
	if !ok || rf.killed() {
		if !heartbeatOnly {
			rf.retrySoon(p, args.Term)
		}
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	rf.lastAck[p] = time.Now()

	if reply.Success {
		// Replies can arrive out of order, so only ever move forward.
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[p] = max(rf.matchIndex[p], match)
		rf.nextIndex[p] = max(rf.nextIndex[p], match+1)
		rf.advanceCommit()
	} else {
		rf.backoff(p, &reply)
	}

	if !heartbeatOnly && (rf.pending[p] || rf.nextIndex[p] <= rf.lastIndex()) {
		rf.sendTo(p, false)
	}
}

// retrySoon schedules a quick resend after a lost message to a peer that
// answered recently, since the loss was probably transient. Peers that have
// been silent are left to the next heartbeat so a dead node is not flooded.
func (rf *Raft) retrySoon(p, term int) {
	if rf.role != Leader || rf.currentTerm != term || time.Since(rf.lastAck[p]) > rf.cfg.HeartbeatInterval*2 {
		return
	}
	time.AfterFunc(rf.cfg.HeartbeatInterval/10, func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !rf.killed() && rf.role == Leader && rf.currentTerm == term && !rf.inflight[p] {
			rf.sendTo(p, false)
		}
	})
}

// backoff lowers nextIndex after a consistency-check failure, skipping a
// whole conflicting term at a time instead of one entry per round trip.
func (rf *Raft) backoff(p int, reply *AppendEntriesReply) {
	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		// If we have entries from the conflicting term, the follower's log
		// may agree with ours up to our last one of them.
		for i := rf.lastIndex(); i > rf.firstIndex(); i-- {
			t := rf.term(i)
			if t == reply.ConflictTerm {
				next = i + 1
				break
			}
			if t < reply.ConflictTerm {
				break
			}
		}
	}
	// Never back up past what the follower is known to hold.
	rf.nextIndex[p] = max(min(next, rf.lastIndex()+1), rf.matchIndex[p]+1, 1)
}

// advanceCommit commits the highest index stored on a majority, provided it
// belongs to the current term (§5.4.2).
func (rf *Raft) advanceCommit() {
	if rf.role != Leader {
		return
	}
	matches := make([]int, 0, len(rf.peers))
	for p := range rf.peers {
		if p == rf.me {
			matches = append(matches, rf.syncedIndex)
		} else {
			matches = append(matches, rf.matchIndex[p])
		}
	}
	slices.Sort(matches)
	// matches[(n-1)/2] is held by at least n/2+1 nodes.
	n := matches[(len(matches)-1)/2]
	if n > rf.commitIndex && n > rf.firstIndex() && rf.term(n) == rf.currentTerm {
		rf.commitIndex = n
		rf.applyCond.Signal()
	}
}

// AppendEntries handles log replication and heartbeats from the leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.becomeFollower(args.Term)
	}
	rf.leaderID = args.LeaderID
	rf.lastLeaderMsg = time.Now()
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	prev, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prev < rf.firstIndex() {
		// The start of this batch is already in our snapshot. Snapshots
		// hold only committed entries, which match the leader's log, so
		// skip that part and check consistency at the snapshot point.
		skip := min(rf.firstIndex()-prev, len(entries))
		entries = entries[skip:]
		prev, prevTerm = rf.firstIndex(), rf.log[0].Term
	}

	if prev > rf.lastIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIndex() + 1
		return
	}
	if t := rf.term(prev); t != prevTerm {
		// Report the conflicting term and where it starts in our log, so
		// the leader can skip the whole term in one step.
		reply.ConflictTerm = t
		i := prev
		for i > rf.firstIndex()+1 && rf.term(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Find the first entry we do not already have, truncate any conflicting
	// suffix there, and append the rest. Entries we already have are left
	// alone: this request may be older than one we have processed.
	for i, e := range entries {
		if e.Index <= rf.lastIndex() && rf.term(e.Index) == e.Term {
			continue
		}
		if e.Index <= rf.lastIndex() {
			if e.Index <= rf.commitIndex {
				panic(fmt.Sprintf("raft %d: truncating committed entry %d (commit %d)", rf.me, e.Index, rf.commitIndex))
			}
			rf.log = rf.log[:e.Index-rf.firstIndex()]
			rf.syncedIndex = min(rf.syncedIndex, e.Index-1)
		}
		rest := entries[i:]
		rf.log = append(rf.log, rest...)
		rf.must(rf.storage.Append(rest))
		break
	}

	// Acknowledging means promising the entries are on stable storage.
	// (They may have been appended earlier, unsynced, while we were leader.)
	lastNew := prev + len(entries)
	if rf.syncedIndex < lastNew {
		rf.must(rf.storage.Sync())
		rf.syncedIndex = rf.lastIndex()
	}

	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastNew)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendSnapshot(p int, args *InstallSnapshotArgs) {
	var reply InstallSnapshotReply
	ok := rf.peers[p].Call("Raft.InstallSnapshot", args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == args.Term {
		rf.inflight[p] = false
	}
	if !ok || rf.killed() {
		rf.retrySoon(p, args.Term)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	rf.lastAck[p] = time.Now()
	rf.matchIndex[p] = max(rf.matchIndex[p], args.LastIncludedIndex)
	rf.nextIndex[p] = max(rf.nextIndex[p], args.LastIncludedIndex+1)
	rf.advanceCommit()
	if rf.pending[p] || rf.nextIndex[p] <= rf.lastIndex() {
		rf.sendTo(p, false)
	}
}

// InstallSnapshot handles a snapshot sent by a leader to a follower that is
// too far behind to be caught up from the log.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.becomeFollower(args.Term)
	}
	rf.leaderID = args.LeaderID
	rf.lastLeaderMsg = time.Now()
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return // we already have everything it contains
	}
	var rest []LogEntry
	if args.LastIncludedIndex < rf.lastIndex() && rf.term(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Our log agrees with the snapshot; keep what follows it.
		rest = rf.log[args.LastIncludedIndex-rf.firstIndex()+1:]
	}
	rf.log = append([]LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}, rest...)
	rf.snapshot = args.Data
	rf.must(rf.storage.SaveSnapshot(SnapshotMeta{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm},
		args.Data, rf.hardState(), rf.log[1:]))
	rf.persistedHS = rf.hardState()
	rf.syncedIndex = rf.lastIndex()
	rf.commitIndex = args.LastIncludedIndex
	rf.pendingSnapshot = true
	rf.applyCond.Signal()
}
