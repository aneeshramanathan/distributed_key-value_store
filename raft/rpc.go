package raft

import "github.com/aneeshramanathan/distributed_key-value_store/transport"

// RequestVoteArgs is sent by candidates to gather votes (§5.2).
//
// With PreVote set it is a pre-vote (Ongaro's thesis, §9.6): it asks "would
// you vote for me in Term?" without anyone changing their term or vote.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	PreVote      bool
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate entries; with no
// entries it serves as a heartbeat (§5.3).
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply carries, on failure, enough information for the leader
// to skip back over a whole conflicting term at once:
//
//	ConflictTerm == -1: the follower's log is too short; retry at ConflictIndex.
//	otherwise:          the follower's entry at PrevLogIndex has ConflictTerm,
//	                    whose first entry in its log is at ConflictIndex.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// InstallSnapshotArgs carries a leader's snapshot to a lagging follower (§7).
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// RegisterRPC exposes rf's RPC handlers on srv.
func RegisterRPC(srv *transport.Server, rf *Raft) {
	transport.Register(srv, "Raft.RequestVote", rf.RequestVote)
	transport.Register(srv, "Raft.AppendEntries", rf.AppendEntries)
	transport.Register(srv, "Raft.InstallSnapshot", rf.InstallSnapshot)
}
