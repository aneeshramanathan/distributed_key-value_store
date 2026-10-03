package kv

import (
	"math/rand/v2"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

// Clerk is a client of the replicated store. It finds the leader, retries
// through failures, and tags writes so retries are applied at most once. A
// Clerk issues one operation at a time; use one Clerk per concurrent
// client.
type Clerk struct {
	servers  []transport.Caller
	leader   int
	clientID int64
	seq      int64
}

// NewClerk returns a Clerk talking to servers.
func NewClerk(servers []transport.Caller) *Clerk {
	return &Clerk{
		servers:  servers,
		clientID: rand.Int64N(1<<62) + 1, // nonzero: 0 opts out of dedup
	}
}

// Get returns the value of key and whether it exists.
func (ck *Clerk) Get(key string) (string, bool) {
	r := ck.do(Op{Kind: OpGet, Key: key})
	return r.Value, r.Err == OK
}

// Put sets key to value.
func (ck *Clerk) Put(key, value string) {
	ck.do(Op{Kind: OpPut, Key: key, Value: value})
}

// Delete removes key.
func (ck *Clerk) Delete(key string) {
	ck.do(Op{Kind: OpDelete, Key: key})
}

func (ck *Clerk) do(op Op) Reply {
	op.ClientID = ck.clientID
	if op.Kind != OpGet {
		ck.seq++
		op.Seq = ck.seq
	}
	n := len(ck.servers)
	s := ck.leader
	for tries := 1; ; tries++ {
		var reply Reply
		ok := ck.servers[s].Call("KV.Do", &Args{Op: op}, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			ck.leader = s
			return reply
		}
		if ok && reply.Err == ErrWrongLeader && reply.LeaderHint >= 0 && reply.LeaderHint < n && reply.LeaderHint != s {
			s = reply.LeaderHint
		} else {
			s = (s + 1) % n
		}
		if tries%n == 0 {
			// Every server failed: probably mid-election. Back off briefly.
			time.Sleep(20 * time.Millisecond)
		}
	}
}
