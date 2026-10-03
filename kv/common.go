// Package kv is a linearizable key/value store replicated with Raft.
//
// Every operation — reads included — is appended to the Raft log and
// executed by each replica's state machine in log order once committed. A
// reply is only sent after the operation has been applied by the leader that
// accepted it, so every completed operation takes effect at a single point
// between its invocation and its response: the definition of
// linearizability.
//
// Clients retry operations when a leader fails, so the same write can be
// committed more than once. Each client tags its writes with a unique client
// ID and an increasing sequence number, and the state machine remembers the
// highest sequence number it has applied for every client, so a retried
// write is executed at most once.
package kv

import (
	"encoding/binary"
	"errors"
)

// OpKind identifies an operation.
type OpKind uint8

const (
	OpGet OpKind = iota + 1
	OpPut
	OpDelete
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	}
	return "?"
}

// Op is one client operation. ClientID 0 means the client opted out of
// duplicate detection.
type Op struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

// Err is the outcome of an operation.
type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"       // Get of a missing key
	ErrWrongLeader Err = "ErrWrongLeader" // retry at another server
	ErrTimeout     Err = "ErrTimeout"     // not committed in time; retry
	ErrShutdown    Err = "ErrShutdown"
)

// Args and Reply are the KV.Do RPC messages.
type Args struct {
	Op Op
}

type Reply struct {
	Err        Err
	Value      string
	LeaderHint int // the server's best guess at the leader, or -1
}

// encodeOp serializes an op compactly for the Raft log.
func encodeOp(op Op) []byte {
	buf := make([]byte, 0, 1+3*binary.MaxVarintLen64+len(op.Key)+len(op.Value)+2*binary.MaxVarintLen64)
	buf = append(buf, byte(op.Kind))
	buf = binary.AppendVarint(buf, op.ClientID)
	buf = binary.AppendVarint(buf, op.Seq)
	buf = binary.AppendUvarint(buf, uint64(len(op.Key)))
	buf = append(buf, op.Key...)
	buf = binary.AppendUvarint(buf, uint64(len(op.Value)))
	buf = append(buf, op.Value...)
	return buf
}

var errBadOp = errors.New("kv: malformed op")

func decodeOp(b []byte) (Op, error) {
	var op Op
	if len(b) < 1 {
		return op, errBadOp
	}
	op.Kind = OpKind(b[0])
	b = b[1:]
	var n int
	if op.ClientID, n = binary.Varint(b); n <= 0 {
		return op, errBadOp
	}
	b = b[n:]
	if op.Seq, n = binary.Varint(b); n <= 0 {
		return op, errBadOp
	}
	b = b[n:]
	readStr := func() (string, error) {
		l, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b)-n) < l {
			return "", errBadOp
		}
		s := string(b[n : n+int(l)])
		b = b[n+int(l):]
		return s, nil
	}
	var err error
	if op.Key, err = readStr(); err != nil {
		return op, err
	}
	if op.Value, err = readStr(); err != nil {
		return op, err
	}
	return op, nil
}
