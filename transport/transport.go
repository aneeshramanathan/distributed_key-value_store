// Package transport defines the small RPC abstraction shared by every layer
// of the system.
//
// Raft and the key/value service never talk to the network directly. They
// send requests through a Caller and receive them through a Server. Two
// implementations of the wire exist:
//
//   - simnet.ClientEnd, an in-memory network used by the tests, which can drop,
//     delay, reorder and partition messages.
//   - HTTPCaller / Server.ServeHTTP, a real network transport (gob over HTTP)
//     used by the kvnode binary and the benchmarks.
//
// Because both implementations serialize arguments and replies with gob,
// the code under test never shares memory between nodes, exactly as it would
// in production.
package transport

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

// Caller sends an RPC to a single remote endpoint.
//
// Call returns true if a reply was received and decoded into reply. It
// returns false if the request or the reply was lost, the remote server is
// down, or the call timed out. A false return says nothing about whether the
// remote side executed the request, so callers must be prepared for both.
type Caller interface {
	Call(method string, args any, reply any) bool
}

// HandlerFunc processes an encoded request and returns an encoded reply.
type HandlerFunc func(req []byte) ([]byte, error)

// Server dispatches encoded requests to registered handlers by method name.
// It is safe for concurrent use.
type Server struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewServer returns an empty Server.
func NewServer() *Server {
	return &Server{handlers: make(map[string]HandlerFunc)}
}

// Handle registers a raw handler for method.
func (s *Server) Handle(method string, h HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

// Register adds a typed handler for method. Arguments and replies are
// gob-encoded on the wire.
func Register[A any, R any](s *Server, method string, fn func(args *A, reply *R)) {
	s.Handle(method, func(req []byte) ([]byte, error) {
		args := new(A)
		if err := Decode(req, args); err != nil {
			return nil, fmt.Errorf("decode %s args: %w", method, err)
		}
		reply := new(R)
		fn(args, reply)
		return Encode(reply)
	})
}

// Dispatch runs the handler registered for method.
func (s *Server) Dispatch(method string, req []byte) ([]byte, error) {
	s.mu.RLock()
	h, ok := s.handlers[method]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown method %q", method)
	}
	return h(req)
}

// Encode gob-encodes v.
func Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode gob-decodes data into v.
func Decode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
