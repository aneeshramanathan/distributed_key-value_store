// Package linearizability records client histories and checks them against
// a sequential key/value specification with Porcupine.
package linearizability

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Kind of an operation in a history.
type Kind uint8

const (
	Get Kind = iota
	Put
	Delete
)

// Input is what a client asked for.
type Input struct {
	Kind  Kind
	Key   string
	Value string
}

// Output is what the client was told.
type Output struct {
	Value string
	Found bool
}

// state is the per-key state of the sequential specification.
type state struct {
	value  string
	exists bool
}

// Model is the sequential specification of a key/value map, partitioned by
// key: a history is linearizable iff each key's sub-history is, which makes
// checking dramatically cheaper.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(Input).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return state{} },
	Step: func(st, in, out any) (bool, any) {
		s, i, o := st.(state), in.(Input), out.(Output)
		switch i.Kind {
		case Get:
			return o.Found == s.exists && o.Value == s.value, s
		case Put:
			return true, state{value: i.Value, exists: true}
		case Delete:
			return true, state{}
		}
		return false, s
	},
	DescribeOperation: func(in, out any) string {
		i, o := in.(Input), out.(Output)
		switch i.Kind {
		case Get:
			if !o.Found {
				return fmt.Sprintf("get(%q) -> <none>", i.Key)
			}
			return fmt.Sprintf("get(%q) -> %q", i.Key, o.Value)
		case Put:
			return fmt.Sprintf("put(%q, %q)", i.Key, i.Value)
		case Delete:
			return fmt.Sprintf("delete(%q)", i.Key)
		}
		return "?"
	},
	DescribeState: func(st any) string {
		s := st.(state)
		if !s.exists {
			return "<none>"
		}
		return fmt.Sprintf("%q", s.value)
	},
}

// Recorder collects a concurrent history. It is safe for concurrent use.
type Recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

// Now returns the timestamp to use for an invocation or response. It uses
// the monotonic clock.
func (r *Recorder) Now() int64 { return int64(time.Since(r.start)) }

// Record adds a completed operation that was invoked at call and returned
// at ret.
func (r *Recorder) Record(client int, in Input, out Output, call, ret int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
}

// Len returns the number of recorded operations.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// Result of a check.
type Result struct {
	Outcome       porcupine.CheckResult
	Visualization string // path to an HTML visualization, when written
}

// Check verifies the recorded history. If it is not linearizable, an HTML
// visualization of the offending history is written to dir.
func (r *Recorder) Check(timeout time.Duration, dir string) Result {
	r.mu.Lock()
	ops := append([]porcupine.Operation(nil), r.ops...)
	r.mu.Unlock()

	// The verbose checker also builds visualization data, which can be slow
	// on large histories, so only use it once a violation is known.
	res := porcupine.CheckOperationsTimeout(Model, ops, timeout)
	out := Result{Outcome: res}
	if res == porcupine.Illegal && dir != "" {
		_, info := porcupine.CheckOperationsVerbose(Model, ops, timeout)
		path := filepath.Join(dir, "linearizability-violation.html")
		if f, err := os.Create(path); err == nil {
			if porcupine.Visualize(Model, info, f) == nil {
				out.Visualization = path
			}
			f.Close()
		}
	}
	return out
}
