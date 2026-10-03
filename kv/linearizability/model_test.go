package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestCheckerAcceptsLinearizableHistory(t *testing.T) {
	r := NewRecorder()
	// put(x,1) [0,10]; get(x)->1 [5,15] overlaps the put, so it may see it.
	r.Record(0, Input{Kind: Put, Key: "x", Value: "1"}, Output{}, 0, 10)
	r.Record(1, Input{Kind: Get, Key: "x"}, Output{Value: "1", Found: true}, 5, 15)
	// get(y) on a never-written key sees nothing.
	r.Record(1, Input{Kind: Get, Key: "y"}, Output{}, 16, 20)
	if res := r.Check(time.Second, ""); res.Outcome != porcupine.Ok {
		t.Fatalf("expected Ok, got %v", res.Outcome)
	}
}

// The checker must catch the bugs it exists to catch.
func TestCheckerRejectsStaleRead(t *testing.T) {
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "x", Value: "1"}, Output{}, 0, 10)
	r.Record(0, Input{Kind: Put, Key: "x", Value: "2"}, Output{}, 20, 30)
	// Starts after put(x,2) completed, yet returns the older value.
	r.Record(1, Input{Kind: Get, Key: "x"}, Output{Value: "1", Found: true}, 40, 50)
	res := r.Check(time.Second, t.TempDir())
	if res.Outcome != porcupine.Illegal {
		t.Fatalf("expected Illegal, got %v", res.Outcome)
	}
	if res.Visualization == "" {
		t.Fatal("expected a visualization to be written")
	}
}

func TestCheckerRejectsLostDelete(t *testing.T) {
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "k", Value: "v"}, Output{}, 0, 1)
	r.Record(0, Input{Kind: Delete, Key: "k"}, Output{}, 2, 3)
	r.Record(1, Input{Kind: Get, Key: "k"}, Output{Value: "v", Found: true}, 4, 5)
	if res := r.Check(time.Second, ""); res.Outcome != porcupine.Illegal {
		t.Fatalf("expected Illegal, got %v", res.Outcome)
	}
}

func TestCheckerRejectsDuplicatedWrite(t *testing.T) {
	// A retried put(x,1) re-applied after put(x,2) shows up as x reverting.
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "x", Value: "1"}, Output{}, 0, 10)
	r.Record(1, Input{Kind: Put, Key: "x", Value: "2"}, Output{}, 11, 20)
	r.Record(1, Input{Kind: Get, Key: "x"}, Output{Value: "2", Found: true}, 21, 22)
	r.Record(1, Input{Kind: Get, Key: "x"}, Output{Value: "1", Found: true}, 23, 24)
	if res := r.Check(time.Second, ""); res.Outcome != porcupine.Illegal {
		t.Fatalf("expected Illegal, got %v", res.Outcome)
	}
}
