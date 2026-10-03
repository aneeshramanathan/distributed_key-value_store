package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aneeshramanathan/distributed_key-value_store/transport"
)

func mkEntries(from, to, term int) []LogEntry {
	var out []LogEntry
	for i := from; i <= to; i++ {
		out = append(out, LogEntry{Index: i, Term: term, Command: []byte(fmt.Sprintf("cmd-%d-%d", term, i))})
	}
	return out
}

func openLoad(t *testing.T, dir string) (*FileStorage, HardState, SnapshotMeta, []LogEntry) {
	t.Helper()
	s, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	hs, meta, entries, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, hs, meta, entries
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, hs, meta, entries := openLoad(t, dir)
	if hs != (HardState{VotedFor: -1}) || meta != (SnapshotMeta{}) || len(entries) != 0 {
		t.Fatalf("fresh storage not empty: %v %v %v", hs, meta, entries)
	}

	want := mkEntries(1, 10, 1)
	want[3].Command = nil // a no-op entry
	must(t, s.SetHardState(HardState{Term: 1, VotedFor: 2}))
	must(t, s.Append(want))
	// Overwrite a suffix, as a follower does on conflict.
	must(t, s.SetHardState(HardState{Term: 2, VotedFor: -1}))
	must(t, s.Append(mkEntries(8, 12, 2)))
	must(t, s.Sync())
	must(t, s.Close())

	want = append(want[:7], mkEntries(8, 12, 2)...)
	_, hs, meta, entries = openLoad(t, dir)
	if hs != (HardState{Term: 2, VotedFor: -1}) {
		t.Fatalf("hard state = %+v", hs)
	}
	if meta != (SnapshotMeta{}) {
		t.Fatalf("meta = %+v", meta)
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("entries mismatch:\n got %v\nwant %v", entries, want)
	}
}

func TestFileStorageTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _, _, _ := openLoad(t, dir)
	must(t, s.Append(mkEntries(1, 5, 1)))
	must(t, s.Append(mkEntries(6, 6, 1)))
	must(t, s.Close())

	// Simulate a crash in the middle of writing the last record.
	path := filepath.Join(dir, walName)
	info, err := os.Stat(path)
	must(t, err)
	must(t, os.Truncate(path, info.Size()-3))

	s, _, _, entries := openLoad(t, dir)
	if !reflect.DeepEqual(entries, mkEntries(1, 5, 1)) {
		t.Fatalf("after torn write got %v", entries)
	}
	// The torn record was cut off, so new appends are readable.
	must(t, s.Append(mkEntries(6, 7, 2)))
	must(t, s.Close())
	_, _, _, entries = openLoad(t, dir)
	if !reflect.DeepEqual(entries, append(mkEntries(1, 5, 1), mkEntries(6, 7, 2)...)) {
		t.Fatalf("after repair got %v", entries)
	}
}

func TestFileStorageCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	s, _, _, _ := openLoad(t, dir)
	must(t, s.Append(mkEntries(1, 3, 1)))
	before := s.Size()
	must(t, s.Append(mkEntries(4, 6, 1)))
	must(t, s.Close())

	// Flip a byte inside the second batch: its checksum no longer matches.
	path := filepath.Join(dir, walName)
	data, err := os.ReadFile(path)
	must(t, err)
	data[before+12] ^= 0xff
	must(t, os.WriteFile(path, data, 0o644))

	_, _, _, entries := openLoad(t, dir)
	if !reflect.DeepEqual(entries, mkEntries(1, 3, 1)) {
		t.Fatalf("got %v", entries)
	}
}

func TestFileStorageSnapshot(t *testing.T) {
	dir := t.TempDir()
	s, _, _, _ := openLoad(t, dir)
	must(t, s.Append(mkEntries(1, 100, 1)))
	sizeBefore := s.Size()

	hs := HardState{Term: 3, VotedFor: 1}
	must(t, s.SaveSnapshot(SnapshotMeta{Index: 90, Term: 1}, []byte("state@90"), hs, mkEntries(91, 100, 1)))
	if s.Size() >= sizeBefore {
		t.Fatalf("compaction did not shrink the log: %d -> %d", sizeBefore, s.Size())
	}
	must(t, s.Append(mkEntries(101, 105, 3)))
	must(t, s.Close())

	s, gotHS, meta, entries := openLoad(t, dir)
	if gotHS != hs || meta != (SnapshotMeta{Index: 90, Term: 1}) {
		t.Fatalf("hs=%+v meta=%+v", gotHS, meta)
	}
	if string(s.Snapshot()) != "state@90" {
		t.Fatalf("snapshot = %q", s.Snapshot())
	}
	if !reflect.DeepEqual(entries, append(mkEntries(91, 100, 1), mkEntries(101, 105, 3)...)) {
		t.Fatalf("entries = %v", entries)
	}

	// A second snapshot replaces the first, which is deleted.
	must(t, s.SaveSnapshot(SnapshotMeta{Index: 105, Term: 3}, []byte("state@105"), hs, nil))
	must(t, s.Close())
	snaps, _ := filepath.Glob(filepath.Join(dir, "snap-*.snap"))
	if len(snaps) != 1 {
		t.Fatalf("expected one snapshot file, found %v", snaps)
	}
	s, _, meta, entries = openLoad(t, dir)
	if meta.Index != 105 || len(entries) != 0 || string(s.Snapshot()) != "state@105" {
		t.Fatalf("meta=%+v entries=%v snapshot=%q", meta, entries, s.Snapshot())
	}
}

// TestFileStorageRaftRestart runs a single-node Raft on disk, restarts it,
// and checks the committed state comes back.
func TestFileStorageRaftRestart(t *testing.T) {
	dir := t.TempDir()
	run := func(write []int) (applied []int) {
		s, err := OpenFileStorage(dir)
		must(t, err)
		defer s.Close()
		ch := make(chan ApplyMsg, 100)
		rf, err := New(make([]transport.Caller, 1), 0, s, ch, DefaultConfig())
		must(t, err)
		defer rf.Kill()
		for {
			if _, isLeader := rf.GetState(); isLeader {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		for _, x := range write {
			rf.Start(cmd(x))
		}
		want := rf.Status().LastLogIndex
		for {
			m := <-ch
			if m.CommandValid && m.Command != nil {
				applied = append(applied, cmdVal(m.Command))
			}
			if m.CommandIndex == want {
				return applied
			}
		}
	}
	if got := run([]int{1, 2, 3}); !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("first run applied %v", got)
	}
	if got := run([]int{4}); !reflect.DeepEqual(got, []int{1, 2, 3, 4}) {
		t.Fatalf("after restart applied %v", got)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
