package raft

import (
	"fmt"
	"sync"
)

// LogEntry is one slot in the replicated log.
type LogEntry struct {
	Index   int
	Term    int
	Command []byte // nil for the no-op a new leader appends
}

// HardState is the part of Raft's state that must survive a crash besides
// the log itself: the latest term this node has seen and whom it voted for
// in that term.
type HardState struct {
	Term     int
	VotedFor int // -1 if no vote was cast in Term
}

// SnapshotMeta identifies the last log entry folded into a snapshot.
type SnapshotMeta struct {
	Index int
	Term  int
}

// Storage is Raft's stable storage.
//
// Raft calls every method except Sync while holding its own lock, so the
// implementation never sees two of those calls at once. Sync is called
// concurrently with them.
type Storage interface {
	// Load returns everything that was persisted. Entries all have
	// Index > meta.Index and are contiguous.
	Load() (hs HardState, meta SnapshotMeta, entries []LogEntry, err error)

	// SetHardState durably records hs before returning.
	SetHardState(hs HardState) error

	// Append writes entries to the log. If entries[0].Index is at or below
	// the last stored index, the stored suffix starting there is replaced.
	// The entries are not guaranteed to be durable until Sync returns.
	Append(entries []LogEntry) error

	// Sync makes all previously appended entries durable.
	Sync() error

	// SaveSnapshot durably replaces the snapshot and compacts the log so
	// that it holds only entries (all of which follow meta.Index).
	SaveSnapshot(meta SnapshotMeta, data []byte, hs HardState, entries []LogEntry) error

	// Snapshot returns the most recently saved snapshot, or nil.
	Snapshot() []byte

	// Size returns the approximate number of bytes of log state, which the
	// service uses to decide when to take a snapshot.
	Size() int

	// Close releases resources. The storage is unusable afterwards.
	Close() error
}

// MemoryStorage keeps state in memory. It is what the simulated test
// harness uses: a "crash" is modelled by cloning the storage, so whatever the
// old node writes after it was killed is not seen by its replacement.
type MemoryStorage struct {
	mu       sync.Mutex
	hs       HardState
	meta     SnapshotMeta
	snapshot []byte
	entries  []LogEntry
	size     int
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{hs: HardState{VotedFor: -1}}
}

// Clone returns an independent copy of the storage.
func (m *MemoryStorage) Clone() *MemoryStorage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &MemoryStorage{
		hs:       m.hs,
		meta:     m.meta,
		snapshot: m.snapshot, // never mutated in place
		entries:  append([]LogEntry(nil), m.entries...),
		size:     m.size,
	}
}

func (m *MemoryStorage) Load() (HardState, SnapshotMeta, []LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, m.meta, append([]LogEntry(nil), m.entries...), nil
}

func (m *MemoryStorage) SetHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	return nil
}

func (m *MemoryStorage) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	first := entries[0].Index
	last := m.meta.Index + len(m.entries)
	if first <= m.meta.Index || first > last+1 {
		return fmt.Errorf("memory storage: append at %d, have (%d, %d]", first, m.meta.Index, last)
	}
	keep := first - m.meta.Index - 1
	for _, e := range m.entries[keep:] {
		m.size -= entrySize(e)
	}
	m.entries = append(m.entries[:keep], entries...)
	for _, e := range entries {
		m.size += entrySize(e)
	}
	return nil
}

func (m *MemoryStorage) Sync() error { return nil }

func (m *MemoryStorage) SaveSnapshot(meta SnapshotMeta, data []byte, hs HardState, entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meta = meta
	m.snapshot = data
	m.hs = hs
	m.entries = append([]LogEntry(nil), entries...)
	m.size = 0
	for _, e := range entries {
		m.size += entrySize(e)
	}
	return nil
}

func (m *MemoryStorage) Snapshot() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot
}

func (m *MemoryStorage) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

func (m *MemoryStorage) Close() error { return nil }

// entrySize approximates an entry's encoded size.
func entrySize(e LogEntry) int { return 16 + len(e.Command) }
