package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

// FileStorage persists Raft state in a directory:
//
//	wal.log                 write-ahead log of records (see below)
//	snap-<index>.snap       the current snapshot
//
// The WAL is a sequence of records, each framed as
//
//	[4-byte length][4-byte CRC-32C][1-byte type][payload]
//
// where length covers type+payload and the CRC covers type+payload. Record
// types are:
//
//	header     snapshot index and term this WAL builds on (always first)
//	hardstate  current term and vote
//	entries    a batch of log entries; replaces any stored suffix it overlaps
//
// On startup the WAL is replayed. A record that is cut short or fails its
// checksum can only be the result of a crash during a write, so the file is
// truncated just before it ("torn tail" recovery). Records that were never
// fsynced were never acknowledged to anyone, so dropping them is safe.
//
// Compaction writes the new snapshot file, then writes a brand-new WAL to a
// temporary file and renames it over wal.log. The rename is the commit
// point: a crash before it leaves the old WAL and old snapshot in place, a
// crash after it leaves the new ones.
type FileStorage struct {
	dir string

	// rw guards the identity of f. Append, SetHardState and Sync hold it
	// for reading; SaveSnapshot and Close hold it for writing when they
	// swap or close the file.
	rw   sync.RWMutex
	f    *os.File
	size atomic.Int64

	snapMu   sync.Mutex
	snapshot []byte
	meta     SnapshotMeta

	// NoSync disables fsync. Only for benchmarking the cost of fsync;
	// it makes the storage unsafe across machine crashes.
	NoSync bool
}

const (
	walName      = "wal.log"
	recHeader    = 1
	recHardState = 2
	recEntries   = 3
	frameSize    = 8
	maxRecordLen = 1 << 30
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// OpenFileStorage opens (or creates) the storage in dir.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

// Load replays the WAL, repairs a torn tail if there is one, and opens the
// WAL for appending.
func (s *FileStorage) Load() (HardState, SnapshotMeta, []LogEntry, error) {
	hs := HardState{VotedFor: -1}
	var meta SnapshotMeta
	var entries []LogEntry

	path := filepath.Join(s.dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return hs, meta, nil, err
	}

	r := bufio.NewReader(f)
	var good int64 // offset just past the last intact record
	for {
		typ, payload, n, err := readRecord(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// Torn or corrupt tail: cut it off.
				if terr := f.Truncate(good); terr != nil {
					f.Close()
					return hs, meta, nil, terr
				}
			}
			break
		}
		good += n
		switch typ {
		case recHeader:
			idx, term, err := decodePair(payload)
			if err != nil {
				f.Close()
				return hs, meta, nil, err
			}
			meta = SnapshotMeta{Index: int(idx), Term: int(term)}
		case recHardState:
			term, vote, err := decodePair(payload)
			if err != nil {
				f.Close()
				return hs, meta, nil, err
			}
			hs = HardState{Term: int(term), VotedFor: int(vote)}
		case recEntries:
			batch, err := decodeEntries(payload)
			if err != nil {
				f.Close()
				return hs, meta, nil, err
			}
			if len(batch) == 0 {
				continue
			}
			// Overwrite any suffix the batch overlaps.
			first := batch[0].Index
			cut := sort.Search(len(entries), func(i int) bool { return entries[i].Index >= first })
			entries = append(entries[:cut], batch...)
		default:
			f.Close()
			return hs, meta, nil, fmt.Errorf("wal: unknown record type %d", typ)
		}
	}

	// Entries at or below the snapshot are already compacted.
	cut := sort.Search(len(entries), func(i int) bool { return entries[i].Index > meta.Index })
	entries = entries[cut:]
	if len(entries) > 0 && entries[0].Index != meta.Index+1 {
		f.Close()
		return hs, meta, nil, fmt.Errorf("wal: gap after snapshot %d: first entry %d", meta.Index, entries[0].Index)
	}

	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return hs, meta, nil, err
	}
	if good == 0 {
		// Fresh WAL: write the header so the file is self-describing.
		if err := writeRecord(f, recHeader, encodePair(0, 0)); err != nil {
			f.Close()
			return hs, meta, nil, err
		}
		good, _ = f.Seek(0, io.SeekCurrent)
	}
	s.f = f
	s.size.Store(good)

	var data []byte
	if meta.Index > 0 {
		data, err = os.ReadFile(s.snapPath(meta.Index))
		if err != nil {
			return hs, meta, nil, fmt.Errorf("read snapshot %d: %w", meta.Index, err)
		}
	}
	s.snapMu.Lock()
	s.snapshot, s.meta = data, meta
	s.snapMu.Unlock()
	s.removeStaleSnapshots(meta.Index)
	return hs, meta, entries, nil
}

func (s *FileStorage) SetHardState(hs HardState) error {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if err := s.write(recHardState, encodePair(uint64(hs.Term), uint64(int64(hs.VotedFor)))); err != nil {
		return err
	}
	return s.sync()
}

func (s *FileStorage) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.write(recEntries, encodeEntries(entries))
}

func (s *FileStorage) Sync() error {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.sync()
}

func (s *FileStorage) SaveSnapshot(meta SnapshotMeta, data []byte, hs HardState, entries []LogEntry) error {
	// 1. Write the snapshot file durably.
	if err := writeFileSync(s.snapPath(meta.Index), data, !s.NoSync); err != nil {
		return err
	}

	// 2. Build the replacement WAL in a temporary file.
	tmpPath := filepath.Join(s.dir, walName+".tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(tmp)
	err = writeRecord(w, recHeader, encodePair(uint64(meta.Index), uint64(meta.Term)))
	if err == nil {
		err = writeRecord(w, recHardState, encodePair(uint64(hs.Term), uint64(int64(hs.VotedFor))))
	}
	if err == nil && len(entries) > 0 {
		err = writeRecord(w, recEntries, encodeEntries(entries))
	}
	if err == nil {
		err = w.Flush()
	}
	if err == nil && !s.NoSync {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpPath)
		return err
	}

	// 3. Swap it in. Windows cannot rename over an open file, so close the
	// current WAL first.
	s.rw.Lock()
	defer s.rw.Unlock()
	if s.f != nil {
		s.f.Close()
		s.f = nil
	}
	path := filepath.Join(s.dir, walName)
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	syncDir(s.dir)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	s.f = f
	s.size.Store(end)

	s.snapMu.Lock()
	s.snapshot, s.meta = data, meta
	s.snapMu.Unlock()
	s.removeStaleSnapshots(meta.Index)
	return nil
}

func (s *FileStorage) Snapshot() []byte {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	return s.snapshot
}

func (s *FileStorage) Size() int { return int(s.size.Load()) }

func (s *FileStorage) Close() error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// write appends one record. The caller holds rw for reading; Raft's lock
// guarantees writes themselves are not concurrent.
func (s *FileStorage) write(typ byte, payload []byte) error {
	if s.f == nil {
		return errors.New("wal: storage closed")
	}
	if err := writeRecord(s.f, typ, payload); err != nil {
		return err
	}
	s.size.Add(int64(frameSize + 1 + len(payload)))
	return nil
}

func (s *FileStorage) sync() error {
	if s.f == nil {
		return errors.New("wal: storage closed")
	}
	if s.NoSync {
		return nil
	}
	return s.f.Sync()
}

func (s *FileStorage) snapPath(index int) string {
	return filepath.Join(s.dir, fmt.Sprintf("snap-%020d.snap", index))
}

// removeStaleSnapshots deletes every snapshot file except the current one,
// including leftovers from compactions that crashed before committing.
func (s *FileStorage) removeStaleSnapshots(current int) {
	matches, _ := filepath.Glob(filepath.Join(s.dir, "snap-*.snap"))
	keep := s.snapPath(current)
	for _, m := range matches {
		if m != keep {
			os.Remove(m)
		}
	}
	// Any temporary file is left over from an interrupted write.
	tmps, _ := filepath.Glob(filepath.Join(s.dir, "*.tmp"))
	for _, t := range tmps {
		os.Remove(t)
	}
}

// ---- record framing and encoding ----

func writeRecord(w io.Writer, typ byte, payload []byte) error {
	buf := make([]byte, frameSize+1+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(1+len(payload)))
	buf[8] = typ
	copy(buf[9:], payload)
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(buf[8:], crcTable))
	_, err := w.Write(buf)
	return err
}

var errCorrupt = errors.New("wal: corrupt record")

// readRecord returns the record type, payload and number of bytes consumed.
// It returns io.EOF only at a clean record boundary.
func readRecord(r io.Reader) (byte, []byte, int64, error) {
	var hdr [frameSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, 0, io.EOF
		}
		return 0, nil, 0, errCorrupt
	}
	n := binary.LittleEndian.Uint32(hdr[0:4])
	if n == 0 || n > maxRecordLen {
		return 0, nil, 0, errCorrupt
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, 0, errCorrupt
	}
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(hdr[4:8]) {
		return 0, nil, 0, errCorrupt
	}
	return body[0], body[1:], int64(frameSize + n), nil
}

func encodePair(a, b uint64) []byte {
	buf := make([]byte, 0, 2*binary.MaxVarintLen64)
	buf = binary.AppendUvarint(buf, a)
	return binary.AppendUvarint(buf, b)
}

func decodePair(p []byte) (uint64, uint64, error) {
	a, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, 0, errCorrupt
	}
	b, m := binary.Uvarint(p[n:])
	if m <= 0 {
		return 0, 0, errCorrupt
	}
	return a, b, nil
}

func encodeEntries(entries []LogEntry) []byte {
	size := binary.MaxVarintLen64
	for _, e := range entries {
		size += 3*binary.MaxVarintLen64 + len(e.Command) + 1
	}
	buf := make([]byte, 0, size)
	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	for _, e := range entries {
		buf = binary.AppendUvarint(buf, uint64(e.Index))
		buf = binary.AppendUvarint(buf, uint64(e.Term))
		if e.Command == nil {
			buf = append(buf, 0)
			continue
		}
		buf = append(buf, 1)
		buf = binary.AppendUvarint(buf, uint64(len(e.Command)))
		buf = append(buf, e.Command...)
	}
	return buf
}

func decodeEntries(p []byte) ([]LogEntry, error) {
	count, n := binary.Uvarint(p)
	if n <= 0 || count > uint64(len(p)) {
		return nil, errCorrupt
	}
	p = p[n:]
	out := make([]LogEntry, 0, count)
	next := func() (uint64, error) {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return 0, errCorrupt
		}
		p = p[n:]
		return v, nil
	}
	for i := uint64(0); i < count; i++ {
		idx, err := next()
		if err != nil {
			return nil, err
		}
		term, err := next()
		if err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errCorrupt
		}
		hasCmd := p[0] == 1
		p = p[1:]
		e := LogEntry{Index: int(idx), Term: int(term)}
		if hasCmd {
			l, err := next()
			if err != nil || l > uint64(len(p)) {
				return nil, errCorrupt
			}
			e.Command = append([]byte{}, p[:l]...)
			p = p[l:]
		}
		out = append(out, e)
	}
	return out, nil
}

func writeFileSync(path string, data []byte, doSync bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil && doSync {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}

// syncDir fsyncs a directory so that a rename inside it is durable. Windows
// does not support (or need) this.
func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
