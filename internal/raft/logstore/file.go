package logstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"mini-kv/internal/raft"
)

const (
	fileStorageRecordHardState      = "hard_state"
	fileStorageRecordAppend         = "append"
	fileStorageRecordTruncateSuffix = "truncate_suffix"
	fileStorageRecordTruncatePrefix = "truncate_prefix"
	fileStorageRecordSaveSnapshot   = "save_snapshot"
	fileStorageRecordApplySnapshot  = "apply_snapshot"
	fileStorageRecordHeaderSize     = 16
)

var (
	fileStorageCRCTable = crc32.MakeTable(crc32.Castagnoli)
	errPartialWALRecord = errors.New("partial wal record")
)

type FileStorage struct {
	mu        sync.RWMutex
	path      string
	file      *os.File
	hardState raft.HardState
	entries   []raft.LogEntry
	offset    uint64
	snapshot  raft.Snapshot
}

type fileStorageRecord struct {
	Type      string          `json:"type"`
	HardState *raft.HardState `json:"hard_state,omitempty"`
	Entries   []raft.LogEntry `json:"entries,omitempty"`
	Index     uint64          `json:"index,omitempty"`
	Snapshot  *raft.Snapshot  `json:"snapshot,omitempty"`
}

var _ raft.Storage = (*FileStorage)(nil)

func OpenFileStorage(path string) (*FileStorage, error) {
	if path == "" {
		return nil, raft.ErrInvalidConfig
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	storage := &FileStorage{
		path:    path,
		file:    file,
		entries: initialLogEntries(),
	}

	if err := storage.replayLocked(); err != nil {
		_ = file.Close()
		return nil, err
	}

	return storage, nil
}

func NewFileStorage(path string) (*FileStorage, error) {
	return OpenFileStorage(path)
}

func (s *FileStorage) SaveHardState(state raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIndex := s.offset + uint64(len(s.entries)) - 1
	if state.Commit > lastIndex {
		return raft.ErrEntryNotFound
	}

	next := state
	if err := s.writeRecordLocked(fileStorageRecord{
		Type:      fileStorageRecordHardState,
		HardState: &next,
	}); err != nil {
		return err
	}

	s.hardState = next
	return nil
}

func (s *FileStorage) LoadHardState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.hardState, nil
}

func (s *FileStorage) Append(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nextEntries, err := appendEntriesState(s.offset, s.entries, entries)
	if err != nil {
		return err
	}

	if err := s.writeRecordLocked(fileStorageRecord{
		Type:    fileStorageRecordAppend,
		Entries: cloneEntries(entries),
	}); err != nil {
		return err
	}

	s.entries = nextEntries
	return nil
}

func (s *FileStorage) Entries(start, end uint64) ([]raft.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < s.offset {
		return nil, raft.ErrCompacted
	}
	if end < start {
		return nil, raft.ErrEntryNotFound
	}

	first := s.offset
	last := s.offset + uint64(len(s.entries)) - 1
	if start > last+1 || end > last+1 {
		return nil, raft.ErrEntryNotFound
	}

	return cloneEntries(s.entries[start-first : end-first]), nil
}

func (s *FileStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.offset + uint64(len(s.entries)) - 1, nil
}

func (s *FileStorage) Term(index uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.offset {
		return 0, raft.ErrCompacted
	}

	last := s.offset + uint64(len(s.entries)) - 1
	if index > last {
		return 0, raft.ErrEntryNotFound
	}

	return s.entries[index-s.offset].Term, nil
}

func (s *FileStorage) TruncateSuffix(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextEntries, changed, err := truncateSuffixState(s.offset, s.entries, index)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	if err := s.writeRecordLocked(fileStorageRecord{
		Type:  fileStorageRecordTruncateSuffix,
		Index: index,
	}); err != nil {
		return err
	}

	s.entries = nextEntries
	return nil
}

func (s *FileStorage) TruncatePrefix(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextOffset, nextEntries, changed, err := truncatePrefixState(s.offset, s.entries, index)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	if err := s.writeRecordLocked(fileStorageRecord{
		Type:  fileStorageRecordTruncatePrefix,
		Index: index,
	}); err != nil {
		return err
	}

	s.offset = nextOffset
	s.entries = nextEntries
	return nil
}

func (s *FileStorage) SaveSnapshot(snapshot raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextOffset, nextEntries, err := saveSnapshotState(s.offset, s.entries, snapshot)
	if err != nil {
		return err
	}

	nextSnapshot := cloneSnapshot(snapshot)
	if err := s.writeRecordLocked(fileStorageRecord{
		Type:     fileStorageRecordSaveSnapshot,
		Snapshot: &nextSnapshot,
	}); err != nil {
		return err
	}

	s.snapshot = nextSnapshot
	s.offset = nextOffset
	s.entries = nextEntries
	return nil
}

func (s *FileStorage) LoadSnapshot() (raft.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return cloneSnapshot(s.snapshot), nil
}

func (s *FileStorage) ApplySnapshot(snapshot raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snapshot.Index == 0 {
		return raft.ErrInvalidConfig
	}
	if snapshot.Index < s.snapshot.Index {
		return raft.ErrCompacted
	}

	nextSnapshot := cloneSnapshot(snapshot)
	if err := s.writeRecordLocked(fileStorageRecord{
		Type:     fileStorageRecordApplySnapshot,
		Snapshot: &nextSnapshot,
	}); err != nil {
		return err
	}

	s.snapshot = nextSnapshot
	s.offset = snapshot.Index
	s.entries = []raft.LogEntry{
		{Index: snapshot.Index, Term: snapshot.Term, Type: raft.EntryNormal},
	}
	return nil
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	err := s.file.Close()
	s.file = nil
	return err
}

func (s *FileStorage) replayLocked() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	stat, err := s.file.Stat()
	if err != nil {
		return err
	}

	fileSize := stat.Size()
	recordOffset := int64(0)
	header := make([]byte, fileStorageRecordHeaderSize)

	for recordOffset < fileSize {
		if _, err := io.ReadFull(s.file, header); err != nil {
			if isPartialWALRead(err) {
				if err := s.truncateReplayTailLocked(recordOffset); err != nil {
					return err
				}
				break
			}
			return fmt.Errorf("replay raft wal at offset %d: %w", recordOffset, err)
		}

		payloadLen := int64(binary.LittleEndian.Uint32(header[:4]))
		expectedCRC := binary.LittleEndian.Uint32(header[4:8])
		storedOffset := int64(binary.LittleEndian.Uint64(header[8:]))
		if storedOffset != recordOffset {
			return fmt.Errorf("replay raft wal at offset %d: offset mismatch", recordOffset)
		}
		payloadEnd := recordOffset + fileStorageRecordHeaderSize + payloadLen
		if payloadLen == 0 {
			return fmt.Errorf("replay raft wal at offset %d: empty record", recordOffset)
		}
		if payloadEnd > fileSize {
			if err := s.truncateReplayTailLocked(recordOffset); err != nil {
				return err
			}
			break
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(s.file, payload); err != nil {
			if isPartialWALRead(err) {
				if err := s.truncateReplayTailLocked(recordOffset); err != nil {
					return err
				}
				break
			}
			return fmt.Errorf("replay raft wal at offset %d: %w", recordOffset, err)
		}
		if crc32.Checksum(payload, fileStorageCRCTable) != expectedCRC {
			return fmt.Errorf("replay raft wal at offset %d: crc mismatch", recordOffset)
		}

		var record fileStorageRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("replay raft wal at offset %d: %w", recordOffset, err)
		}
		if err := s.applyRecordLocked(record); err != nil {
			return err
		}

		recordOffset = payloadEnd
	}

	_, err = s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *FileStorage) applyRecordLocked(record fileStorageRecord) error {
	switch record.Type {
	case fileStorageRecordHardState:
		if record.HardState == nil {
			return raft.ErrInvalidConfig
		}
		s.hardState = *record.HardState
		return nil

	case fileStorageRecordAppend:
		nextEntries, err := appendEntriesState(s.offset, s.entries, record.Entries)
		if err != nil {
			return err
		}
		s.entries = nextEntries
		return nil

	case fileStorageRecordTruncateSuffix:
		nextEntries, _, err := truncateSuffixState(s.offset, s.entries, record.Index)
		if err != nil {
			return err
		}
		s.entries = nextEntries
		return nil

	case fileStorageRecordTruncatePrefix:
		nextOffset, nextEntries, _, err := truncatePrefixState(s.offset, s.entries, record.Index)
		if err != nil {
			return err
		}
		s.offset = nextOffset
		s.entries = nextEntries
		return nil

	case fileStorageRecordSaveSnapshot:
		if record.Snapshot == nil {
			return raft.ErrInvalidConfig
		}
		nextOffset, nextEntries, err := saveSnapshotState(s.offset, s.entries, *record.Snapshot)
		if err != nil {
			return err
		}
		s.snapshot = cloneSnapshot(*record.Snapshot)
		s.offset = nextOffset
		s.entries = nextEntries
		return nil

	case fileStorageRecordApplySnapshot:
		if record.Snapshot == nil {
			return raft.ErrInvalidConfig
		}
		if record.Snapshot.Index == 0 {
			return raft.ErrInvalidConfig
		}
		s.snapshot = cloneSnapshot(*record.Snapshot)
		s.offset = record.Snapshot.Index
		s.entries = []raft.LogEntry{
			{Index: record.Snapshot.Index, Term: record.Snapshot.Term, Type: raft.EntryNormal},
		}
		return nil

	default:
		return raft.ErrInvalidConfig
	}
}

func (s *FileStorage) writeRecordLocked(record fileStorageRecord) error {
	if s.file == nil {
		return raft.ErrNodeStopped
	}

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if uint64(len(data)) > uint64(^uint32(0)) {
		return fmt.Errorf("raft wal record too large: %d", len(data))
	}

	recordOffset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	header := make([]byte, fileStorageRecordHeaderSize)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.Checksum(data, fileStorageCRCTable))
	binary.LittleEndian.PutUint64(header[8:], uint64(recordOffset))

	if _, err := s.file.Write(header); err != nil {
		return err
	}
	if _, err := s.file.Write(data); err != nil {
		return err
	}

	return s.file.Sync()
}

func (s *FileStorage) truncateReplayTailLocked(offset int64) error {
	if err := s.file.Truncate(offset); err != nil {
		return err
	}
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return s.file.Sync()
}

func isPartialWALRead(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func initialLogEntries() []raft.LogEntry {
	return []raft.LogEntry{
		{
			Index: 0,
			Term:  0,
			Type:  raft.EntryNormal,
		},
	}
}

func appendEntriesState(offset uint64, current []raft.LogEntry, entries []raft.LogEntry) ([]raft.LogEntry, error) {
	if len(entries) == 0 {
		return cloneEntries(current), nil
	}

	firstIndex := entries[0].Index
	if firstIndex < offset {
		return nil, raft.ErrCompacted
	}

	nextIndex := offset + uint64(len(current))
	if firstIndex == nextIndex {
		return append(cloneEntries(current), cloneEntries(entries)...), nil
	}

	if firstIndex > nextIndex {
		return nil, raft.ErrStorageConflict
	}

	cut := int(firstIndex - offset)
	next := cloneEntries(current[:cut])
	next = append(next, cloneEntries(entries)...)
	return next, nil
}

func truncateSuffixState(offset uint64, current []raft.LogEntry, index uint64) ([]raft.LogEntry, bool, error) {
	if index < offset {
		return nil, false, raft.ErrCompacted
	}

	last := offset + uint64(len(current)) - 1
	if index >= last {
		return cloneEntries(current), false, nil
	}

	cut := int(index - offset + 1)
	return cloneEntries(current[:cut]), true, nil
}

func truncatePrefixState(offset uint64, current []raft.LogEntry, index uint64) (uint64, []raft.LogEntry, bool, error) {
	if index <= offset {
		return offset, cloneEntries(current), false, nil
	}

	last := offset + uint64(len(current)) - 1
	if index > last {
		return 0, nil, false, raft.ErrEntryNotFound
	}

	cut := int(index - offset)
	term := current[cut].Term

	next := []raft.LogEntry{
		{
			Index: index,
			Term:  term,
			Type:  raft.EntryNormal,
		},
	}
	next = append(next, cloneEntries(current[cut+1:])...)
	return index, next, true, nil
}

func cloneEntries(entries []raft.LogEntry) []raft.LogEntry {
	if len(entries) == 0 {
		return nil
	}

	cloned := make([]raft.LogEntry, len(entries))
	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Data = append([]byte(nil), entry.Data...)
	}
	return cloned
}

func saveSnapshotState(offset uint64, current []raft.LogEntry, snapshot raft.Snapshot) (uint64, []raft.LogEntry, error) {
	if snapshot.Index == 0 {
		return 0, nil, raft.ErrInvalidConfig
	}
	if snapshot.Index < offset {
		return 0, nil, raft.ErrCompacted
	}

	last := offset + uint64(len(current)) - 1
	if snapshot.Index > last {
		return 0, nil, raft.ErrEntryNotFound
	}

	cut := int(snapshot.Index - offset)
	if current[cut].Term != snapshot.Term {
		return 0, nil, raft.ErrStorageConflict
	}

	next := []raft.LogEntry{
		{Index: snapshot.Index, Term: snapshot.Term, Type: raft.EntryNormal},
	}
	next = append(next, cloneEntries(current[cut+1:])...)
	return snapshot.Index, next, nil
}

func cloneSnapshot(snapshot raft.Snapshot) raft.Snapshot {
	return raft.Snapshot{
		Index: snapshot.Index,
		Term:  snapshot.Term,
		Data:  append([]byte(nil), snapshot.Data...),
	}
}
