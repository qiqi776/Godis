package logstore

import (
	"bytes"
	"sync"

	"mini-kv/internal/raft"
)

type MemoryStorage struct {
	mu        sync.RWMutex
	hardState raft.HardState
	entries   []raft.LogEntry
	offset    uint64
	snapshot  raft.Snapshot
}

var _ raft.Storage = (*MemoryStorage)(nil)

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: []raft.LogEntry{
			{
				Index: 0,
				Term:  0,
				Type:  raft.EntryNormal,
			},
		},
	}
}

func (s *MemoryStorage) SaveHardState(state raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.hardState = state
	return nil
}

func (s *MemoryStorage) LoadHardState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.hardState, nil
}

func (s *MemoryStorage) Append(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	firstIndex := entries[0].Index
	if firstIndex < s.offset {
		return raft.ErrCompacted
	}

	if firstIndex == s.offset+uint64(len(s.entries)) {
		s.entries = append(s.entries, cloneEntries(entries)...)
		return nil
	}

	if firstIndex > s.offset+uint64(len(s.entries)) {
		return raft.ErrStorageConflict
	}

	cut := firstIndex - s.offset
	s.entries = append(s.entries[:cut], cloneEntries(entries)...)
	return nil
}

func (s *MemoryStorage) Entries(start, end uint64) ([]raft.LogEntry, error) {
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

	result := s.entries[start-first : end-first]
	return cloneEntries(result), nil
}

func (s *MemoryStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.offset + uint64(len(s.entries)) - 1, nil
}

func (s *MemoryStorage) Term(index uint64) (uint64, error) {
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

func (s *MemoryStorage) TruncateSuffix(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < s.offset {
		return raft.ErrCompacted
	}

	last := s.offset + uint64(len(s.entries)) - 1
	if index >= last {
		return nil
	}

	s.entries = s.entries[:index-s.offset+1]
	return nil
}

func (s *MemoryStorage) TruncatePrefix(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index <= s.offset {
		return nil
	}

	last := s.offset + uint64(len(s.entries)) - 1
	if index > last {
		return raft.ErrEntryNotFound
	}

	term := s.entries[index-s.offset].Term
	s.entries = append([]raft.LogEntry{
		{Index: index, Term: term, Type: raft.EntryNormal},
	}, cloneEntries(s.entries[index-s.offset+1:])...)
	s.offset = index
	return nil
}

func (s *MemoryStorage) SaveSnapshot(snapshot raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snapshot.Index == 0 {
		return raft.ErrInvalidConfig
	}
	if snapshot.Index < s.offset {
		return raft.ErrCompacted
	}
	last := s.offset + uint64(len(s.entries)) - 1
	if snapshot.Index > last {
		return raft.ErrEntryNotFound
	}
	if s.entries[snapshot.Index-s.offset].Term != snapshot.Term {
		return raft.ErrStorageConflict
	}

	s.snapshot = cloneSnapshot(snapshot)
	return s.truncatePrefixLocked(snapshot.Index)
}

func (s *MemoryStorage) LoadSnapshot() (raft.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return cloneSnapshot(s.snapshot), nil
}

func (s *MemoryStorage) ApplySnapshot(snapshot raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snapshot.Index == 0 {
		return raft.ErrInvalidConfig
	}
	if snapshot.Index < s.snapshot.Index {
		return raft.ErrCompacted
	}
	if snapshot.Index == s.snapshot.Index && s.snapshot.Index != 0 {
		if s.snapshot.Term != snapshot.Term || !bytes.Equal(s.snapshot.Data, snapshot.Data) {
			return raft.ErrStorageConflict
		}
		return nil
	}

	s.snapshot = cloneSnapshot(snapshot)
	s.offset = snapshot.Index
	s.entries = []raft.LogEntry{{Index: snapshot.Index, Term: snapshot.Term, Type: raft.EntryNormal}}
	return nil
}

func (s *MemoryStorage) truncatePrefixLocked(index uint64) error {
	if index <= s.offset {
		return nil
	}
	last := s.offset + uint64(len(s.entries)) - 1
	if index > last {
		return raft.ErrEntryNotFound
	}
	term := s.entries[index-s.offset].Term
	s.entries = append([]raft.LogEntry{{Index: index, Term: term, Type: raft.EntryNormal}}, cloneEntries(s.entries[index-s.offset+1:])...)
	s.offset = index
	return nil
}
