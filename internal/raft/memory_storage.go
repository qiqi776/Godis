package raft

import "sync"

type MemoryStorage struct {
	mu        sync.RWMutex
	hardState HardState
	entries   []LogEntry
	offset    uint64
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: []LogEntry{
			{
				Index: 0,
				Term:  0,
				Type:  EntryNormal,
			},
		},
	}
}

func (s *MemoryStorage) SaveHardState(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.hardState = state
	return nil
}

func (s *MemoryStorage) LoadHardState() (HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.hardState, nil
}

func (s *MemoryStorage) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	firstIndex := entries[0].Index
	if firstIndex < s.offset {
		return ErrCompacted
	}

	if firstIndex == s.offset+uint64(len(s.entries)) {
		s.entries = append(s.entries, cloneEntries(entries)...)
		return nil
	}

	if firstIndex > s.offset+uint64(len(s.entries)) {
		return ErrStorageConflict
	}

	cut := firstIndex - s.offset
	s.entries = append(s.entries[:cut], cloneEntries(entries)...)
	return nil
}

func (s *MemoryStorage) Entries(start, end uint64) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < s.offset {
		return nil, ErrCompacted
	}
	if end < start {
		return nil, ErrEntryNotFound
	}

	first := s.offset
	last := s.offset + uint64(len(s.entries)) - 1
	if start > last+1 || end > last+1 {
		return nil, ErrEntryNotFound
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
		return 0, ErrCompacted
	}

	last := s.offset + uint64(len(s.entries)) - 1
	if index > last {
		return 0, ErrEntryNotFound
	}

	return s.entries[index-s.offset].Term, nil
}

func (s *MemoryStorage) TruncateSuffix(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < s.offset {
		return ErrCompacted
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
		return ErrEntryNotFound
	}

	term := s.entries[index-s.offset].Term
	s.entries = append([]LogEntry{
		{Index: index, Term: term, Type: EntryNormal},
	}, cloneEntries(s.entries[index-s.offset+1:])...)
	s.offset = index
	return nil
}

func cloneEntries(entries []LogEntry) []LogEntry {
	if len(entries) == 0 {
		return nil
	}

	cloned := make([]LogEntry, len(entries))
	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Data = append([]byte(nil), entry.Data...)
	}
	return cloned
}
