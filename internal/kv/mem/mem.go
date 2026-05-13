package mem

import (
	"fmt"
	"sort"
	"sync"

	"mini-kv/internal/kv"
)

type MemoryStore struct {
	mu       sync.RWMutex
	data     map[string][]byte
	sessions map[string]kv.Session
}

var _ kv.Store = (*MemoryStore)(nil)
var _ kv.FSM = (*MemoryStore)(nil)
var _ kv.Reader = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data:     make(map[string][]byte),
		sessions: make(map[string]kv.Session),
	}
}

func (s *MemoryStore) Close() {}

func (s *MemoryStore) Reader() kv.Reader {
	return s
}

func (s *MemoryStore) Apply(command kv.Command) kv.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if command.ClientID != "" && command.RequestID > 0 {
		if result, ok := s.lookupResult(command.ClientID, command.RequestID); ok {
			return result
		}
	}

	result := s.applyLocked(command)

	if command.ClientID != "" && command.RequestID > 0 {
		s.saveResult(command.ClientID, command.RequestID, result)
	}

	return kv.CloneApplyResult(result)
}

func (s *MemoryStore) applyLocked(command kv.Command) kv.ApplyResult {
	switch command.Type {
	case kv.CommandPut:
		return s.applyPut(command)
	case kv.CommandDelete:
		return s.applyDelete(command)
	default:
		return kv.ApplyResult{Error: fmt.Sprintf("unknown command type: %d", command.Type)}
	}
}

func (s *MemoryStore) lookupResult(clientID string, requestID uint64) (kv.ApplyResult, bool) {
	session, ok := s.sessions[clientID]
	if !ok {
		return kv.ApplyResult{}, false
	}
	result, ok := session.Results[requestID]
	if !ok {
		return kv.ApplyResult{}, false
	}
	return kv.CloneApplyResult(result), true
}

func (s *MemoryStore) saveResult(clientID string, requestID uint64, result kv.ApplyResult) {
	session := s.sessions[clientID]
	if session.Results == nil {
		session.Results = make(map[uint64]kv.ApplyResult)
	}
	session.Results[requestID] = kv.CloneApplyResult(result)
	if requestID > session.LastRequestID {
		session.LastRequestID = requestID
	}
	s.sessions[clientID] = session
}

func (s *MemoryStore) Get(key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return nil, false, nil
	}
	return kv.CloneBytes(value), true, nil
}

func (s *MemoryStore) applyPut(command kv.Command) kv.ApplyResult {
	s.data[command.Key] = kv.CloneBytes(command.Value)
	return kv.ApplyResult{Found: true}
}

func (s *MemoryStore) applyDelete(command kv.Command) kv.ApplyResult {
	if _, ok := s.data[command.Key]; !ok {
		return kv.ApplyResult{Found: false}
	}

	delete(s.data, command.Key)
	return kv.ApplyResult{Found: true}
}

func (s *MemoryStore) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]kv.SnapshotEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, kv.SnapshotEntry{
			Key:   key,
			Value: kv.CloneBytes(s.data[key]),
		})
	}

	return kv.MarshalSnapshot(entries, s.sessions)
}

func (s *MemoryStore) Restore(data []byte) error {
	in, err := kv.ParseSnapshot(data)
	if err != nil {
		return err
	}

	nextData := make(map[string][]byte, len(in.Entries))
	for _, entry := range in.Entries {
		nextData[entry.Key] = kv.CloneBytes(entry.Value)
	}

	s.mu.Lock()
	s.data = nextData
	s.sessions = in.SessionsMap()
	s.mu.Unlock()
	return nil
}
