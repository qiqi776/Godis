package mem

import (
	"fmt"
	"sort"
	"sync"

	"mini-kv/internal/kv"
)

type MemoryStore struct {
	mu           sync.RWMutex
	data         map[string][]byte
	sessions     map[string]kv.Session
	snapshotRefs uint64
	cowShared    bool
}

var _ kv.Store = (*MemoryStore)(nil)
var _ kv.FSM = (*MemoryStore)(nil)
var _ kv.Reader = (*MemoryStore)(nil)
var _ kv.Snapshotter = (*MemoryStore)(nil)

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

	s.ensureWritableLocked()
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
	handle, err := s.BeginSnapshot()
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	return handle.Marshal()
}

func (s *MemoryStore) BeginSnapshot() (kv.SnapshotHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshotRefs++
	s.cowShared = true
	return &snapshotHandle{
		store:    s,
		data:     s.data,
		sessions: s.sessions,
	}, nil
}

func (s *MemoryStore) ensureWritableLocked() {
	if !s.cowShared {
		return
	}
	s.data = cloneDataMap(s.data)
	s.sessions = cloneSessionsMap(s.sessions)
	s.cowShared = false
}

type snapshotHandle struct {
	store    *MemoryStore
	once     sync.Once
	data     map[string][]byte
	sessions map[string]kv.Session
}

func (h *snapshotHandle) Marshal() ([]byte, error) {
	keys := make([]string, 0, len(h.data))
	for key := range h.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]kv.SnapshotEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, kv.SnapshotEntry{
			Key:   key,
			Value: kv.CloneBytes(h.data[key]),
		})
	}

	return kv.MarshalSnapshot(entries, h.sessions)
}

func (h *snapshotHandle) Close() error {
	h.once.Do(func() {
		h.store.mu.Lock()
		if h.store.snapshotRefs > 0 {
			h.store.snapshotRefs--
		}
		if h.store.snapshotRefs == 0 {
			h.store.cowShared = false
		}
		h.store.mu.Unlock()
	})
	return nil
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
	s.cowShared = false
	s.mu.Unlock()
	return nil
}

func cloneDataMap(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneSessionsMap(in map[string]kv.Session) map[string]kv.Session {
	out := make(map[string]kv.Session, len(in))
	for clientID, session := range in {
		next := kv.Session{
			LastRequestID: session.LastRequestID,
		}
		if session.Results != nil {
			next.Results = make(map[uint64]kv.ApplyResult, len(session.Results))
			for requestID, result := range session.Results {
				next.Results[requestID] = kv.CloneApplyResult(result)
			}
		}
		out[clientID] = next
	}
	return out
}
