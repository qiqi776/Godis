package mem

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"mini-kv/internal/kv"
)

type clientSession struct {
	LastRequestID uint64
	Results       map[uint64]kv.ApplyResult
}

type MemoryStore struct {
	mu       sync.RWMutex
	data     map[string][]byte
	sessions map[string]clientSession
}

var _ kv.Store = (*MemoryStore)(nil)
var _ kv.FSM = (*MemoryStore)(nil)
var _ kv.Reader = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data:     make(map[string][]byte),
		sessions: make(map[string]clientSession),
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

	return cloneApplyResult(result)
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
	return cloneApplyResult(result), true
}

func (s *MemoryStore) saveResult(clientID string, requestID uint64, result kv.ApplyResult) {
	session := s.sessions[clientID]
	if session.Results == nil {
		session.Results = make(map[uint64]kv.ApplyResult)
	}
	session.Results[requestID] = cloneApplyResult(result)
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
	return cloneBytes(value), true, nil
}

func (s *MemoryStore) applyPut(command kv.Command) kv.ApplyResult {
	s.data[command.Key] = cloneBytes(command.Value)
	return kv.ApplyResult{Found: true}
}

func (s *MemoryStore) applyDelete(command kv.Command) kv.ApplyResult {
	if _, ok := s.data[command.Key]; !ok {
		return kv.ApplyResult{Found: false}
	}

	s.deleteLocked(command.Key)
	return kv.ApplyResult{Found: true}
}

func cloneApplyResult(result kv.ApplyResult) kv.ApplyResult {
	return kv.ApplyResult{
		Value: cloneBytes(result.Value),
		Found: result.Found,
		Error: result.Error,
	}
}

func (s *MemoryStore) deleteLocked(key string) {
	delete(s.data, key)
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

const (
	legacySnapshotVersion = 1
	snapshotVersion       = 2
)

type snapshot struct {
	Version  int               `json:"version"`
	Entries  []snapshotEntry   `json:"entries"`
	Sessions []snapshotSession `json:"sessions,omitempty"`
}

type snapshotEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type snapshotSession struct {
	ClientID      string           `json:"client_id"`
	LastRequestID uint64           `json:"last_request_id"`
	Results       []snapshotResult `json:"results,omitempty"`
}

type snapshotResult struct {
	RequestID uint64         `json:"request_id"`
	Result    kv.ApplyResult `json:"result"`
}

func (s *MemoryStore) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := snapshot{Version: snapshotVersion}
	for _, key := range keys {
		out.Entries = append(out.Entries, snapshotEntry{
			Key:   key,
			Value: cloneBytes(s.data[key]),
		})
	}

	clients := make([]string, 0, len(s.sessions))
	for clientID := range s.sessions {
		clients = append(clients, clientID)
	}
	sort.Strings(clients)

	for _, clientID := range clients {
		session := s.sessions[clientID]
		requestIDs := make([]uint64, 0, len(session.Results))
		for requestID := range session.Results {
			requestIDs = append(requestIDs, requestID)
		}
		sort.Slice(requestIDs, func(i, j int) bool {
			return requestIDs[i] < requestIDs[j]
		})

		outSession := snapshotSession{
			ClientID:      clientID,
			LastRequestID: session.LastRequestID,
		}
		for _, requestID := range requestIDs {
			outSession.Results = append(outSession.Results, snapshotResult{
				RequestID: requestID,
				Result:    cloneApplyResult(session.Results[requestID]),
			})
		}
		out.Sessions = append(out.Sessions, outSession)
	}

	return json.Marshal(out)
}

func (s *MemoryStore) Restore(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty snapshot")
	}

	var in snapshot
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	if in.Version != legacySnapshotVersion && in.Version != snapshotVersion {
		return fmt.Errorf("unsupported snapshot version: %d", in.Version)
	}

	nextData := make(map[string][]byte, len(in.Entries))
	nextSessions := make(map[string]clientSession, len(in.Sessions))
	seen := make(map[string]struct{}, len(in.Entries))

	for _, entry := range in.Entries {
		if _, ok := seen[entry.Key]; ok {
			return fmt.Errorf("duplicate snapshot key: %q", entry.Key)
		}
		seen[entry.Key] = struct{}{}
		nextData[entry.Key] = cloneBytes(entry.Value)
	}

	seenClients := make(map[string]struct{}, len(in.Sessions))
	for _, session := range in.Sessions {
		if session.ClientID == "" {
			return errors.New("snapshot session client id is empty")
		}
		if _, ok := seenClients[session.ClientID]; ok {
			return fmt.Errorf("duplicate snapshot client session: %q", session.ClientID)
		}
		seenClients[session.ClientID] = struct{}{}

		nextSession := clientSession{
			LastRequestID: session.LastRequestID,
			Results:       make(map[uint64]kv.ApplyResult, len(session.Results)),
		}
		seenRequests := make(map[uint64]struct{}, len(session.Results))
		for _, result := range session.Results {
			if result.RequestID == 0 {
				return errors.New("snapshot request id must be positive")
			}
			if _, ok := seenRequests[result.RequestID]; ok {
				return fmt.Errorf("duplicate snapshot request id: client=%q request=%d", session.ClientID, result.RequestID)
			}
			seenRequests[result.RequestID] = struct{}{}
			nextSession.Results[result.RequestID] = cloneApplyResult(result.Result)
			if result.RequestID > nextSession.LastRequestID {
				nextSession.LastRequestID = result.RequestID
			}
		}
		nextSessions[session.ClientID] = nextSession
	}

	s.mu.Lock()
	s.data = nextData
	s.sessions = nextSessions
	s.mu.Unlock()
	return nil
}
