package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"mini-kv/internal/kv"
	lsmstore "mini-kv/internal/storage/lsm"
)

const (
	dataNamespace    = byte(1)
	sessionNamespace = byte(2)
	upperNamespace   = byte(3)
)

type Store struct {
	mu       sync.RWMutex
	dir      string
	engine   *lsmstore.Engine
	sessions map[string]kv.Session
	closed   bool
}

var _ kv.Store = (*Store)(nil)
var _ kv.FSM = (*Store)(nil)
var _ kv.Reader = (*Store)(nil)

func Open(dir string, opts ...lsmstore.Option) (*Store, error) {
	engine, err := lsmstore.Open(dir, opts...)
	if err != nil {
		return nil, err
	}
	store := &Store{
		dir:      dir,
		engine:   engine,
		sessions: make(map[string]kv.Session),
	}
	if err := store.loadSessions(); err != nil {
		_ = engine.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.engine == nil {
		return nil
	}
	return s.engine.Close()
}

func (s *Store) Reader() kv.Reader {
	return s
}

func (s *Store) Apply(command kv.Command) kv.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.engine == nil {
		return kv.ApplyResult{Error: lsmstore.ErrClosed.Error()}
	}
	if command.ClientID != "" && command.RequestID > 0 {
		if result, ok := s.lookupResult(command.ClientID, command.RequestID); ok {
			return result
		}
	}

	var batch lsmstore.WriteBatch
	result := s.applyToBatch(command, &batch)
	if command.ClientID != "" && command.RequestID > 0 {
		session := s.nextSession(command.ClientID, command.RequestID, result)
		payload, err := json.Marshal(session)
		if err != nil {
			return kv.ApplyResult{Error: fmt.Sprintf("marshal client session: %v", err)}
		}
		batch.Put(sessionKey(command.ClientID), payload)
	}

	if batch.Len() > 0 {
		if err := s.engine.Write(&batch, lsmstore.WriteOptions{}); err != nil {
			return kv.ApplyResult{Error: err.Error()}
		}
	}
	if command.ClientID != "" && command.RequestID > 0 {
		s.sessions[command.ClientID] = s.nextSession(command.ClientID, command.RequestID, result)
	}
	return kv.CloneApplyResult(result)
}

func (s *Store) Get(key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed || s.engine == nil {
		return nil, false, lsmstore.ErrClosed
	}
	return s.engine.Get(dataKey(key))
}

func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed || s.engine == nil {
		return nil, lsmstore.ErrClosed
	}
	snapshot, err := s.engine.Snapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snapshot.Close() }()

	iter := snapshot.NewIterator(lsmstore.IterOptions{
		LowerBound: namespaceLower(dataNamespace),
		UpperBound: namespaceLower(sessionNamespace),
	})
	defer func() { _ = iter.Close() }()

	var entries []kv.SnapshotEntry
	for ok := iter.First(); ok; ok = iter.Next() {
		entries = append(entries, kv.SnapshotEntry{
			Key:   userKey(iter.Key()),
			Value: kv.CloneBytes(iter.Value()),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return kv.MarshalSnapshot(entries, s.sessions)
}

func (s *Store) Restore(data []byte) error {
	in, err := kv.ParseSnapshot(data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.engine != nil {
		if err := s.engine.Close(); err != nil {
			return err
		}
		s.engine = nil
	}
	if err := os.RemoveAll(s.dir); err != nil {
		return fmt.Errorf("remove lsm dir before restore: %w", err)
	}
	engine, err := lsmstore.Open(s.dir)
	if err != nil {
		return err
	}

	sessions := in.SessionsMap()
	var batch lsmstore.WriteBatch
	for _, entry := range in.Entries {
		batch.Put(dataKey(entry.Key), entry.Value)
	}
	for clientID, session := range sessions {
		payload, err := json.Marshal(session)
		if err != nil {
			_ = engine.Close()
			return fmt.Errorf("marshal client session: %w", err)
		}
		batch.Put(sessionKey(clientID), payload)
	}
	if batch.Len() > 0 {
		if err := engine.Write(&batch, lsmstore.WriteOptions{Sync: true}); err != nil {
			_ = engine.Close()
			return err
		}
	}

	s.engine = engine
	s.sessions = sessions
	s.closed = false
	return nil
}

func (s *Store) loadSessions() error {
	iter := s.engine.NewIterator(lsmstore.IterOptions{
		LowerBound: namespaceLower(sessionNamespace),
		UpperBound: namespaceLower(upperNamespace),
	})
	defer func() { _ = iter.Close() }()

	for ok := iter.First(); ok; ok = iter.Next() {
		var session kv.Session
		if err := json.Unmarshal(iter.Value(), &session); err != nil {
			return fmt.Errorf("unmarshal client session: %w", err)
		}
		if session.Results == nil {
			session.Results = make(map[uint64]kv.ApplyResult)
		}
		s.sessions[userKey(iter.Key())] = cloneSession(session)
	}
	return iter.Error()
}

func (s *Store) applyToBatch(command kv.Command, batch *lsmstore.WriteBatch) kv.ApplyResult {
	switch command.Type {
	case kv.CommandPut:
		batch.Put(dataKey(command.Key), command.Value)
		return kv.ApplyResult{Found: true}
	case kv.CommandDelete:
		_, found, err := s.engine.Get(dataKey(command.Key))
		if err != nil {
			return kv.ApplyResult{Error: err.Error()}
		}
		if found {
			batch.Delete(dataKey(command.Key))
		}
		return kv.ApplyResult{Found: found}
	default:
		return kv.ApplyResult{Error: fmt.Sprintf("unknown command type: %d", command.Type)}
	}
}

func (s *Store) lookupResult(clientID string, requestID uint64) (kv.ApplyResult, bool) {
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

func (s *Store) nextSession(clientID string, requestID uint64, result kv.ApplyResult) kv.Session {
	session := cloneSession(s.sessions[clientID])
	if session.Results == nil {
		session.Results = make(map[uint64]kv.ApplyResult)
	}
	session.Results[requestID] = kv.CloneApplyResult(result)
	if requestID > session.LastRequestID {
		session.LastRequestID = requestID
	}
	return session
}

func cloneSession(session kv.Session) kv.Session {
	cloned := kv.Session{
		LastRequestID: session.LastRequestID,
		Results:       make(map[uint64]kv.ApplyResult, len(session.Results)),
	}
	for requestID, result := range session.Results {
		cloned.Results[requestID] = kv.CloneApplyResult(result)
	}
	return cloned
}

func dataKey(key string) []byte {
	return prefixedKey(dataNamespace, key)
}

func sessionKey(clientID string) []byte {
	return prefixedKey(sessionNamespace, clientID)
}

func prefixedKey(namespace byte, key string) []byte {
	out := make([]byte, 1+len(key))
	out[0] = namespace
	copy(out[1:], key)
	return out
}

func namespaceLower(namespace byte) []byte {
	return []byte{namespace}
}

func userKey(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return string(key[1:])
}
