package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

const (
	LegacySnapshotVersion = 1
	SnapshotVersion       = 2
)

type Session struct {
	LastRequestID uint64
	Results       map[uint64]ApplyResult
}

type SnapshotData struct {
	Version  int               `json:"version"`
	Entries  []SnapshotEntry   `json:"entries"`
	Sessions []SnapshotSession `json:"sessions,omitempty"`
}

type SnapshotEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type SnapshotSession struct {
	ClientID      string           `json:"client_id"`
	LastRequestID uint64           `json:"last_request_id"`
	Results       []SnapshotResult `json:"results,omitempty"`
}

type SnapshotResult struct {
	RequestID uint64      `json:"request_id"`
	Result    ApplyResult `json:"result"`
}

func MarshalSnapshot(entries []SnapshotEntry, sessions map[string]Session) ([]byte, error) {
	out := SnapshotData{
		Version: SnapshotVersion,
		Entries: cloneSnapshotEntries(entries),
	}
	sort.Slice(out.Entries, func(i, j int) bool {
		return out.Entries[i].Key < out.Entries[j].Key
	})

	clients := make([]string, 0, len(sessions))
	for clientID := range sessions {
		clients = append(clients, clientID)
	}
	sort.Strings(clients)

	for _, clientID := range clients {
		session := sessions[clientID]
		requestIDs := make([]uint64, 0, len(session.Results))
		for requestID := range session.Results {
			requestIDs = append(requestIDs, requestID)
		}
		sort.Slice(requestIDs, func(i, j int) bool {
			return requestIDs[i] < requestIDs[j]
		})

		outSession := SnapshotSession{
			ClientID:      clientID,
			LastRequestID: session.LastRequestID,
		}
		for _, requestID := range requestIDs {
			outSession.Results = append(outSession.Results, SnapshotResult{
				RequestID: requestID,
				Result:    CloneApplyResult(session.Results[requestID]),
			})
		}
		out.Sessions = append(out.Sessions, outSession)
	}

	return json.Marshal(out)
}

func ParseSnapshot(data []byte) (SnapshotData, error) {
	if len(data) == 0 {
		return SnapshotData{}, errors.New("empty snapshot")
	}

	var in SnapshotData
	if err := json.Unmarshal(data, &in); err != nil {
		return SnapshotData{}, err
	}
	if in.Version != LegacySnapshotVersion && in.Version != SnapshotVersion {
		return SnapshotData{}, fmt.Errorf("unsupported snapshot version: %d", in.Version)
	}

	seen := make(map[string]struct{}, len(in.Entries))
	for i := range in.Entries {
		entry := &in.Entries[i]
		if _, ok := seen[entry.Key]; ok {
			return SnapshotData{}, fmt.Errorf("duplicate snapshot key: %q", entry.Key)
		}
		seen[entry.Key] = struct{}{}
		entry.Value = CloneBytes(entry.Value)
	}

	seenClients := make(map[string]struct{}, len(in.Sessions))
	for i := range in.Sessions {
		session := &in.Sessions[i]
		if session.ClientID == "" {
			return SnapshotData{}, errors.New("snapshot session client id is empty")
		}
		if _, ok := seenClients[session.ClientID]; ok {
			return SnapshotData{}, fmt.Errorf("duplicate snapshot client session: %q", session.ClientID)
		}
		seenClients[session.ClientID] = struct{}{}

		seenRequests := make(map[uint64]struct{}, len(session.Results))
		for j := range session.Results {
			result := &session.Results[j]
			if result.RequestID == 0 {
				return SnapshotData{}, errors.New("snapshot request id must be positive")
			}
			if _, ok := seenRequests[result.RequestID]; ok {
				return SnapshotData{}, fmt.Errorf("duplicate snapshot request id: client=%q request=%d", session.ClientID, result.RequestID)
			}
			seenRequests[result.RequestID] = struct{}{}
			result.Result = CloneApplyResult(result.Result)
			if result.RequestID > session.LastRequestID {
				session.LastRequestID = result.RequestID
			}
		}
	}

	return in, nil
}

func (s SnapshotData) SessionsMap() map[string]Session {
	sessions := make(map[string]Session, len(s.Sessions))
	for _, snapshotSession := range s.Sessions {
		session := Session{
			LastRequestID: snapshotSession.LastRequestID,
			Results:       make(map[uint64]ApplyResult, len(snapshotSession.Results)),
		}
		for _, result := range snapshotSession.Results {
			session.Results[result.RequestID] = CloneApplyResult(result.Result)
		}
		sessions[snapshotSession.ClientID] = session
	}
	return sessions
}

func CloneApplyResult(result ApplyResult) ApplyResult {
	return ApplyResult{
		Value: CloneBytes(result.Value),
		Found: result.Found,
		Error: result.Error,
	}
}

func CloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func cloneSnapshotEntries(entries []SnapshotEntry) []SnapshotEntry {
	out := make([]SnapshotEntry, len(entries))
	for i := range entries {
		out[i] = SnapshotEntry{
			Key:   entries[i].Key,
			Value: CloneBytes(entries[i].Value),
		}
	}
	return out
}
