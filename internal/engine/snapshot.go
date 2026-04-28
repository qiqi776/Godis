package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const fsmSnapshotVersion = 1

type SnapshotCommandProvider interface {
	SnapshotCommands() [][][]byte
}

type fsmSnapshot struct {
	Version   int             `json:"version"`
	DBCount   int             `json:"db_count"`
	Databases []dbSnapshot    `json:"databases"`
}

type dbSnapshot struct {
	Index   int             `json:"index"`
	Entries []snapshotEntry `json:"entries"`
}

type snapshotEntry struct {
	Key      string `json:"key"`
	Kind     Kind   `json:"kind"`
	Value    []byte `json:"value"`
	ExpireAt int64  `json:"expire_at"`
}

func (e *Engine) Snapshot() ([]byte, error) {
	snapshot := fsmSnapshot{
		Version: fsmSnapshotVersion,
		DBCount: len(e.dbs),
	}

	for index, db := range e.dbs {
		dbSnapshot, err := db.snapshot(index)
		if err != nil {
			return nil, err
		}
		if len(dbSnapshot.Entries) > 0 {
			snapshot.Databases = append(snapshot.Databases, dbSnapshot)
		}
	}

	return json.Marshal(snapshot)
}

func (db *DB) snapshot(index int) (dbSnapshot, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now()
	keys := make([]string, 0, len(db.data))
	for key := range db.data {
		if expireAt, ok := db.expireAt[key]; ok && !expireAt.After(now) {
			db.deleteKey(key)
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := dbSnapshot{Index: index}
	for _, key := range keys {
		entity := db.data[key]
		if entity.Kind != KindString {
			return dbSnapshot{}, fmt.Errorf("snapshot only supports string values in raft kv mode: key=%q kind=%d", key, entity.Kind)
		}

		value, ok := entity.Value.([]byte)
		if !ok {
			return dbSnapshot{}, fmt.Errorf("invalid string value type for key %q", key)
		}

		var expireAt int64
		if deadline, ok := db.expireAt[key]; ok {
			expireAt = deadline.UnixMilli()
		}

		result.Entries = append(result.Entries, snapshotEntry{
			Key:      key,
			Kind:     entity.Kind,
			Value:    copyBytes(value),
			ExpireAt: expireAt,
		})
	}

	return result, nil
}

func (e *Engine) Restore(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty snapshot")
	}

	var snapshot fsmSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	if snapshot.Version != fsmSnapshotVersion {
		return fmt.Errorf("unsupported snapshot version: %d", snapshot.Version)
	}
	if snapshot.DBCount <= 0 {
		return errors.New("invalid snapshot db count")
	}
	if snapshot.DBCount > len(e.dbs) {
		return fmt.Errorf("snapshot db count %d exceeds engine db count %d", snapshot.DBCount, len(e.dbs))
	}

	newData := make([]map[string]*Entity, len(e.dbs))
	newExpireAt := make([]map[string]time.Time, len(e.dbs))
	for index := range e.dbs {
		newData[index] = make(map[string]*Entity)
		newExpireAt[index] = make(map[string]time.Time)
	}

	now := time.Now()
	seenDB := make(map[int]struct{})
	for _, dbSnapshot := range snapshot.Databases {
		if dbSnapshot.Index < 0 || dbSnapshot.Index >= len(e.dbs) {
			return fmt.Errorf("snapshot db index out of range: %d", dbSnapshot.Index)
		}
		if _, ok := seenDB[dbSnapshot.Index]; ok {
			return fmt.Errorf("duplicate snapshot db index: %d", dbSnapshot.Index)
		}
		seenDB[dbSnapshot.Index] = struct{}{}

		seenKeys := make(map[string]struct{})
		for _, entry := range dbSnapshot.Entries {
			if entry.Kind != KindString {
				return fmt.Errorf("restore only supports string values in raft kv mode: key=%q kind=%d", entry.Key, entry.Kind)
			}
			if _, ok := seenKeys[entry.Key]; ok {
				return fmt.Errorf("duplicate snapshot key in db %d: %q", dbSnapshot.Index, entry.Key)
			}
			seenKeys[entry.Key] = struct{}{}

			if entry.ExpireAt > 0 {
				deadline := time.UnixMilli(entry.ExpireAt)
				if !deadline.After(now) {
					continue
				}
				newExpireAt[dbSnapshot.Index][entry.Key] = deadline
			}

			newData[dbSnapshot.Index][entry.Key] = &Entity{
				Kind:  KindString,
				Value: copyBytes(entry.Value),
			}
		}
	}

	for index, db := range e.dbs {
		db.mu.Lock()
		db.data = newData[index]
		db.expireAt = newExpireAt[index]
		db.revisions = make(map[string]uint64)
		db.nextRev = 0

		keys := make([]string, 0, len(db.data))
		for key := range db.data {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			db.touchKey(key)
		}

		db.mu.Unlock()
	}

	return nil
}

func (e *Engine) SnapshotCommands() [][][]byte {
	return e.AOFRewriteCommands()
}
