package lsm

import (
	"bytes"
	"encoding/json"
	"testing"

	"mini-kv/internal/kv"
)

func TestStoreApplyGetReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	if result := store.Apply(kv.Command{Type: kv.CommandPut, Key: "a", Value: []byte("1")}); result.Error != "" {
		t.Fatalf("put error: %s", result.Error)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = store.Close() }()

	value, ok, err := store.Get("a")
	if err != nil || !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("get a: value=%q ok=%v err=%v", value, ok, err)
	}
	if result := store.Apply(kv.Command{Type: kv.CommandDelete, Key: "a"}); result.Error != "" || !result.Found {
		t.Fatalf("delete result: %+v", result)
	}
	value, ok, err = store.Get("a")
	if err != nil || ok || value != nil {
		t.Fatalf("get after delete: value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestStoreSnapshotRestoreAndDedup(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = store.Close() }()

	command := kv.Command{
		Type:      kv.CommandPut,
		Key:       "a",
		Value:     []byte("1"),
		ClientID:  "client-1",
		RequestID: 1,
	}
	if result := store.Apply(command); result.Error != "" {
		t.Fatalf("apply error: %s", result.Error)
	}
	data, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error = %v", err)
	}

	restored, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open restored error = %v", err)
	}
	defer func() { _ = restored.Close() }()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore error = %v", err)
	}

	duplicate := command
	duplicate.Value = []byte("2")
	if result := restored.Apply(duplicate); result.Error != "" {
		t.Fatalf("duplicate apply error: %s", result.Error)
	}
	value, ok, err := restored.Get("a")
	if err != nil || !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("dedup value = %q, ok=%v err=%v; want 1, true, nil", value, ok, err)
	}
}

func TestStoreRestoreLegacySnapshot(t *testing.T) {
	data, err := json.Marshal(kv.SnapshotData{
		Version: kv.LegacySnapshotVersion,
		Entries: []kv.SnapshotEntry{
			{Key: "a", Value: []byte("1")},
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy snapshot: %v", err)
	}

	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Restore(data); err != nil {
		t.Fatalf("Restore error = %v", err)
	}

	value, ok, err := store.Get("a")
	if err != nil || !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("legacy value = %q, ok=%v err=%v; want 1, true, nil", value, ok, err)
	}
}

func TestStoreImplementsKVStore(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = store.Close() }()

	var _ kv.Store = store
	var _ kv.Reader = store.Reader()
}
