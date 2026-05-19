package lsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"mini-kv/internal/kv"
	lsmstore "mini-kv/internal/storage/lsm"
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

func TestStoreMatrixWorkloadCorrectness(t *testing.T) {
	tests := []struct {
		name          string
		mode          string
		operations    int
		keyspace      int
		readPercent   int
		writePercent  int
		deletePercent int
		preload       bool
	}{
		{
			name:       "set-small-1",
			mode:       "set",
			operations: 128,
			keyspace:   32,
			preload:    false,
		},
		{
			name:       "set-small-16",
			mode:       "set",
			operations: 512,
			keyspace:   64,
			preload:    false,
		},
		{
			name:       "get-small-16",
			mode:       "get",
			operations: 512,
			keyspace:   64,
			preload:    true,
		},
		{
			name:          "mixed-medium-32",
			mode:          "mixed",
			operations:    768,
			keyspace:      96,
			readPercent:   70,
			writePercent:  25,
			deletePercent: 5,
			preload:       true,
		},
		{
			name:       "delete-medium-16",
			mode:       "delete",
			operations: 512,
			keyspace:   64,
			preload:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := Open(t.TempDir())
			if err != nil {
				t.Fatalf("Open error = %v", err)
			}
			defer func() { _ = store.Close() }()

			model := make(map[string][]byte, tt.keyspace)
			if tt.preload {
				for i := 0; i < tt.keyspace; i++ {
					key := matrixKey(i)
					value := []byte(fmt.Sprintf("preload-%03d", i))
					applyPut(t, store, key, value)
					model[key] = append([]byte(nil), value...)
				}
			}

			for i := 0; i < tt.operations; i++ {
				key := matrixKey(i % tt.keyspace)
				switch pickMatrixOperation(tt.mode, i, tt.readPercent, tt.writePercent, tt.deletePercent) {
				case "set":
					value := []byte(fmt.Sprintf("value-%s-%04d", tt.name, i))
					applyPut(t, store, key, value)
					model[key] = append([]byte(nil), value...)
				case "get":
					assertStoreValue(t, store, key, model[key])
				case "delete":
					result := store.Apply(kv.Command{Type: kv.CommandDelete, Key: key})
					if result.Error != "" {
						t.Fatalf("delete %q error: %s", key, result.Error)
					}
					if _, exists := model[key]; result.Found != exists {
						t.Fatalf("delete %q found=%v, want %v", key, result.Found, exists)
					}
					delete(model, key)
				default:
					t.Fatalf("unsupported mode %q", tt.mode)
				}
			}

			assertModel(t, store, tt.keyspace, model)
		})
	}
}

func TestStoreStabilityAcrossFlushSnapshotAndReopen(t *testing.T) {
	opts := []lsmstore.Option{
		lsmstore.WithMemTableSize(256),
		lsmstore.WithL0CompactionTrigger(2),
		lsmstore.WithMaxImmutableTables(8),
	}
	dir := t.TempDir()
	store, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = store.Close() }()

	const keyspace = 41
	model := make(map[string][]byte, keyspace)
	for i := 0; i < 240; i++ {
		key := matrixKey(i % keyspace)
		value := []byte(fmt.Sprintf("stable-%04d", i))
		applyPut(t, store, key, value)
		model[key] = append([]byte(nil), value...)

		if i%7 == 0 {
			deleteKey := matrixKey((i + 19) % keyspace)
			result := store.Apply(kv.Command{Type: kv.CommandDelete, Key: deleteKey})
			if result.Error != "" {
				t.Fatalf("delete %q at %d error: %s", deleteKey, i, result.Error)
			}
			delete(model, deleteKey)
		}
		if i%10 == 0 {
			if err := store.engine.Flush(); err != nil {
				t.Fatalf("Flush at %d error = %v", i, err)
			}
		}
		if i%30 == 0 {
			assertSnapshotRestore(t, store, keyspace, model)
		}
		if i%45 == 0 {
			if err := store.Close(); err != nil {
				t.Fatalf("Close at %d error = %v", i, err)
			}
			store, err = Open(dir, opts...)
			if err != nil {
				t.Fatalf("reopen at %d error = %v", i, err)
			}
			assertModel(t, store, keyspace, model)
		}
	}

	if err := store.engine.Flush(); err != nil {
		t.Fatalf("final Flush error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("final Close error = %v", err)
	}
	store, err = Open(dir, opts...)
	if err != nil {
		t.Fatalf("final reopen error = %v", err)
	}
	assertModel(t, store, keyspace, model)
	assertSnapshotRestore(t, store, keyspace, model)
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

func pickMatrixOperation(mode string, index int, readPercent int, writePercent int, deletePercent int) string {
	switch mode {
	case "set", "get", "delete":
		return mode
	case "mixed":
		roll := index % 100
		switch {
		case roll < readPercent:
			return "get"
		case roll < readPercent+writePercent:
			return "set"
		case roll < readPercent+writePercent+deletePercent:
			return "delete"
		default:
			return "get"
		}
	default:
		return ""
	}
}

func matrixKey(index int) string {
	return fmt.Sprintf("bench:%08d", index)
}

func applyPut(t *testing.T, store *Store, key string, value []byte) {
	t.Helper()
	result := store.Apply(kv.Command{Type: kv.CommandPut, Key: key, Value: value})
	if result.Error != "" {
		t.Fatalf("put %q error: %s", key, result.Error)
	}
}

func assertStoreValue(t *testing.T, store *Store, key string, want []byte) {
	t.Helper()
	got, ok, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	if want == nil {
		if ok || got != nil {
			t.Fatalf("Get(%q) = (%q, %v), want nil false", key, got, ok)
		}
		return
	}
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = (%q, %v), want (%q, true)", key, got, ok, want)
	}
	got[0] ^= 0xff
	gotAgain, ok, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) after caller mutation error = %v", key, err)
	}
	if !ok || !bytes.Equal(gotAgain, want) {
		t.Fatalf("Get(%q) after caller mutation = (%q, %v), want (%q, true)", key, gotAgain, ok, want)
	}
}

func assertModel(t *testing.T, store *Store, keyspace int, model map[string][]byte) {
	t.Helper()
	for i := 0; i < keyspace; i++ {
		key := matrixKey(i)
		assertStoreValue(t, store, key, model[key])
	}
}

func assertSnapshotRestore(t *testing.T, store *Store, keyspace int, model map[string][]byte) {
	t.Helper()
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
	assertModel(t, restored, keyspace, model)
}
