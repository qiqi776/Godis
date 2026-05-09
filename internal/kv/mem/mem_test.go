package mem

import (
	"bytes"
	"encoding/json"
	"testing"

	"mini-kv/internal/kv"
)

func TestApply(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	if result := store.Apply(kv.Command{Type: kv.CommandPut, Key: "a", Value: []byte("1")}); result.Error != "" {
		t.Fatalf("put error: %s", result.Error)
	}

	value, ok, err := store.Get("a")
	if err != nil || !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("get a: value=%q ok=%v err=%v", value, ok, err)
	}

	if result := store.Apply(kv.Command{Type: kv.CommandDelete, Key: "a"}); result.Error != "" || !result.Found {
		t.Fatalf("delete result: %+v", result)
	}
	value, ok, err = store.Get("a")
	if err != nil || ok {
		t.Fatalf("get after delete: value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestRestore(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	store.Apply(kv.Command{Type: kv.CommandPut, Key: "a", Value: []byte("1")})
	store.Apply(kv.Command{Type: kv.CommandPut, Key: "b", Value: []byte("2")})

	data, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	restored := NewMemoryStore()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}

	value, ok, err := restored.Get("a")
	if err != nil || !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("restored a: value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = restored.Get("b")
	if err != nil || !ok || !bytes.Equal(value, []byte("2")) {
		t.Fatalf("restored b: value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestDedup(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	first := kv.Command{
		Type:      kv.CommandPut,
		Key:       "a",
		Value:     []byte("1"),
		ClientID:  "client-1",
		RequestID: 1,
	}
	if result := store.Apply(first); result.Error != "" {
		t.Fatalf("first apply error: %s", result.Error)
	}

	data, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	restored := NewMemoryStore()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}

	duplicate := first
	duplicate.Value = []byte("2")
	if result := restored.Apply(duplicate); result.Error != "" {
		t.Fatalf("duplicate apply error: %s", result.Error)
	}

	value, ok, err := restored.Get("a")
	if err != nil || !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("dedup value = %q, ok=%v err=%v; want 1, true, nil", value, ok, err)
	}
}

func TestLegacy(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(snapshot{
		Version: legacySnapshotVersion,
		Entries: []snapshotEntry{
			{Key: "a", Value: []byte("1")},
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy snapshot: %v", err)
	}

	store := NewMemoryStore()
	if err := store.Restore(data); err != nil {
		t.Fatalf("restore legacy snapshot: %v", err)
	}

	value, ok, err := store.Get("a")
	if err != nil || !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("legacy value = %q, ok=%v err=%v; want 1, true, nil", value, ok, err)
	}
}

func TestBadRestore(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	if err := store.Restore([]byte("bad-json")); err == nil {
		t.Fatal("expected invalid snapshot error")
	}
}
