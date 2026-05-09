package logstore

import (
	"testing"

	"mini-kv/internal/raft"
)

func TestMemoryAppend(t *testing.T) {
	storage := NewMemoryStorage()

	err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.EntryNormal, Data: []byte("b")},
	})
	if err != nil {
		t.Fatalf("append error: %v", err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("last index error: %v", err)
	}
	if lastIndex != 2 {
		t.Fatalf("last index = %d, want 2", lastIndex)
	}

	term, err := storage.Term(2)
	if err != nil {
		t.Fatalf("term error: %v", err)
	}
	if term != 1 {
		t.Fatalf("term = %d, want 1", term)
	}

	entries, err := storage.Entries(1, 3)
	if err != nil {
		t.Fatalf("entries error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
}
