package logstore

import (
	"testing"

	"mini-kv/internal/raft"
)

func TestMemoryApplySnapshotSameIndexIdempotent(t *testing.T) {
	storage := NewMemoryStorage()
	snapshot := raft.Snapshot{
		Index: 5,
		Term:  3,
		Data:  []byte("remote-snapshot"),
	}
	if err := storage.ApplySnapshot(snapshot); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	if err := storage.ApplySnapshot(snapshot); err != nil {
		t.Fatalf("reapply snapshot: %v", err)
	}
}

func TestMemoryApplySnapshotSameIndexConflict(t *testing.T) {
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(raft.Snapshot{
		Index: 5,
		Term:  3,
		Data:  []byte("remote-snapshot"),
	}); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	if err := storage.ApplySnapshot(raft.Snapshot{
		Index: 5,
		Term:  4,
		Data:  []byte("different"),
	}); err != raft.ErrStorageConflict {
		t.Fatalf("conflicting snapshot error = %v, want %v", err, raft.ErrStorageConflict)
	}
}
