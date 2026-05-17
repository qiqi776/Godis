package logstore

import (
	"path/filepath"
	"testing"

	"mini-kv/internal/raft"
)

var benchmarkEntrySink []raft.LogEntry

func BenchmarkMemoryStorageAppend(b *testing.B) {
	storage := NewMemoryStorage()
	entry := raft.LogEntry{
		Term: 1,
		Type: raft.EntryNormal,
		Data: make([]byte, 256),
	}

	var index uint64
	b.ReportAllocs()
	for b.Loop() {
		index++
		entry.Index = index
		if err := storage.Append([]raft.LogEntry{entry}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStorageAppend(b *testing.B) {
	storage, err := OpenFileStorage(filepath.Join(b.TempDir(), "raft.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	entry := raft.LogEntry{
		Term: 1,
		Type: raft.EntryNormal,
		Data: make([]byte, 256),
	}

	var index uint64
	b.ReportAllocs()
	for b.Loop() {
		index++
		entry.Index = index
		if err := storage.Append([]raft.LogEntry{entry}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStorageSaveHardState(b *testing.B) {
	storage, err := OpenFileStorage(filepath.Join(b.TempDir(), "raft.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	state := raft.HardState{
		CurrentTerm: 1,
		VotedFor:    "node1",
		Commit:      0,
	}

	b.ReportAllocs()
	for b.Loop() {
		state.CurrentTerm++
		if err := storage.SaveHardState(state); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStorageEntries(b *testing.B) {
	storage, err := OpenFileStorage(filepath.Join(b.TempDir(), "raft.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	entries := make([]raft.LogEntry, 64)
	for i := range entries {
		entries[i] = raft.LogEntry{
			Index: uint64(i + 1),
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  make([]byte, 256),
		}
	}
	if err := storage.Append(entries); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		out, err := storage.Entries(1, 65)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkEntrySink = out
	}
}
