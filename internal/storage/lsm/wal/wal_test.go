package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mini-kv/internal/storage/lsm/record"
)

func TestStoreAppendReplay(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 1, Options{SegmentSize: 1024})
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	batch := record.Batch{SeqStart: 1, Entries: []record.Entry{record.NewPut([]byte("a"), []byte("1"), 1)}}
	if err := store.Append(batch, true); err != nil {
		t.Fatalf("Append error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	store, err = Open(dir, 1, Options{SegmentSize: 1024})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = store.Close() }()

	var got []record.Batch
	if err := store.Replay(func(batch record.Batch) error {
		got = append(got, batch)
		return nil
	}); err != nil {
		t.Fatalf("Replay error = %v", err)
	}
	if len(got) != 1 || len(got[0].Entries) != 1 || string(got[0].Entries[0].Key) != "a" {
		t.Fatalf("replayed batches = %+v, want one put", got)
	}
}

func TestReplayTruncatesTailPartial(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 1, Options{})
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	if err := store.Append(record.Batch{SeqStart: 1, Entries: []record.Entry{record.NewPut([]byte("a"), []byte("1"), 1)}}, false); err != nil {
		t.Fatalf("Append error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	path := filepath.Join(dir, "WAL-000001")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile error = %v", err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatalf("Write partial error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file close error = %v", err)
	}

	store, err = Open(dir, 1, Options{})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = store.Close() }()
	count := 0
	if err := store.Replay(func(record.Batch) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Replay error = %v", err)
	}
	if count != 1 {
		t.Fatalf("replay count = %d, want 1", count)
	}
}

func TestReplayRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	encoded, err := record.EncodeBatchFrame(record.Batch{
		SeqStart: 1,
		Entries:  []record.Entry{record.NewPut([]byte("a"), []byte("1"), 1)},
	})
	if err != nil {
		t.Fatalf("EncodeBatchFrame error = %v", err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, "WAL-000001"), encoded, 0o644); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}
	store, err := Open(dir, 1, Options{})
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = store.Close() }()
	err = store.Replay(func(record.Batch) error { return nil })
	if !errors.Is(err, record.ErrChecksum) {
		t.Fatalf("Replay error = %v, want checksum", err)
	}
}

func TestStorePurgeRemovesFlushedSegments(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 1, Options{SegmentSize: 72})
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	for i := 1; i <= 5; i++ {
		batch := record.Batch{
			SeqStart: uint64(i),
			Entries:  []record.Entry{record.NewPut([]byte{byte('a' + i)}, []byte("1"), uint64(i))},
		}
		if err := store.Append(batch, true); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	before := countSegments(t, dir)
	if before < 2 {
		t.Fatalf("segment count before purge = %d, want at least 2", before)
	}
	if err := store.Purge(5); err != nil {
		t.Fatalf("Purge error = %v", err)
	}
	after := countSegments(t, dir)
	if after >= before {
		t.Fatalf("segment count after purge = %d, want less than %d", after, before)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
}

func countSegments(t *testing.T, dir string) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "WAL-*"))
	if err != nil {
		t.Fatalf("Glob error = %v", err)
	}
	return len(matches)
}
