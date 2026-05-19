package wal

import (
	"bytes"
	"fmt"
	"testing"

	"mini-kv/internal/storage/lsm/record"
)

var benchmarkWALBatchSink record.Batch

func BenchmarkStoreAppend(b *testing.B) {
	store, err := Open(b.TempDir(), 1, Options{SegmentSize: 128 << 20})
	if err != nil {
		b.Fatalf("Open error = %v", err)
	}
	defer func() { _ = store.Close() }()

	value := bytes.Repeat([]byte("x"), 128)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := record.Batch{
			SeqStart: uint64(i + 1),
			Entries:  []record.Entry{record.NewPut([]byte("bench-key"), value, uint64(i+1))},
		}
		if err := store.Append(batch, false); err != nil {
			b.Fatalf("Append error = %v", err)
		}
	}
}

func BenchmarkStoreAppendSync(b *testing.B) {
	store, err := Open(b.TempDir(), 1, Options{SegmentSize: 128 << 20})
	if err != nil {
		b.Fatalf("Open error = %v", err)
	}
	defer func() { _ = store.Close() }()

	value := bytes.Repeat([]byte("x"), 128)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := record.Batch{
			SeqStart: uint64(i + 1),
			Entries:  []record.Entry{record.NewPut([]byte("bench-key"), value, uint64(i+1))},
		}
		if err := store.Append(batch, true); err != nil {
			b.Fatalf("Append sync error = %v", err)
		}
	}
}

func BenchmarkStoreReplay(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, 1, Options{SegmentSize: 128 << 20})
	if err != nil {
		b.Fatalf("Open error = %v", err)
	}
	value := bytes.Repeat([]byte("x"), 128)
	for i := 0; i < 8192; i++ {
		batch := record.Batch{
			SeqStart: uint64(i + 1),
			Entries:  []record.Entry{record.NewPut([]byte(fmt.Sprintf("bench-key-%08d", i)), value, uint64(i+1))},
		}
		if err := store.Append(batch, false); err != nil {
			b.Fatalf("Append setup error = %v", err)
		}
	}
	if err := store.Close(); err != nil {
		b.Fatalf("Close setup store: %v", err)
	}

	store, err = Open(dir, 1, Options{SegmentSize: 128 << 20})
	if err != nil {
		b.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = store.Close() }()

	b.ReportAllocs()
	for b.Loop() {
		var last record.Batch
		if err := store.Replay(func(batch record.Batch) error {
			last = batch
			return nil
		}); err != nil {
			b.Fatalf("Replay error = %v", err)
		}
		benchmarkWALBatchSink = last
	}
}
