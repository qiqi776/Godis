package sstable

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"mini-kv/internal/storage/lsm/record"
)

var (
	benchmarkSSTableEntrySink record.Entry
	benchmarkSSTableBoolSink  bool
	benchmarkSSTableCountSink int
)

func BenchmarkManagerBuild(b *testing.B) {
	dir := b.TempDir()
	manager := NewManager(dir, Options{BlockSize: 32 << 10})
	entries := benchmarkSSTableEntries(1024, 128)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Build(context.Background(), uint64(i+1), 0, entries); err != nil {
			b.Fatalf("Build error = %v", err)
		}
	}
}

func BenchmarkReaderGet(b *testing.B) {
	reader, keys := newBenchmarkReader(b, 8192, 128)
	defer func() { _ = reader.Close() }()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry, ok, err := reader.Get(keys[i&(len(keys)-1)], ^uint64(0))
		if err != nil {
			b.Fatalf("Get error = %v", err)
		}
		benchmarkSSTableEntrySink = entry
		benchmarkSSTableBoolSink = ok
	}
}

func BenchmarkReaderIterator(b *testing.B) {
	reader, _ := newBenchmarkReader(b, 8192, 128)
	defer func() { _ = reader.Close() }()

	b.ReportAllocs()
	for b.Loop() {
		iter, err := reader.NewIterator(^uint64(0), record.KeyBounds{
			Lower: []byte("bench-key-00001000"),
			Upper: []byte("bench-key-00005000"),
		})
		if err != nil {
			b.Fatalf("NewIterator error = %v", err)
		}
		count := 0
		for ok := iter.First(); ok; ok = iter.Next() {
			count++
		}
		if err := iter.Close(); err != nil {
			b.Fatalf("iterator close error = %v", err)
		}
		benchmarkSSTableCountSink = count
	}
}

func newBenchmarkReader(b *testing.B, count int, valueSize int) (*Reader, [][]byte) {
	b.Helper()
	dir := b.TempDir()
	manager := NewManager(dir, Options{BlockSize: 32 << 10})
	entries := benchmarkSSTableEntries(count, valueSize)
	meta, err := manager.Build(context.Background(), 1, 0, entries)
	if err != nil {
		b.Fatalf("Build setup table: %v", err)
	}
	reader, err := Open(filepath.Join(dir, FileName(1)), meta)
	if err != nil {
		b.Fatalf("Open reader: %v", err)
	}
	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = record.CloneBytes(entries[i].Key)
	}
	return reader, keys
}

func benchmarkSSTableEntries(count int, valueSize int) []record.Entry {
	entries := make([]record.Entry, count)
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	for i := range entries {
		entries[i] = record.NewPut([]byte(fmt.Sprintf("bench-key-%08d", i)), value, uint64(i+1))
	}
	return entries
}
