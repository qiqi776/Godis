package memtable

import (
	"bytes"
	"fmt"
	"testing"
)

var (
	benchmarkMemTableEntrySink any
	benchmarkMemTableBoolSink  bool
	benchmarkMemTableCountSink int
)

func BenchmarkTablePut(b *testing.B) {
	keys := benchmarkMemTableKeys(65536)
	value := bytes.Repeat([]byte("x"), 128)
	table := New()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%100_000 == 0 {
			b.StopTimer()
			table = New()
			b.StartTimer()
		}
		table.Put(keys[i&(len(keys)-1)], value, uint64(i+1))
	}
	benchmarkMemTableCountSink = table.Len()
}

func BenchmarkTableGet(b *testing.B) {
	keys := benchmarkMemTableKeys(65536)
	value := bytes.Repeat([]byte("x"), 128)
	table := New()
	for i, key := range keys {
		table.Put(key, value, uint64(i+1))
	}
	readSeq := uint64(len(keys))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry, ok := table.Get(keys[i&(len(keys)-1)], readSeq)
		benchmarkMemTableEntrySink = entry
		benchmarkMemTableBoolSink = ok
	}
}

func BenchmarkTableIterator(b *testing.B) {
	keys := benchmarkMemTableKeys(65536)
	value := bytes.Repeat([]byte("x"), 128)
	table := New()
	for i, key := range keys {
		table.Put(key, value, uint64(i+1))
	}
	readSeq := uint64(len(keys))

	b.ReportAllocs()
	for b.Loop() {
		iter := table.NewIterator(readSeq, benchmarkKeyBounds([]byte("bench-key-00010000"), []byte("bench-key-00020000")))
		count := 0
		for ok := iter.First(); ok; ok = iter.Next() {
			count++
		}
		if err := iter.Close(); err != nil {
			b.Fatalf("iterator close error = %v", err)
		}
		benchmarkMemTableCountSink = count
	}
}

func benchmarkMemTableKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%08d", i))
	}
	return keys
}

func benchmarkKeyBounds(lower, upper []byte) struct {
	Lower []byte
	Upper []byte
} {
	return struct {
		Lower []byte
		Upper []byte
	}{Lower: lower, Upper: upper}
}
