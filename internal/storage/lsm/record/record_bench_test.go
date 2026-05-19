package record

import (
	"bytes"
	"fmt"
	"testing"
)

var (
	benchmarkRecordBytesSink []byte
	benchmarkRecordBatchSink Batch
)

func BenchmarkEncodeBatchFrame(b *testing.B) {
	batch := benchmarkBatch(128, 256)

	b.ReportAllocs()
	for b.Loop() {
		encoded, err := EncodeBatchFrame(batch)
		if err != nil {
			b.Fatalf("EncodeBatchFrame error = %v", err)
		}
		benchmarkRecordBytesSink = encoded
	}
}

func BenchmarkDecodeBatchFrame(b *testing.B) {
	encoded, err := EncodeBatchFrame(benchmarkBatch(128, 256))
	if err != nil {
		b.Fatalf("EncodeBatchFrame setup error = %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		batch, _, err := DecodeBatchFrame(encoded)
		if err != nil {
			b.Fatalf("DecodeBatchFrame error = %v", err)
		}
		benchmarkRecordBatchSink = batch
	}
}

func benchmarkBatch(count int, valueSize int) Batch {
	value := bytes.Repeat([]byte("x"), valueSize)
	entries := make([]Entry, count)
	for i := range entries {
		entries[i] = NewPut([]byte(fmt.Sprintf("bench-key-%08d", i)), value, uint64(i+1))
	}
	return Batch{
		SeqStart: 1,
		Entries:  entries,
	}
}
