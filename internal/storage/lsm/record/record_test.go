package record

import (
	"errors"
	"testing"
)

func TestCloneBytesReturnsIndependentCopy(t *testing.T) {
	input := []byte("abc")
	cloned := CloneBytes(input)
	input[0] = 'z'
	if string(cloned) != "abc" {
		t.Fatalf("CloneBytes result = %q, want abc", cloned)
	}
}

func TestBytePoolReusesResetBuffers(t *testing.T) {
	pool := NewBytePool(8)
	buf := pool.Get()
	buf = append(buf, "abc"...)
	pool.Put(buf)

	got := pool.Get()
	if len(got) != 0 {
		t.Fatalf("pooled buffer len = %d, want 0", len(got))
	}
}

func TestBatchFrameRoundTrip(t *testing.T) {
	batch := Batch{
		SeqStart: 7,
		Entries: []Entry{
			NewPut(nil, nil, 7),
			NewPut([]byte("a"), []byte("1"), 8),
			NewDelete([]byte("b"), 9),
		},
	}
	encoded, err := EncodeBatchFrame(batch)
	if err != nil {
		t.Fatalf("EncodeBatchFrame error = %v", err)
	}
	got, consumed, err := DecodeBatchFrame(encoded)
	if err != nil {
		t.Fatalf("DecodeBatchFrame error = %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("consumed = %d, want %d", consumed, len(encoded))
	}
	if got.SeqStart != batch.SeqStart || len(got.Entries) != len(batch.Entries) {
		t.Fatalf("decoded batch = %+v, want %+v", got, batch)
	}
}

func TestRecordFrameRoundTrip(t *testing.T) {
	entry := NewPut([]byte("k"), []byte("v"), 11)
	encoded, err := EncodeRecord(entry)
	if err != nil {
		t.Fatalf("EncodeRecord error = %v", err)
	}
	got, consumed, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord error = %v", err)
	}
	if consumed != len(encoded) || string(got.Key) != "k" || string(got.Value) != "v" || got.Seq != 11 {
		t.Fatalf("DecodeRecord = (%+v, %d), want entry and full consumed", got, consumed)
	}
}

func TestDecodeFrameRejectsCorruption(t *testing.T) {
	encoded := EncodeFrame([]byte("payload"))
	encoded[len(encoded)-1] ^= 0xff
	if _, _, err := DecodeFrame(encoded); !errors.Is(err, ErrChecksum) {
		t.Fatalf("DecodeFrame checksum error = %v, want %v", err, ErrChecksum)
	}

	encoded = EncodeFrame([]byte("payload"))
	encoded = encoded[:len(encoded)-1]
	if _, _, err := DecodeFrame(encoded); !errors.Is(err, ErrPartial) {
		t.Fatalf("DecodeFrame partial error = %v, want %v", err, ErrPartial)
	}
}
