package sstable

import (
	"context"
	"errors"
	"testing"

	"mini-kv/internal/storage/lsm/record"
)

// ----------------sstable-------------------

func TestManagerBuildOpenGetAndIterator(t *testing.T) {
	manager := NewManager(t.TempDir(), Options{BlockSize: 32})
	entries := []record.Entry{
		record.NewPut([]byte("a"), []byte("1"), 1),
		record.NewPut([]byte("b"), []byte("2"), 2),
		record.NewPut([]byte("c"), []byte("3"), 3),
	}
	meta, err := manager.Build(context.Background(), 1, 0, entries)
	if err != nil {
		t.Fatalf("Build error = %v", err)
	}
	reader, err := manager.Open(meta)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = reader.Close() }()

	got, ok, err := reader.Get([]byte("b"), 10)
	if err != nil {
		t.Fatalf("Get error = %v", err)
	}
	if !ok || string(got.Value) != "2" {
		t.Fatalf("Get(b) = (%+v, %v), want value 2", got, ok)
	}

	iter, err := reader.NewIterator(10, record.KeyBounds{Lower: []byte("b")})
	if err != nil {
		t.Fatalf("NewIterator error = %v", err)
	}
	defer func() { _ = iter.Close() }()
	if !iter.First() || string(iter.Entry().Key) != "b" {
		t.Fatalf("iterator first = %+v, want b", iter.Entry())
	}
}

// ----------------version-----------------

func TestStateApplyFindAndDelete(t *testing.T) {
	state := (&State{NextFileNum: 1}).Apply(Edit{
		Added: []TableMeta{
			{FileNum: 1, Level: 0, Smallest: []byte("a"), Largest: []byte("c"), MaxSeq: 3},
			{FileNum: 2, Level: 1, Smallest: []byte("d"), Largest: []byte("f"), MaxSeq: 6},
		},
	})
	if state.NextFileNum != 3 || state.LastSeq != 6 {
		t.Fatalf("state after add = %+v, want next=3 lastSeq=6", state)
	}
	files := state.FilesForKey([]byte("b"))
	if len(files) != 1 || files[0].FileNum != 1 {
		t.Fatalf("FilesForKey(b) = %+v, want file 1", files)
	}

	state = state.Apply(Edit{Deleted: []uint64{1}})
	files = state.FilesForKey([]byte("b"))
	if len(files) != 0 {
		t.Fatalf("FilesForKey(b) after delete = %+v, want empty", files)
	}
}

func TestStateCloneIsIndependent(t *testing.T) {
	state := &State{Levels: [][]TableMeta{{{FileNum: 1, Smallest: []byte("a"), Largest: []byte("z")}}}}
	cloned := state.Clone()
	state.Levels[0][0].Smallest[0] = 'x'
	if string(cloned.Levels[0][0].Smallest) != "a" {
		t.Fatalf("clone smallest = %q, want a", cloned.Levels[0][0].Smallest)
	}
}

// ----------------bloom--------------------

func TestBloomHasNoFalseNegativesAfterRoundTrip(t *testing.T) {
	keys := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
	}
	builder := NewBloomBuilder(len(keys), 12)
	for _, key := range keys {
		builder.Add(key)
	}
	bloom := builder.Finish()

	encoded, err := bloom.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary error = %v", err)
	}
	decoded, err := DecodeBloom(encoded)
	if err != nil {
		t.Fatalf("DecodeBloom error = %v", err)
	}

	for _, key := range keys {
		if !decoded.MayContain(key) {
			t.Fatalf("MayContain(%q) = false, want true", key)
		}
	}
}

func TestEmptyBloomMisses(t *testing.T) {
	var bloom *Bloom
	if bloom.MayContain([]byte("missing")) {
		t.Fatal("nil bloom MayContain = true, want false")
	}
}

func TestDecodeBloomRejectsCorruption(t *testing.T) {
	_, err := DecodeBloom([]byte{1, 2, 3})
	if !errors.Is(err, ErrInvalidBloom) {
		t.Fatalf("DecodeBloom error = %v, want %v", err, ErrInvalidBloom)
	}
}

// -----------------index--------------------

func TestIndexFindAndRoundTrip(t *testing.T) {
	entries := []IndexEntry{
		{FirstKey: []byte("a"), LastKey: []byte("c"), Handle: BlockHandle{Offset: 10, Length: 100}},
		{FirstKey: []byte("d"), LastKey: []byte("f"), Handle: BlockHandle{Offset: 110, Length: 80}},
	}

	encoded, err := EncodeIndex(entries)
	if err != nil {
		t.Fatalf("EncodeIndex error = %v", err)
	}
	index, err := DecodeIndex(encoded)
	if err != nil {
		t.Fatalf("DecodeIndex error = %v", err)
	}

	handle, ok := index.Find([]byte("e"))
	if !ok || handle.Offset != 110 || handle.Length != 80 {
		t.Fatalf("Find(e) = (%+v, %v), want second block", handle, ok)
	}
	if _, ok := index.Find([]byte("z")); ok {
		t.Fatal("Find(z) = true, want false")
	}
	if _, ok := index.Find([]byte("0")); ok {
		t.Fatal("Find(0) = true, want false")
	}
}

func TestIndexRejectsOverlap(t *testing.T) {
	_, err := NewIndex([]IndexEntry{
		{FirstKey: []byte("a"), LastKey: []byte("d"), Handle: BlockHandle{Offset: 0, Length: 1}},
		{FirstKey: []byte("c"), LastKey: []byte("f"), Handle: BlockHandle{Offset: 1, Length: 1}},
	})
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("NewIndex overlap error = %v, want %v", err, ErrInvalidIndex)
	}
}

func TestDecodeIndexRejectsTrailingBytes(t *testing.T) {
	encoded, err := EncodeIndex([]IndexEntry{
		{FirstKey: []byte("a"), LastKey: []byte("b"), Handle: BlockHandle{Offset: 0, Length: 1}},
	})
	if err != nil {
		t.Fatalf("EncodeIndex error = %v", err)
	}
	encoded = append(encoded, 1)

	_, err = DecodeIndex(encoded)
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("DecodeIndex trailing error = %v, want %v", err, ErrInvalidIndex)
	}
}
