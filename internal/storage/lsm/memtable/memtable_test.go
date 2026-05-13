package memtable

import (
	"bytes"
	"testing"

	"mini-kv/internal/storage/lsm/record"
)

func TestTableGetUsesNewestVisibleVersion(t *testing.T) {
	table := NewWithSeed(1)
	table.Put([]byte("a"), []byte("v1"), 1)
	table.Put([]byte("a"), []byte("v3"), 3)
	table.Delete([]byte("a"), 4)

	entry, ok := table.Get([]byte("a"), 2)
	if !ok || entry.Kind != record.KindPut || !bytes.Equal(entry.Value, []byte("v1")) {
		t.Fatalf("read seq 2 = (%+v, %v), want v1 put", entry, ok)
	}

	entry, ok = table.Get([]byte("a"), 3)
	if !ok || entry.Kind != record.KindPut || !bytes.Equal(entry.Value, []byte("v3")) {
		t.Fatalf("read seq 3 = (%+v, %v), want v3 put", entry, ok)
	}

	entry, ok = table.Get([]byte("a"), 4)
	if !ok || entry.Kind != record.KindDelete {
		t.Fatalf("read seq 4 = (%+v, %v), want delete tombstone", entry, ok)
	}
}

func TestIteratorOrdersKeysAndHonorsBounds(t *testing.T) {
	table := NewWithSeed(1)
	table.Put([]byte("a"), []byte("1"), 1)
	table.Put([]byte("b"), []byte("2"), 2)
	table.Put([]byte("b"), []byte("3"), 3)
	table.Put([]byte("c"), []byte("4"), 4)

	iter := table.NewIterator(3, record.KeyBounds{
		Lower: []byte("b"),
		Upper: []byte("d"),
	})
	defer func() { _ = iter.Close() }()

	if !iter.First() {
		t.Fatal("First() = false, want true")
	}
	entry := iter.Entry()
	if !bytes.Equal(entry.Key, []byte("b")) || !bytes.Equal(entry.Value, []byte("3")) {
		t.Fatalf("first entry = (%q, %q), want (b, 3)", entry.Key, entry.Value)
	}

	if iter.Next() {
		t.Fatalf("Next() = true at read seq 3, want c hidden because seq 4")
	}
}

func TestIteratorSeekNormalizesLowerBound(t *testing.T) {
	table := NewWithSeed(1)
	table.Put([]byte("a"), []byte("1"), 1)
	table.Put([]byte("b"), []byte("2"), 2)

	iter := table.NewIterator(10, record.KeyBounds{Lower: []byte("b")})
	defer func() { _ = iter.Close() }()

	if !iter.Seek([]byte("a")) {
		t.Fatal("Seek(a) = false, want true")
	}
	entry := iter.Entry()
	if !bytes.Equal(entry.Key, []byte("b")) {
		t.Fatalf("Seek(a) landed on %q, want b", entry.Key)
	}
}
