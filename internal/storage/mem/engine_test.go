package mem

import (
	"bytes"
	"errors"
	"testing"

	"mini-kv/internal/storage"
)

func TestWriteGet(t *testing.T) {
	engine := New()

	var batch storage.WriteBatch
	batch.Put([]byte("b"), []byte("2"))
	batch.Put([]byte("a"), []byte("1"))
	batch.Delete([]byte("b"))
	if err := engine.Write(&batch, storage.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("write error = %v", err)
	}

	value, ok, err := engine.Get([]byte("a"))
	if err != nil {
		t.Fatalf("get a error = %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("get a = (%q, %v), want (%q, true)", value, ok, []byte("1"))
	}

	value, ok, err = engine.Get([]byte("b"))
	if err != nil {
		t.Fatalf("get b error = %v", err)
	}
	if ok || value != nil {
		t.Fatalf("get b = (%q, %v), want (nil, false)", value, ok)
	}
}

func TestIterBounds(t *testing.T) {
	engine := New()

	var batch storage.WriteBatch
	batch.Put([]byte("a"), []byte("1"))
	batch.Put([]byte("b"), []byte("2"))
	batch.Put([]byte("c"), []byte("3"))
	if err := engine.Write(&batch, storage.WriteOptions{}); err != nil {
		t.Fatalf("write error = %v", err)
	}

	iter := engine.NewIterator(storage.IterOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
	})
	defer func() { _ = iter.Close() }()

	if !iter.First() {
		t.Fatal("first = false, want true")
	}
	if !bytes.Equal(iter.Key(), []byte("b")) || !bytes.Equal(iter.Value(), []byte("2")) {
		t.Fatalf("first = (%q, %q), want (%q, %q)", iter.Key(), iter.Value(), []byte("b"), []byte("2"))
	}
	if !iter.Next() {
		t.Fatal("next = false, want true")
	}
	if !bytes.Equal(iter.Key(), []byte("c")) || !bytes.Equal(iter.Value(), []byte("3")) {
		t.Fatalf("next = (%q, %q), want (%q, %q)", iter.Key(), iter.Value(), []byte("c"), []byte("3"))
	}
	if iter.Next() {
		t.Fatal("next past end = true, want false")
	}
}

func TestSnapshotStable(t *testing.T) {
	engine := New()

	var batch storage.WriteBatch
	batch.Put([]byte("a"), []byte("1"))
	if err := engine.Write(&batch, storage.WriteOptions{}); err != nil {
		t.Fatalf("write error = %v", err)
	}

	snap, err := engine.Snapshot()
	if err != nil {
		t.Fatalf("snapshot error = %v", err)
	}
	defer func() { _ = snap.Close() }()

	batch.Reset()
	batch.Put([]byte("a"), []byte("2"))
	if err := engine.Write(&batch, storage.WriteOptions{}); err != nil {
		t.Fatalf("write after snapshot error = %v", err)
	}

	value, ok, err := snap.Get([]byte("a"))
	if err != nil {
		t.Fatalf("snapshot get error = %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("snapshot get = (%q, %v), want (%q, true)", value, ok, []byte("1"))
	}
}

func TestClose(t *testing.T) {
	engine := New()
	if err := engine.Close(); err != nil {
		t.Fatalf("close error = %v", err)
	}

	if _, _, err := engine.Get([]byte("a")); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("get after close error = %v, want %v", err, storage.ErrClosed)
	}
	if err := engine.Flush(); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("flush after close error = %v, want %v", err, storage.ErrClosed)
	}
}
