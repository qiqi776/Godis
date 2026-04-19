package engine

import (
	"errors"
	"testing"
)

func TestBitmapOps(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	old, err := db.SetBit("bits", 7, 1)
	if err != nil {
		t.Fatalf("unexpected setbit error: %v", err)
	}
	if old != 0 {
		t.Fatalf("unexpected old bit: %d", old)
	}

	bit, err := db.GetBit("bits", 7)
	if err != nil {
		t.Fatalf("unexpected getbit error: %v", err)
	}
	if bit != 1 {
		t.Fatalf("unexpected bit value: %d", bit)
	}

	count, err := db.BitCount("bits")
	if err != nil {
		t.Fatalf("unexpected bitcount error: %v", err)
	}
	if count != 1 {
		t.Fatalf("unexpected bitcount: %d", count)
	}
}

func TestBitmapWrongType(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)
	db.Set("key", []byte("value"))

	if _, err := db.SetBit("key", 1, 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected wrong type on setbit, got %v", err)
	}

	if _, err := db.GetBit("key", 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected wrong type on getbit, got %v", err)
	}
}
