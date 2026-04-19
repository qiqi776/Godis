package engine

import (
	"errors"
	"testing"
)

func TestList(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	if n, err := db.LPush("list", []byte("b"), []byte("a")); err != nil || n != 2 {
		t.Fatalf("unexpected lpush result: n=%d err=%v", n, err)
	}

	if n, err := db.RPush("list", []byte("c"), []byte("d")); err != nil || n != 4 {
		t.Fatalf("unexpected rpush result: n=%d err=%v", n, err)
	}

	values, err := db.LRange("list", 0, -1)
	if err != nil {
		t.Fatalf("unexpected lrange error: %v", err)
	}

	want := []string{"a", "b", "c", "d"}
	if len(values) != len(want) {
		t.Fatalf("unexpected lrange size: %d", len(values))
	}
	for i, item := range values {
		if string(item) != want[i] {
			t.Fatalf("unexpected list value at %d: %q", i, string(item))
		}
	}

	left, ok, err := db.LPop("list")
	if err != nil || !ok || string(left) != "a" {
		t.Fatalf("unexpected lpop result: value=%q ok=%v err=%v", string(left), ok, err)
	}

	right, ok, err := db.RPop("list")
	if err != nil || !ok || string(right) != "d" {
		t.Fatalf("unexpected rpop result: value=%q ok=%v err=%v", string(right), ok, err)
	}

	values, err = db.LRange("list", -2, -1)
	if err != nil {
		t.Fatalf("unexpected tail range error: %v", err)
	}
	if len(values) != 2 || string(values[0]) != "b" || string(values[1]) != "c" {
		t.Fatalf("unexpected tail range: %#v", values)
	}
}

func TestWrongType(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)
	db.Set("key", []byte("value"))

	if _, err := db.LPush("key", []byte("x")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected wrong type on lpush, got %v", err)
	}

	if _, err := db.LRange("key", 0, -1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected wrong type on lrange, got %v", err)
	}
}