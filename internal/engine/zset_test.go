package engine

import (
	"errors"
	"testing"
)

func TestZSetOps(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	if n, err := db.ZAdd("rank", 1, "a"); err != nil || n != 1 {
		t.Fatalf("unexpected zadd result: n=%d err=%v", n, err)
	}
	if n, err := db.ZAdd("rank", 2, "b"); err != nil || n != 1 {
		t.Fatalf("unexpected zadd result: n=%d err=%v", n, err)
	}
	if n, err := db.ZAdd("rank", 3, "a"); err != nil || n != 0 {
		t.Fatalf("unexpected zadd update result: n=%d err=%v", n, err)
	}

	score, ok, err := db.ZScore("rank", "a")
	if err != nil {
		t.Fatalf("unexpected zscore error: %v", err)
	}
	if !ok || score != 3 {
		t.Fatalf("unexpected zscore result: score=%v ok=%v", score, ok)
	}

	values, err := db.ZRange("rank", 0, -1)
	if err != nil {
		t.Fatalf("unexpected zrange error: %v", err)
	}
	if len(values) != 2 || string(values[0]) != "b" || string(values[1]) != "a" {
		t.Fatalf("unexpected zrange values: %#v", values)
	}

	if n, err := db.ZRem("rank", "b"); err != nil || n != 1 {
		t.Fatalf("unexpected zrem result: n=%d err=%v", n, err)
	}
}

func TestZSetWrongType(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)
	db.Set("key", []byte("value"))

	if _, err := db.ZAdd("key", 1, "a"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected wrong type on zadd, got %v", err)
	}
}
