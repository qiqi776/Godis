package engine

import (
	"errors"
	"testing"
)

func TestSetOps(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	if n, err := db.SAdd("tags", "go", "redis", "go"); err != nil || n != 2 {
		t.Fatalf("unexpected sadd result: n=%d err=%v", n, err)
	}

	ok, err := db.SIsMember("tags", "go")
	if err != nil {
		t.Fatalf("unexpected sismember error: %v", err)
	}
	if !ok {
		t.Fatal("expected go to exist in set")
	}

	values, err := db.SMembers("tags")
	if err != nil {
		t.Fatalf("unexpected smembers error: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("unexpected smembers len: %d", len(values))
	}

	if n, err := db.SRem("tags", "go"); err != nil || n != 1 {
		t.Fatalf("unexpected srem result: n=%d err=%v", n, err)
	}

	ok, err = db.SIsMember("tags", "go")
	if err != nil {
		t.Fatalf("unexpected sismember error: %v", err)
	}
	if ok {
		t.Fatal("expected go to be removed")
	}
}

func TestSetDeleteKeyWhenEmpty(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	if _, err := db.SAdd("tags", "go"); err != nil {
		t.Fatalf("unexpected sadd error: %v", err)
	}

	if _, err := db.SRem("tags", "go"); err != nil {
		t.Fatalf("unexpected srem error: %v", err)
	}

	if got := db.Exists("tags"); got != 0 {
		t.Fatalf("expected set key to be deleted when empty, got exists=%d", got)
	}
}

func TestSetWrongType(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	db.Set("key", []byte("value"))

	if _, err := db.SAdd("key", "x"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected wrong type on sadd, got %v", err)
	}

	if _, err := db.SMembers("key"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected wrong type on smembers, got %v", err)
	}
}
