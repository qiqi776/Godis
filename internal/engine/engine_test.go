package engine

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestKV(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	db.Set("name", []byte("godis"))

	value, ok, err := db.Get("name")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(value) != "godis" {
		t.Fatalf("unexpected value: %q", string(value))
	}

	if got := db.Exists("name"); got != 1 {
		t.Fatalf("unexpected exists result: %d", got)
	}

	if got := db.Del("name"); got != 1 {
		t.Fatalf("unexpected del result: %d", got)
	}

	if _, ok, err := db.Get("name"); err != nil || ok {
		t.Fatal("expected key to be deleted")
	}

	if got := db.Exists("name"); got != 0 {
		t.Fatalf("unexpected exists result after delete: %d", got)
	}
}

func TestCopy(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	db.Set("k", []byte("abc"))

	value, ok, err := db.Get("k")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok {
		t.Fatal("expected key to exist")
	}

	value[0] = 'z'

	again, ok, err := db.Get("k")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok {
		t.Fatal("expected key to exist on second read")
	}
	if string(again) != "abc" {
		t.Fatalf("stored value should not be mutated by caller, got %q", string(again))
	}
}

func TestConcurrent(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "k" + strconv.Itoa(i)
			val := "v" + strconv.Itoa(i)
			db.Set(key, []byte(val))
			got, ok, err := db.Get(key)
			if err != nil {
				t.Errorf("unexpected get error for %s: %v", key, err)
				return
			}
			if !ok {
				t.Errorf("expected key %s to exist", key)
				return
			}
			if string(got) != val {
				t.Errorf("unexpected value for %s: %q", key, string(got))
			}
		}(i)
	}

	wg.Wait()
}

func TestPersist(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	db.Set("temp", []byte("42"))

	if ttl := db.TTL("temp"); ttl != -1 {
		t.Fatalf("expected ttl -1 for key without expire, got %d", ttl)
	}

	if !db.Expire("temp", 1500*time.Millisecond) {
		t.Fatal("expected expire to succeed")
	}

	if ttl := db.TTL("temp"); ttl < 1 {
		t.Fatalf("expected positive ttl, got %d", ttl)
	}

	if !db.Persist("temp") {
		t.Fatal("expected persist to succeed")
	}

	if ttl := db.TTL("temp"); ttl != -1 {
		t.Fatalf("expected ttl -1 after persist, got %d", ttl)
	}

	if db.Persist("temp") {
		t.Fatal("persist should fail when key has no expire")
	}

	if db.Expire("missing", time.Second) {
		t.Fatal("expire should fail for missing key")
	}

	if ttl := db.TTL("missing"); ttl != -2 {
		t.Fatalf("expected ttl -2 for missing key, got %d", ttl)
	}
}

func TestExpire(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	db.Set("gone", []byte("1"))
	if !db.Expire("gone", 50*time.Millisecond) {
		t.Fatal("expected expire to succeed")
	}

	time.Sleep(80 * time.Millisecond)

	if _, ok, err := db.Get("gone"); err != nil || ok {
		t.Fatal("expected expired key to be removed")
	}

	if got := db.Exists("gone"); got != 0 {
		t.Fatalf("expected expired key to be absent, got exists=%d", got)
	}

	if ttl := db.TTL("gone"); ttl != -2 {
		t.Fatalf("expected ttl -2 for expired key, got %d", ttl)
	}
}