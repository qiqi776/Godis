package engine

import (
    "strconv"
    "sync"
    "testing"
)

func TestDBSetGetDelExists(t *testing.T) {
    t.Parallel()

    eng := NewEngine(1)
    db := eng.DB(0)

    db.Set("name", []byte("godis"))

    value, ok := db.Get("name")
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

    if _, ok := db.Get("name"); ok {
        t.Fatal("expected key to be deleted")
    }

    if got := db.Exists("name"); got != 0 {
        t.Fatalf("unexpected exists result after delete: %d", got)
    }
}

func TestDBGetReturnsCopy(t *testing.T) {
    t.Parallel()

    eng := NewEngine(1)
    db := eng.DB(0)

    db.Set("k", []byte("abc"))

    value, ok := db.Get("k")
    if !ok {
        t.Fatal("expected key to exist")
    }

    value[0] = 'z'

    again, ok := db.Get("k")
    if !ok {
        t.Fatal("expected key to exist on second read")
    }
    if string(again) != "abc" {
        t.Fatalf("stored value should not be mutated by caller, got %q", string(again))
    }
}

func TestDBConcurrentSetAndGet(t *testing.T) {
    t.Parallel()

    eng := NewEngine(1)
    db := eng.DB(0)

    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            key := "k" + strconv.Itoa(i)
            val := "v" + strconv.Itoa(i)
            db.Set(key, []byte(val))
            got, ok := db.Get(key)
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