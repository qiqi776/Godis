package zset

import "testing"

func TestZSet(t *testing.T) {
    t.Parallel()

    z := New()

    if n, changed := z.Add("a", 1); n != 1 || !changed {
        t.Fatalf("unexpected add result: n=%d changed=%v", n, changed)
    }
    if n, changed := z.Add("b", 2); n != 1 || !changed {
        t.Fatalf("unexpected add result: n=%d changed=%v", n, changed)
    }
    if n, changed := z.Add("a", 3); n != 0 || !changed {
        t.Fatalf("unexpected update result: n=%d changed=%v", n, changed)
    }
    if n, changed := z.Add("a", 3); n != 0 || changed {
        t.Fatalf("unexpected noop update result: n=%d changed=%v", n, changed)
    }

    score, ok := z.Score("a")
    if !ok || score != 3 {
        t.Fatalf("unexpected score: %v ok=%v", score, ok)
    }

    values := z.Range(0, 1)
    if len(values) != 2 {
        t.Fatalf("unexpected range len: %d", len(values))
    }
    if values[0].Member != "b" || values[1].Member != "a" {
        t.Fatalf("unexpected order: %v %v", values[0], values[1])
    }

    if n := z.Remove("b"); n != 1 {
        t.Fatalf("unexpected remove result: %d", n)
    }
}