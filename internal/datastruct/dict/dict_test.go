package dict

import "testing"

func TestPutGetRemove(t *testing.T) {
	t.Parallel()

	d := New()

	if n := d.Put("name", "godis"); n != 1 {
		t.Fatalf("unexpected put result: %d", n)
	}

	value, ok := d.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if value.(string) != "godis" {
		t.Fatalf("unexpected value: %v", value)
	}

	if n := d.Put("name", "redis"); n != 0 {
		t.Fatalf("unexpected overwrite put result: %d", n)
	}

	value, ok = d.Get("name")
	if !ok || value.(string) != "redis" {
		t.Fatalf("unexpected value after overwrite: %v ok=%v", value, ok)
	}

	removed, n := d.Remove("name")
	if n != 1 {
		t.Fatalf("unexpected remove result: %d", n)
	}
	if removed.(string) != "redis" {
		t.Fatalf("unexpected removed value: %v", removed)
	}

	if _, ok := d.Get("name"); ok {
		t.Fatal("expected key to be removed")
	}
}

func TestForEach(t *testing.T) {
	t.Parallel()

	d := New()
	d.Put("a", 1)
	d.Put("b", 2)

	seen := 0
	d.ForEach(func(key string, value any) bool {
		seen++
		return true
	})

	if seen != 2 {
		t.Fatalf("unexpected foreach count: %d", seen)
	}
}
