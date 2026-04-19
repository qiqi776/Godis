package hash

import "testing"

func TestSetGetDel(t *testing.T) {
	t.Parallel()

	h := New()

	if n := h.Set("name", []byte("godis")); n != 1 {
		t.Fatalf("unexpected first hset result: %d", n)
	}

	if n := h.Set("name", []byte("redis")); n != 0 {
		t.Fatalf("unexpected second hset result: %d", n)
	}

	value, ok := h.Get("name")
	if !ok || string(value) != "redis" {
		t.Fatalf("unexpected hget result: %q ok=%v", string(value), ok)
	}

	if n := h.Del("name"); n != 1 {
		t.Fatalf("unexpected hdel result: %d", n)
	}

	if _, ok := h.Get("name"); ok {
		t.Fatal("expected field to be deleted")
	}
}

func TestGetAll(t *testing.T) {
	t.Parallel()

	h := New()
	h.Set("b", []byte("2"))
	h.Set("a", []byte("1"))

	values := h.GetAll()
	want := []string{"a", "1", "b", "2"}

	if len(values) != len(want) {
		t.Fatalf("unexpected getall len: %d", len(values))
	}
	for i, value := range values {
		if string(value) != want[i] {
			t.Fatalf("unexpected getall value at %d: %q", i, string(value))
		}
	}
}