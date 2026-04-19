package set

import "testing"

func TestSet(t *testing.T) {
	t.Parallel()

	s := New()

	if n := s.Add("a", "b", "a"); n != 2 {
		t.Fatalf("unexpected add result: %d", n)
	}

	if !s.Has("a") || !s.Has("b") {
		t.Fatal("expected members to exist")
	}

	if n := s.Remove("a", "c"); n != 1 {
		t.Fatalf("unexpected remove result: %d", n)
	}

	if s.Has("a") {
		t.Fatal("expected a to be removed")
	}

	if s.Len() != 1 {
		t.Fatalf("unexpected set len: %d", s.Len())
	}
}

func TestMembers(t *testing.T) {
	t.Parallel()

	s := New()
	s.Add("b", "a")

	values := s.Members()
	if len(values) != 2 {
		t.Fatalf("unexpected members len: %d", len(values))
	}

	if string(values[0]) != "a" || string(values[1]) != "b" {
		t.Fatalf("unexpected members order: %q %q", string(values[0]), string(values[1]))
	}
}
