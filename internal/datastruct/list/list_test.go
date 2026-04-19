package list

import "testing"

func TestPushPop(t *testing.T) {
	t.Parallel()

	l := New()

	l.PushFront([]byte("b"))
	l.PushFront([]byte("a"))
	l.PushBack([]byte("c"))

	if l.Len() != 3 {
		t.Fatalf("unexpected len: %d", l.Len())
	}

	v, ok := l.PopFront()
	if !ok || string(v) != "a" {
		t.Fatalf("unexpected front pop: %q ok=%v", string(v), ok)
	}

	v, ok = l.PopBack()
	if !ok || string(v) != "c" {
		t.Fatalf("unexpected back pop: %q ok=%v", string(v), ok)
	}

	v, ok = l.PopFront()
	if !ok || string(v) != "b" {
		t.Fatalf("unexpected final pop: %q ok=%v", string(v), ok)
	}

	if l.Len() != 0 {
		t.Fatalf("unexpected len after pops: %d", l.Len())
	}
}

func TestRange(t *testing.T) {
	t.Parallel()

	l := New()
	l.PushBack([]byte("a"))
	l.PushBack([]byte("b"))
	l.PushBack([]byte("c"))
	l.PushBack([]byte("d"))

	values := l.Range(1, 2)
	if len(values) != 2 {
		t.Fatalf("unexpected range len: %d", len(values))
	}
	if string(values[0]) != "b" || string(values[1]) != "c" {
		t.Fatalf("unexpected range values: %q %q", string(values[0]), string(values[1]))
	}
}