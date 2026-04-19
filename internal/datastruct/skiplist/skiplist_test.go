package skiplist

import "testing"

func TestInsertRangeRemove(t *testing.T) {
	t.Parallel()

	sl := New()
	sl.Insert("c", 3)
	sl.Insert("a", 1)
	sl.Insert("d", 2)
	sl.Insert("b", 2)

	values := sl.Range(0, 3)
	want := []string{"a", "b", "d", "c"}

	if len(values) != len(want) {
		t.Fatalf("unexpected range len: %d", len(values))
	}
	for i, v := range values {
		if v.Member != want[i] {
			t.Fatalf("unexpected member at %d: %s", i, v.Member)
		}
	}

	if !sl.Remove("d", 2) {
		t.Fatal("expected remove to succeed")
	}

	values = sl.Range(0, 2)
	want = []string{"a", "b", "c"}
	if len(values) != len(want) {
		t.Fatalf("unexpected range len after remove: %d", len(values))
	}
	for i, v := range values {
		if v.Member != want[i] {
			t.Fatalf("unexpected member after remove at %d: %s", i, v.Member)
		}
	}
}