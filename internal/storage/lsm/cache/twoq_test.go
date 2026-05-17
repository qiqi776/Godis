package cache

import "testing"

func TestTwoQueueProtectsPromotedEntries(t *testing.T) {
	cache := NewTwoQueue[string, int](2)
	cache.Add("a", 1)
	cache.Add("b", 2)

	if value, ok := cache.Get("a"); !ok || value != 1 {
		t.Fatalf("Get(a) = (%d, %v), want (1, true)", value, ok)
	}

	cache.Add("c", 3)
	if _, ok := cache.Get("b"); ok {
		t.Fatal("Get(b) = true, want false after recent eviction")
	}
	if value, ok := cache.Get("a"); !ok || value != 1 {
		t.Fatalf("Get(a) after eviction = (%d, %v), want promoted value", value, ok)
	}
	if value, ok := cache.Get("c"); !ok || value != 3 {
		t.Fatalf("Get(c) = (%d, %v), want (3, true)", value, ok)
	}
	if cache.GhostLen() != 1 {
		t.Fatalf("GhostLen() = %d, want 1", cache.GhostLen())
	}
}

func TestTwoQueueGhostReadmission(t *testing.T) {
	cache := NewTwoQueue[string, int](2)
	cache.Add("a", 1)
	cache.Add("b", 2)
	cache.Add("c", 3)

	if _, ok := cache.Get("a"); ok {
		t.Fatal("Get(a) = true, want false after eviction to ghost")
	}
	cache.Add("a", 10)
	if value, ok := cache.Get("a"); !ok || value != 10 {
		t.Fatalf("readmitted Get(a) = (%d, %v), want (10, true)", value, ok)
	}
}

func TestTwoQueueRemoveAndClear(t *testing.T) {
	cache := NewTwoQueue[string, int](2)
	cache.Add("a", 1)
	cache.Add("b", 2)

	if !cache.Remove("a") {
		t.Fatal("Remove(a) = false, want true")
	}
	if cache.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cache.Len())
	}
	cache.Clear()
	if cache.Len() != 0 || cache.GhostLen() != 0 {
		t.Fatalf("after Clear len=%d ghost=%d, want 0/0", cache.Len(), cache.GhostLen())
	}
}
