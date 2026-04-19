package bitmap

import "testing"

func TestBitmap(t *testing.T) {
	t.Parallel()

	bm := New()

	old, err := bm.SetBit(7, 1)
	if err != nil {
		t.Fatalf("unexpected setbit error: %v", err)
	}
	if old != 0 {
		t.Fatalf("unexpected old bit: %d", old)
	}

	bit, err := bm.GetBit(7)
	if err != nil {
		t.Fatalf("unexpected getbit error: %v", err)
	}
	if bit != 1 {
		t.Fatalf("unexpected bit value: %d", bit)
	}

	old, err = bm.SetBit(7, 0)
	if err != nil {
		t.Fatalf("unexpected setbit error: %v", err)
	}
	if old != 1 {
		t.Fatalf("unexpected old bit after reset: %d", old)
	}

	bit, err = bm.GetBit(7)
	if err != nil {
		t.Fatalf("unexpected getbit error: %v", err)
	}
	if bit != 0 {
		t.Fatalf("unexpected bit value after reset: %d", bit)
	}
}

func TestCount(t *testing.T) {
	t.Parallel()

	bm := New()
	_, _ = bm.SetBit(0, 1)
	_, _ = bm.SetBit(1, 1)
	_, _ = bm.SetBit(9, 1)

	if got := bm.Count(); got != 3 {
		t.Fatalf("unexpected bitcount: %d", got)
	}
}