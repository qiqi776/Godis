package bitmap

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBasicOps(t *testing.T) {
	bm := Make()
	if bm == nil || len(*bm) != 0 || cap(*bm) < BlockSize {
		t.Errorf("Make() invalid: len=%d, cap=%d", len(*bm), cap(*bm))
	}
	if bm.GetBit(100) != 0 {
		t.Error("GetBit out of bounds should be 0")
	}
	tests := []struct {
		off int64
		val byte
	}{
		{0, 1}, {1, 1}, {7, 1}, {8, 1}, {63, 1}, {64, 1},
		{100, 1}, {1000, 1}, {0, 0}, {1000, 0},
	}
	for _, tt := range tests {
		bm.SetBit(tt.off, tt.val)
		if got := bm.GetBit(tt.off); got != tt.val {
			t.Errorf("SetBit(%d, %d) failed, got %d", tt.off, tt.val, got)
		}
	}
	if size := bm.BitSize(); size < 1000 {
		t.Errorf("BitSize incorrect, expected >1000, got %d", size)
	}
	expectedSize := len(bm.ToBytes()) * 8
	if size := bm.BitSize(); size != expectedSize {
		t.Errorf("BitSize incorrect, expected %d, got %d", expectedSize, size)
	}
}

func TestExpansionAndBytes(t *testing.T) {
	bm := Make()
	bm.SetBit(0, 1)
	if len(*bm) == 0 {
		t.Error("Should grow on first set")
	}
	bm.SetBit(511, 1)
	len1 := len(bm.ToBytes())
	if len1 < 64 {
		t.Errorf("Expected len >= 64, got %d", len1)
	}
	bm.SetBit(512, 1)
	len2 := len(bm.ToBytes())
	if len2 <= len1 || len2%BlockSize != 0 {
		t.Errorf("Expansion error: old=%d, new=%d (must match BlockSize)", len1, len2)
	}
	raw := []byte{0x01, 0xFF, 0xAA, 0x00}
	bm2 := FromBytes(raw)
	if !bytes.Equal(bm2.ToBytes(), raw) {
		t.Errorf("FromBytes/ToBytes mismatch")
	}
	if bm2.GetBit(0) != 1 || bm2.GetBit(1) != 0 || bm2.GetBit(8) != 1 {
		t.Error("FromBytes value check failed")
	}
}

func TestIterators(t *testing.T) {
	bm := Make()
	setup := []int64{0, 1, 15, 64, 100}
	for _, off := range setup {
		bm.SetBit(off, 1)
	}
	t.Run("ForEachBit", func(t *testing.T) {
		cases := []struct {
			begin, end int64
			wantMap    map[int64]byte
		}{
			{0, 0, map[int64]byte{0: 1, 1: 1, 2: 0, 15: 1, 64: 1, 100: 1}},
			{2, 16, map[int64]byte{2: 0, 15: 1}},
		}
		for _, tc := range cases {
			got := make(map[int64]byte)
			bm.ForEachBit(tc.begin, tc.end, func(off int64, val byte) bool {
				if _, exists := tc.wantMap[off]; exists || val == 1 {
					got[off] = val
				}
				return true
			})
			for k, v := range got {
				if _, ok := tc.wantMap[k]; !ok && v == 0 {
					delete(got, k)
				}
			}
			if !reflect.DeepEqual(got, tc.wantMap) {
				t.Errorf("ForEachBit(%d,%d) mismatch.\nGot:  %v\nWant: %v", tc.begin, tc.end, got, tc.wantMap)
			}
		}
	})
	t.Run("ForEachSetBit", func(t *testing.T) {
		cases := []struct {
			name       string
			begin, end int64
			want       []int64
		}{
			{"Full", 0, 0, []int64{0, 1, 15, 64, 100}},
			{"Skip Start", 2, 0, []int64{15, 64, 100}},
			{"Cut End", 0, 65, []int64{0, 1, 15, 64}},
			{"Middle Check Empty", 3, 10, []int64{}},
		}
		for _, tc := range cases {
			runSetBitTest(t, bm, tc.name, tc.begin, tc.end, tc.want)
		}
		bm.SetBit(8, 1)
		runSetBitTest(t, bm, "Middle Range Hit", 3, 10, []int64{8})
		bm.SetBit(8, 0)
	})
	t.Run("ForEachByte", func(t *testing.T) {
		expectBytes := map[int]byte{
			0: 3, 1: 128, 8: 1, 12: 16,
		}
		visited := 0
		bm.ForEachByte(0, 0, func(idx int, val byte) bool {
			visited++
			if want, ok := expectBytes[idx]; ok {
				if val != want {
					t.Errorf("Full: Byte %d got %d, want %d", idx, val, want)
				}
			} else if val != 0 {
				t.Errorf("Full: Byte %d expected 0, got %d", idx, val)
			}
			return true
		})
		if visited < 13 {
			t.Error("Full: Too few bytes visited")
		}
		targetCount := 0
		bm.ForEachByte(1, 9, func(idx int, val byte) bool {
			targetCount++
			if idx < 1 || idx >= 9 {
				t.Errorf("Partial: Visited index %d out of requested range [1, 9)", idx)
			}
			if want, ok := expectBytes[idx]; ok {
				if val != want {
					t.Errorf("Partial: Byte %d got %d, want %d", idx, val, want)
				}
			}
			return true
		})
		if targetCount != 8 {
			t.Errorf("Partial: Expected 8 bytes visited, got %d", targetCount)
		}
	})
}

func runSetBitTest(t *testing.T, bm *BitMap, name string, begin, end int64, want []int64) {
	t.Helper()
	var got []int64
	bm.ForEachSetBit(begin, end, func(off int64) bool {
		got = append(got, off)
		return true
	})
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: Range(%d,%d) got %v, want %v", name, begin, end, got, want)
	}
}
