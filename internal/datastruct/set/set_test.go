package set

import (
	"reflect"
	"sort"
	"testing"
)

func assertSliceEqual(t *testing.T, expected, actual []string) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("Slice length mismatch. Expected %d, got %d", len(expected), len(actual))
		return
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Slice content mismatch.\nExpected: %v\nGot:      %v", expected, actual)
	}
}

func TestMakeAndAdd(t *testing.T) {
	s := Make("a", "b")
	if s.Len() != 2 {
		t.Errorf("Expected len 2, got %d", s.Len())
	}
	if !s.Has("a") || !s.Has("b") {
		t.Error("Set should contain initial members")
	}
	count := s.Add("c")
	if count != 1 {
		t.Errorf("Expected add return 1 for new element, got %d", count)
	}
	if s.Len() != 3 {
		t.Errorf("Expected len 3, got %d", s.Len())
	}
	count = s.Add("a")
	if count != 0 {
		t.Errorf("Expected add return 0 for existing element, got %d", count)
	}
	if s.Len() != 3 {
		t.Errorf("Expected len to remain 3, got %d", s.Len())
	}
}

func TestRemove(t *testing.T) {
	s := Make("a", "b", "c")
	count := s.Remove("b")
	if count != 1 {
		t.Errorf("Expected remove return 1, got %d", count)
	}
	if s.Has("b") {
		t.Error("Set should not contain removed element")
	}
	if s.Len() != 2 {
		t.Errorf("Expected len 2, got %d", s.Len())
	}
	count = s.Remove("z")
	if count != 0 {
		t.Errorf("Expected remove return 0 for non-exist element, got %d", count)
	}
	if s.Len() != 2 {
		t.Errorf("Expected len to remain 2, got %d", s.Len())
	}
}

func TestToSlice(t *testing.T) {
	s := Make("apple", "banana", "orange")
	slice := s.ToSlice()

	expected := []string{"apple", "banana", "orange"}
	assertSliceEqual(t, expected, slice)
}

func TestForEach(t *testing.T) {
	s := Make("1", "2", "3")
	count := 0
	sum := 0
	s.ForEach(func(member string) bool {
		count++
		if member == "1" { sum += 1 }
		if member == "2" { sum += 2 }
		if member == "3" { sum += 3 }
		return true
	})
	if count != 3 {
		t.Errorf("Expected to iterate 3 times, got %d", count)
	}
	if sum != 6 {
		t.Errorf("Expected sum 6, got %d", sum)
	}
}

func TestShallowCopy(t *testing.T) {
	s1 := Make("a", "b")
	s2 := s1.ShallowCopy()
	if s2.Len() != s1.Len() {
		t.Error("Copy should have same length")
	}
	s1.Add("c")
	if s2.Has("c") {
		t.Error("ShallowCopy should be independent")
	}
	if s2.Len() != 2 {
		t.Error("s2 length should remain 2")
	}
}

func TestIntersect(t *testing.T) {
	s1 := Make("a", "b", "c")
	s2 := Make("b", "c", "d")
	s3 := Make("c", "d", "e")
	res1 := Intersect(s1, s2)
	assertSliceEqual(t, []string{"b", "c"}, res1.ToSlice())
	res2 := Intersect(s1, s2, s3)
	assertSliceEqual(t, []string{"c"}, res2.ToSlice())
	s4 := Make("x", "y")
	res3 := Intersect(s1, s4)
	if res3.Len() != 0 {
		t.Errorf("Expected empty set, got len %d", res3.Len())
	}
	res4 := Intersect()
	if res4.Len() != 0 {
		t.Error("Intersect with no sets should return empty set")
	}
}

func TestUnion(t *testing.T) {
	s1 := Make("a", "b")
	s2 := Make("b", "c")
	s3 := Make("c", "d")
	res1 := Union(s1, s2)
	assertSliceEqual(t, []string{"a", "b", "c"}, res1.ToSlice())
	res2 := Union(s1, s2, s3)
	assertSliceEqual(t, []string{"a", "b", "c", "d"}, res2.ToSlice())
}

func TestDiff(t *testing.T) {
	s1 := Make("a", "b", "c", "d")
	s2 := Make("b", "d")
	s3 := Make("a", "e")
	res1 := Diff(s1, s2)
	assertSliceEqual(t, []string{"a", "c"}, res1.ToSlice())
	res2 := Diff(s1, s2, s3)
	assertSliceEqual(t, []string{"c"}, res2.ToSlice())
	empty := Make()
	res3 := Diff(s1, empty)
	assertSliceEqual(t, s1.ToSlice(), res3.ToSlice())
}

func TestRandomMembers(t *testing.T) {
	s := Make("a", "b", "c", "d", "e")
	randMembers := s.RandomMembers(3)
	if len(randMembers) != 3 {
		t.Errorf("Expected 3 random members, got %d", len(randMembers))
	}
	for _, m := range randMembers {
		if !s.Has(m) {
			t.Errorf("Random member %s not found in original set", m)
		}
	}
}

func TestRandomDistinctMembers(t *testing.T) {
	s := Make("a", "b", "c")
	randMembers := s.RandomDistinctMembers(5)
	if len(randMembers) != 3 {
		t.Errorf("Expected 3 members (capped by set len), got %d", len(randMembers))
	}
	checkMap := make(map[string]bool)
	for _, m := range randMembers {
		if checkMap[m] {
			t.Error("Duplicate member found in RandomDistinctMembers")
		}
		checkMap[m] = true
	}
}

func TestSetScan(t *testing.T) {
	s := Make("apple", "apricot", "banana", "cherry")
	result, _ := s.SetScan(0, 10, "a*")
	var strResult []string
	for _, bytes := range result {
		strResult = append(strResult, string(bytes))
	}
	expected := []string{"apple", "apricot"}
	assertSliceEqual(t, expected, strResult)
	result2, _ := s.SetScan(0, 10, "*nan*") 
	var strResult2 []string
	for _, bytes := range result2 {
		strResult2 = append(strResult2, string(bytes))
	}
	assertSliceEqual(t, []string{"banana"}, strResult2)
	result3, _ := s.SetScan(0, 10, "*")
	if len(result3) != 4 {
		t.Errorf("Expected 4 items for * pattern, got %d", len(result3))
	}
}

func TestNilSet(t *testing.T) {
	var s *Set = nil
	if s.Len() != 0 {
		t.Error("Nil set len should be 0")
	}
	if s.RandomMembers(5) != nil {
		t.Error("Nil set RandomMembers should be nil")
	}
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		s.ForEach(func(m string) bool { return true })
	}()
	if panicked {
		t.Error("ForEach on nil set panicked")
	}
}