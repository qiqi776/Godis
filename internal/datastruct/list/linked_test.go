package list

import (
	"reflect"
	"testing"
)

func assertPanic(t *testing.T, f func(), desc string) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: expected panic, but did not panic", desc)
		}
	}()
	f()
}

func TestMakeAndAdd(t *testing.T) {
	l1 := Make(1, 2, 3)
	if l1.Len() != 3 {
		t.Errorf("Make(1, 2, 3) len expected 3, got %d", l1.Len())
	}
	if l1.Get(0) != 1 || l1.Get(2) != 3 {
		t.Error("Make order incorrect")
	}
	l2 := Make()
	l2.Add("a")
	l2.Add("b")
	if l2.Len() != 2 {
		t.Error("Add len incorrect")
	}
	if l2.Get(1) != "b" {
		t.Error("Add value incorrect")
	}
}

func TestGetSet(t *testing.T) {
	l := Make(10, 20, 30)
	if val := l.Get(1); val != 20 {
		t.Errorf("Get(1) expected 20, got %v", val)
	}
	l.Set(1, 999)
	if l.Len() != 3 {
		t.Error("Set should not change length")
	}
	if val := l.Get(1); val != 999 {
		t.Errorf("After Set(1, 999), Get(1) expected 999, got %v", val)
	}
	if l.Get(0) != 10 || l.Get(2) != 30 {
		t.Error("Set affected other elements")
	}
}

func TestInsert(t *testing.T) {
	l := Make(2, 3)
	l.Insert(0, 1)
	if l.Get(0) != 1 || l.Len() != 3 {
		t.Error("Insert at head failed")
	}
	l.Insert(3, 4)
	if l.Get(3) != 4 || l.Len() != 4 {
		t.Error("Insert at tail failed")
	}
	l.Insert(2, 2.5)
	if l.Get(2) != 2.5 || l.Get(3) != 3 {
		t.Error("Insert middle failed")
	}
}

func TestRemove(t *testing.T) {
	l := Make(1, 2, 3)
	val := l.Remove(1)
	if val != 2 || l.Len() != 2 {
		t.Error("Remove middle failed")
	}
	if l.Get(0) != 1 || l.Get(1) != 3 {
		t.Error("Remove middle broken links")
	}
	val = l.Remove(0)
	if val != 1 || l.Len() != 1 {
		t.Error("Remove head failed")
	}
	if l.Get(0) != 3 {
		t.Error("Remove head broken links")
	}
	val = l.Remove(0)
	if val != 3 || l.Len() != 0 {
		t.Error("Remove last failed")
	}
	l = Make(1, 2)
	val = l.RemoveLast()
	if val != 2 || l.Len() != 1 {
		t.Error("RemoveLast failed")
	}
	if l.Get(0) != 1 {
		t.Error("RemoveLast broken links")
	}
	l = Make()
	if l.RemoveLast() != nil {
		t.Error("RemoveLast on empty should return nil")
	}
}

func TestRemoveByVal(t *testing.T) {
	isVal := func(target interface{}) func(interface{}) bool {
		return func(val interface{}) bool {
			return val == target
		}
	}
	l := Make(1, 2, 2, 3, 2)
	count := l.RemoveAllByVal(isVal(2))
	if count != 3 || l.Len() != 2 {
		t.Errorf("RemoveAllByVal expected 3, got %d", count)
	}
	if !reflect.DeepEqual(l.Range(0, 2), []interface{}{1, 3}) {
		t.Error("RemoveAllByVal result incorrect")
	}
	l = Make(2, 1, 2, 2, 3)
	count = l.RemoveByVal(isVal(2), 2)
	if count != 2 || l.Len() != 3 {
		t.Error("RemoveByVal limit failed")
	}
	if !reflect.DeepEqual(l.Range(0, 3), []interface{}{1, 2, 3}) {
		t.Error("RemoveByVal result incorrect")
	}
	l = Make(1, 2, 2, 3, 2)
	count = l.ReverseRemoveByVal(isVal(2), 1)
	if count != 1 || l.Get(l.Len()-1) != 3 {
		t.Error("ReverseRemoveByVal failed")
	}
}

func TestRangeAndForEach(t *testing.T) {
	l := Make(0, 1, 2, 3, 4)
	slice := l.Range(0, 5)
	if len(slice) != 5 || slice[4] != 4 {
		t.Error("Range full failed")
	}
	slice = l.Range(1, 4)
	if !reflect.DeepEqual(slice, []interface{}{1, 2, 3}) {
		t.Error("Range part failed")
	}
	slice = l.Range(2, 3)
	if len(slice) != 1 || slice[0] != 2 {
		t.Error("Range single failed")
	}
	found := false
	l.ForEach(func(i int, v interface{}) bool {
		if v == 3 {
			found = true
			return false
		}
		return true
	})
	if !found {
		t.Error("ForEach did not stop correctly")
	}
	if !l.Contains(func(a interface{}) bool { return a == 4 }) {
		t.Error("Contains expected true")
	}
}

func TestPanics(t *testing.T) {
	l := Make(1, 2, 3)
	assertPanic(t, func() { l.Get(-1) }, "Get(-1)")
	assertPanic(t, func() { l.Get(3) }, "Get(size)")
	assertPanic(t, func() { l.Set(3, 0) }, "Set(size)")
	assertPanic(t, func() { l.Remove(3) }, "Remove(size)")
	assertPanic(t, func() { l.Insert(4, 0) }, "Insert(size+1)")
	assertPanic(t, func() { l.Range(0, 4) }, "Range end > size")
	assertPanic(t, func() { l.Range(2, 1) }, "Range start > end")
	var nilList *LinkedList
	assertPanic(t, func() { nilList.Add(1) }, "nilList.Add")
}

func BenchmarkLinkedList_Add(b *testing.B) {
	l := Make()
	for i := 0; i < b.N; i++ {
		l.Add(i)
	}
}

func BenchmarkLinkedList_Get(b *testing.B) {
	l := Make()
	for i := 0; i < 1000; i++ {
		l.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Get(i % 1000)
	}
}

func BenchmarkLinkedList_InsertHead(b *testing.B) {
	l := Make()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Insert(0, i)
	}
}

func BenchmarkLinkedList_Range(b *testing.B) {
	l := Make()
	size := 1000
	for i := 0; i < size; i++ {
		l.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.Range(0, size)
	}
}