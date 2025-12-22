package list

import (
	"fmt"
	"testing"
)

func assert(t *testing.T, condition bool, msg string, args ...interface{}) {
	t.Helper()
	if !condition {
		t.Errorf(msg, args...)
	}
}

func TestQuickList_Basic(t *testing.T) {
	ql := MakeQuickList()

	for i := 0; i < 10; i++ {
		ql.Add(i)
	}
	assert(t, ql.Len() == 10, "Len should be 10")

	val := ql.Get(5)
	assert(t, val.(int) == 5, "Get(5) should be 5")

	ql.Set(5, 100)
	assert(t, ql.Get(5).(int) == 100, "Set(5) failed")

	removed := ql.Remove(5)
	assert(t, removed.(int) == 100, "Remove return value error")
	assert(t, ql.Len() == 9, "Len should be 9 after remove")
	assert(t, ql.Get(5).(int) == 6, "Index 5 should be 6 after remove")
}

func TestQuickList_PageSplit(t *testing.T) {
	ql := MakeQuickList()

	total := 2000
	for i := 0; i < total; i++ {
		ql.Add(i)
	}

	assert(t, ql.Len() == total, "Total len mismatch")
	assert(t, ql.data.Len() >= 2, "Should have multiple pages")

	assert(t, ql.Get(0).(int) == 0, "Head check")
	assert(t, ql.Get(1023).(int) == 1023, "Page boundary check 1")
	assert(t, ql.Get(1024).(int) == 1024, "Page boundary check 2")
	assert(t, ql.Get(1999).(int) == 1999, "Tail check")
}

func TestQuickList_Insert_PageSplit(t *testing.T) {
	ql := MakeQuickList()

	for i := 0; i < pageSize; i++ {
		ql.Add(i)
	}
	assert(t, ql.data.Len() == 1, "Should be 1 page initially")

	ql.Insert(10, 9999)

	assert(t, ql.Len() == pageSize+1, "Total size should increase by 1")
	assert(t, ql.data.Len() == 2, "Should split into 2 pages")

	assert(t, ql.Get(9).(int) == 9, "Data before insert incorrect")
	assert(t, ql.Get(10).(int) == 9999, "Inserted data incorrect")
	assert(t, ql.Get(11).(int) == 10, "Data after insert incorrect")
	assert(t, ql.Get(pageSize).(int) == pageSize-1, "Last element verification failed")
}

func TestQuickList_Insert(t *testing.T) {
	ql := MakeQuickList()
	ql.Add(1)
	ql.Add(3)

	ql.Insert(1, 2)

	assert(t, ql.Len() == 3, "Len mismatch")
	assert(t, ql.Get(0).(int) == 1, "idx 0")
	assert(t, ql.Get(1).(int) == 2, "idx 1")
	assert(t, ql.Get(2).(int) == 3, "idx 2")

	ql.Insert(0, 0)
	assert(t, ql.Get(0).(int) == 0, "Insert head failed")

	ql.Insert(4, 4)
	assert(t, ql.Get(4).(int) == 4, "Insert tail failed")
}

func TestQuickList_Range(t *testing.T) {
	ql := MakeQuickList()
	for i := 0; i < 10; i++ {
		ql.Add(i)
	}

	res := ql.Range(2, 5)
	assert(t, len(res) == 3, "Range len mismatch")
	assert(t, res[0].(int) == 2, "Range[0] error")
	assert(t, res[2].(int) == 4, "Range[2] error")

	all := ql.Range(0, 10)
	assert(t, len(all) == 10, "Range all mismatch")
}

func TestQuickList_RemoveByVal(t *testing.T) {
	ql := MakeQuickList()
	ql.Add(1)
	ql.Add(2)
	ql.Add(3)
	ql.Add(2)
	ql.Add(4)
	ql.Add(2)

	count := ql.RemoveByVal(func(a interface{}) bool {
		return a.(int) == 2
	}, 2)

	assert(t, count == 2, "Should remove 2 items")
	assert(t, ql.Len() == 4, "Len should be 4")
	assert(t, ql.Get(1).(int) == 3, "check idx 1")
	assert(t, ql.Get(3).(int) == 2, "check idx 3")
}

func TestQuickList_ReverseRemoveByVal(t *testing.T) {
	ql := MakeQuickList()
	ql.Add(1)
	ql.Add(2)
	ql.Add(3)
	ql.Add(2)
	ql.Add(4)
	ql.Add(2)

	count := ql.ReverseRemoveByVal(func(a interface{}) bool {
		return a.(int) == 2
	}, 2)

	assert(t, count == 2, "Should remove 2 items")
	assert(t, ql.Len() == 4, "Len should be 4")
	assert(t, ql.Get(1).(int) == 2, "The first 2 should remain")
	assert(t, ql.Get(3).(int) == 4, "Last element should be 4")
}

func TestQuickList_ForEach(t *testing.T) {
	ql := MakeQuickList()
	ql.Add(10)
	ql.Add(20)
	ql.Add(30)

	sum := 0
	ql.ForEach(func(i int, v interface{}) bool {
		sum += v.(int)
		return true
	})

	assert(t, sum == 60, "ForEach sum mismatch")

	count := 0
	ql.ForEach(func(i int, v interface{}) bool {
		count++
		return i < 1
	})
	assert(t, count == 2, "ForEach break failed")
}

func TestQuickList_EdgeCases(t *testing.T) {
	ql := MakeQuickList()

	assert(t, ql.Len() == 0, "Empty len")
	assert(t, ql.RemoveLast() == nil, "RemoveLast on empty")
	assert(t, ql.RemoveByVal(func(a interface{}) bool { return true }, 1) == 0, "RemoveByVal on empty")

	res := ql.Range(0, 0)
	assert(t, len(res) == 0, "Range(0,0) should be empty")
}

func TestQuickList_PageManagement(t *testing.T) {
	ql := MakeQuickList()

	for i := 0; i < pageSize; i++ {
		ql.Add(i)
	}
	assert(t, ql.data.Len() == 1, "Should have 1 page")

	ql.Add(1024)
	assert(t, ql.data.Len() == 2, "Should have 2 pages")

	for i := 0; i < 1025; i++ {
		ql.RemoveLast()
	}

	assert(t, ql.Len() == 0, "Size should be 0")
	assert(t, ql.data.Len() == 0, "Pages should be reclaimed")
}

func TestQuickList_BoundaryInsertRemove(t *testing.T) {
	ql := MakeQuickList()

	for i := 0; i < pageSize+1; i++ {
		ql.Add(i)
	}

	val := ql.Remove(pageSize - 1)
	assert(t, val.(int) == pageSize-1, "Remove boundary failed")

	ql.Insert(pageSize-1, 9999)
	assert(t, ql.Get(pageSize-1).(int) == 9999, "Insert boundary failed")
}

func TestQuickList_Panics(t *testing.T) {
	ql := MakeQuickList()
	ql.Add(1)

	assertPanic(t, func() { ql.Get(-1) }, "Get(-1) should panic")
	assertPanic(t, func() { ql.Get(1) }, "Get(size) should panic")
	assertPanic(t, func() { ql.Range(0, 2) }, "Range out of bound should panic")
	assertPanic(t, func() { ql.Range(1, 0) }, "Range start > end should panic")
}

func (ql *QuickList) debugPrint() {
	i := 0
	e := ql.data.Front()
	for e != nil {
		page := e.Value.([]interface{})
		fmt.Printf("Page %d: len=%d cap=%d %v\n", i, len(page), cap(page), page)
		e = e.Next()
		i++
	}
}
