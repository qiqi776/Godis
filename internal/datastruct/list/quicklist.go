package list

import (
    "container/list"
)

const pageSize = 1024

type QuickList struct {
    data *list.List
    size int
}

type iterator struct {
    node   *list.Element
    offset int
    ql     *QuickList
}

func MakeQuickList() *QuickList {
    quicklist := &QuickList{
        data: list.New(),
    }
    return quicklist
}

// Add 尾插
func (ql *QuickList) Add(val interface{}) {
    ql.size++
    if ql.data.Len() == 0 {
        page := make([]interface{}, 0, pageSize)
        page = append(page, val)
        ql.data.PushBack(page)
        return
    }
    backNode := ql.data.Back()
    backPage := backNode.Value.([]interface{})
    if len(backPage) == cap(backPage) {
        page := make([]interface{}, 0, pageSize)
        page = append(page, val)
        ql.data.PushBack(page)
        return
    }
    backPage = append(backPage, val)
    backNode.Value = backPage
}

// find 根据 index 查找节点
func (ql *QuickList) find(index int) *iterator {
    if ql == nil {
        panic("list is nil")
    }
    if index < 0 || index >= ql.size {
        panic("index out of bound")
    }
    var n *list.Element
    var page []interface{}
    var pageBeg int
    
    if index < ql.size/2 {
        n = ql.data.Front()
        pageBeg = 0
        for {
            page = n.Value.([]interface{})
            if pageBeg+len(page) > index {
                break
            }
            pageBeg += len(page)
            n = n.Next()
        }
    } else {
        n = ql.data.Back()
        pageBeg = ql.size
        for {
            page = n.Value.([]interface{})
            pageBeg -= len(page)
            if pageBeg <= index {
                break
            }
            n = n.Prev()
        }
    }
    pageOffset := index - pageBeg
    return &iterator{
        node:   n,
        offset: pageOffset,
        ql:     ql,
    }
}

func (iter *iterator) get() interface{} {
    return iter.page()[iter.offset]
}

func (iter *iterator) page() []interface{} {
    return iter.node.Value.([]interface{})
}

// next 尝试将迭代器移动到下一个元素
func (iter *iterator) next() bool {
    page := iter.page()
    if iter.offset < len(page)-1 {
        iter.offset++
        return true
    }
    if iter.node == iter.ql.data.Back() {
        iter.offset = len(page)
        return false
    }
    iter.offset = 0
    iter.node = iter.node.Next()
    return true
}

// prev 尝试将迭代器移动到上一个元素
func (iter *iterator) prev() bool {
    if iter.offset > 0 {
        iter.offset--
        return true
    }
    if iter.node == iter.ql.data.Front() {
        iter.offset = -1
        return false
    }
    iter.node = iter.node.Prev()
    prevPage := iter.node.Value.([]interface{})
    iter.offset = len(prevPage) - 1
    return true
}

func (iter *iterator) atEnd() bool {
    if iter.ql.data.Len() == 0 {
        return true
    }
    if iter.node != iter.ql.data.Back() {
        return false
    }
    page := iter.page()
    return iter.offset == len(page)
}

func (iter *iterator) atBegin() bool {
    if iter.ql.data.Len() == 0 {
        return true
    }
    if iter.node != iter.ql.data.Front() {
        return false
    }
    return iter.offset == -1
}

func (iter *iterator) set(val interface{}) {
    page := iter.page()
    page[iter.offset] = val
}

func (ql *QuickList) Len() int {
    return ql.size
}

// Get 获取指定索引位置的元素
func (ql *QuickList) Get(index int) (val interface{}) {
    iter := ql.find(index)
    return iter.get()
}

// Set 修改指定 index 的值
func (ql *QuickList) Set(index int, val interface{}) {
    iter := ql.find(index)
    iter.set(val)
}

// Insert 在指定索引处插入新元素
func (ql *QuickList) Insert(index int, val interface{}) {
    if index == ql.size {
        ql.Add(val)
        return
    }
    iter := ql.find(index)
    page := iter.node.Value.([]interface{})
    // 页未满，直接插入
    if len(page) < pageSize {
        page = append(page[:iter.offset+1], page[iter.offset:]...)
        page[iter.offset] = val
        iter.node.Value = page
        ql.size++
        return
    }
    // 页已满，需要分裂
    half := pageSize / 2
    nextPage := make([]interface{}, 0, pageSize)
    nextPage = append(nextPage, page[half:]...)
    page = page[:half]
    iter.node.Value = page
    // 判断插入位置是在前半页还是后半页
    if iter.offset < half {
        // 插入到旧页
        page = append(page[:iter.offset+1], page[iter.offset:]...)
        page[iter.offset] = val
        iter.node.Value = page
    } else {
        // 插入到新页
        newOffset := iter.offset - half
        nextPage = append(nextPage[:newOffset+1], nextPage[newOffset:]...)
        nextPage[newOffset] = val
    }
    ql.data.InsertAfter(nextPage, iter.node)
    ql.size++
}

// remove 删除指定节点
func (iter *iterator) remove() interface{} {
    page := iter.page()
    val := page[iter.offset]
    page = append(page[:iter.offset], page[iter.offset+1:]...)
    if len(page) > 0 {
        iter.node.Value = page
        if iter.offset == len(page) {
            if iter.node != iter.ql.data.Back() {
                iter.node = iter.node.Next()
                iter.offset = 0
            }
        }
    } else {
        currentNode := iter.node
        if currentNode == iter.ql.data.Back() {
            if prevNode := currentNode.Prev(); prevNode != nil {
                iter.node = prevNode
                iter.offset = len(prevNode.Value.([]interface{}))
            } else {
                iter.node = nil
                iter.offset = 0
            }
        } else {
            iter.node = currentNode.Next()
            iter.offset = 0
        }
        iter.ql.data.Remove(currentNode)
    }
    iter.ql.size--
    return val
}

func (ql *QuickList) Remove(index int) interface{} {
    iter := ql.find(index)
    return iter.remove()
}

func (ql *QuickList) RemoveLast() interface{} {
    if ql.Len() == 0 {
        return nil
    }
    ql.size--
    lastNode := ql.data.Back()
    lastPage := lastNode.Value.([]interface{})
    val := lastPage[len(lastPage)-1]

    if len(lastPage) == 1 {
        ql.data.Remove(lastNode)
        return val
    }
    
    lastPage = lastPage[:len(lastPage)-1]
    lastNode.Value = lastPage
    return val
}

func (ql *QuickList) RemoveAllByVal(exp Expected) int {
    if ql.size == 0 {
		return 0
	}
    iter := ql.find(0)
    removed := 0
    for !iter.atEnd() {
        if exp(iter.get()) {
            iter.remove()
            removed++
        } else {
            iter.next()
        }
    }
    return removed
}

func (ql *QuickList) RemoveByVal(exp Expected, count int) int {
    if ql.size == 0 {
        return 0
    }
    iter := ql.find(0)
    removed := 0
    for !iter.atEnd() {
        if exp(iter.get()) {
            iter.remove()
            removed++
            if removed == count {
                break
            }
        } else {
            iter.next()
        }
    }
    return removed
}

func (ql *QuickList) ReverseRemoveByVal(exp Expected, count int) int {
    if ql.size == 0 {
        return 0
    }
    iter := ql.find(ql.size - 1)
    removed := 0
    for !iter.atBegin() {
        val := iter.get()
        needRemove := exp(val)
        if needRemove {
            iter.remove() 
            removed++
            if removed == count {
                break
            }
            iter.prev() 
        } else {
            iter.prev()
        }
    }
    return removed
}

func (ql *QuickList) ForEach(consumer Consumer) {
    if ql == nil {
        panic("ql is nil")
    }
    if ql.Len() == 0 {
        return
    }
    iter := ql.find(0)
    i := 0
    for {
        goNext := consumer(i, iter.get())
        if !goNext {
            break
        }
        i++
        if !iter.next() {
            break
        }
    }
}

func (ql *QuickList) Contains(exp Expected) bool {
    contains := false
    ql.ForEach(func(i int, actual interface{}) bool {
        if exp(actual) {
            contains = true
            return false
        }
        return true
    })
    return contains
}

// Range 返回区间 [start, end) 的切片
func (ql *QuickList) Range(start int, end int) []interface{} {
    if start < 0 || start > ql.size || end < 0 || end > ql.size {
        panic("`start` or `end` out of range")
    }
    if start > end {
         panic("start > end")
    }
    
    sliceSize := end - start
    slice := make([]interface{}, 0, sliceSize)
    if sliceSize == 0 {
        return slice
    }
    iter := ql.find(start)
    i := 0
    for i < sliceSize {
        slice = append(slice, iter.get())
        iter.next() 
        i++
    }
    return slice
}