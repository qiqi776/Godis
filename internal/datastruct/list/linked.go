package list

type node struct {
	val  interface{}
	prev *node
	next *node
}

type LinkedList struct {
	first *node
	last  *node
	size  int
}

// Add 尾插
func (list *LinkedList) Add(val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	n := &node{val: val}
	if list.size == 0 {
		list.first = n
		list.last = n
	} else {
		n.prev = list.last
		list.last.next = n
		list.last = n
	}
	list.size++
}

// find 根据 index 查找节点,带方向优化：
func (list *LinkedList) find(index int) (n *node) {
	if index < list.size/2 {
		n = list.first
		for i := 0; i < index; i++ {
			n = n.next
		}
	} else {
		n = list.last
		for i := list.size - 1; i > index; i-- {
			n = n.prev
		}
	}
	return n
}

// Get 获取指定 index 的值,边界：index 必须在 [0, size)
func (list *LinkedList) Get(index int) (val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index >= list.size {
		panic("index out of bound")
	}
	return list.find(index).val
}

// Set 修改指定 index 的值
func (list *LinkedList) Set(index int, val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index >= list.size {
		panic("index out of bound")
	}
	n := list.find(index)
	n.val = val
}

// Insert 插入元素
func (list *LinkedList) Insert(index int, val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index > list.size {
		panic("index out of bound")
	}
	if index == list.size {
		list.Add(val)
		return
	}

	p := list.find(index)
	n := &node{
		val:  val,
		prev: p.prev,
		next: p,
	}

	if p.prev != nil {
		p.prev.next = n
	} else {
		list.first = n
	}
	p.prev = n
	list.size++
}

// removeNode 删除指定节点
func (list *LinkedList) removeNode(n *node) {
	if n.prev == nil {
		list.first = n.next
	} else {
		n.prev.next = n.next
	}
	if n.next == nil {
		list.last = n.prev
	} else {
		n.next.prev = n.prev
	}
	// 断开连接，帮助 GC
	n.prev = nil
	n.next = nil
	list.size--
}

// Remove 删除 index 处的节点
func (list *LinkedList) Remove(index int) (val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index >= list.size {
		panic("index out of bound")
	}
	n := list.find(index)
	list.removeNode(n)
	return n.val
}

// RemoveLast 删除尾节点
func (list *LinkedList) RemoveLast() (val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if list.last == nil {
		return nil
	}
	n := list.last
	list.removeNode(n)
	return n.val
}

// RemoveAllByVal 删除所有满足 exp 的节点
func (list *LinkedList) RemoveAllByVal(exp Expected) int {
	if list == nil {
		panic("list is nil")
	}
	n := list.first
	removed := 0
	var nextNode *node
	for n != nil {
		nextNode = n.next
		if exp(n.val) {
			list.removeNode(n)
			removed++
		}
		n = nextNode
	}
	return removed
}

// RemoveByVal 删除前 count 个满足条件的节点
func (list *LinkedList) RemoveByVal(exp Expected, count int) int {
	if list == nil {
		panic("list is nil")
	}
	n := list.first
	removed := 0
	var nextNode *node
	for n != nil {
		nextNode = n.next
		if exp(n.val) {
			list.removeNode(n)
			removed++
		}
		if removed == count {
			break
		}
		n = nextNode
	}
	return removed
}

// ReverseRemoveByVal 从尾部开始删除
func (list *LinkedList) ReverseRemoveByVal(exp Expected, count int) int {
	if list == nil {
		panic("list is nil")
	}
	n := list.last
	removed := 0
	var prevNode *node
	for n != nil {
		prevNode = n.prev
		if exp(n.val) {
			list.removeNode(n)
			removed++
		}
		if removed == count {
			break
		}
		n = prevNode
	}
	return removed
}

// Len 返回链表长度
func (list *LinkedList) Len() int {
	if list == nil {
		panic("list is nil")
	}
	return list.size
}

// ForEach 遍历链表, consumer 返回 false 时提前停止
func (list *LinkedList) ForEach(consumer Consumer) {
	if list == nil {
		panic("list is nil")
	}
	n := list.first
	i := 0
	for n != nil {
		goNext := consumer(i, n.val)
		if !goNext {
			break
		}
		i++
		n = n.next
	}
}

// Contains 是否存在某个满足条件的值
func (list *LinkedList) Contains(exp Expected) bool {
	contains := false
	list.ForEach(func(i int, actual interface{}) bool {
		if exp(actual) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

// Range 返回区间 [start, end) 的切片
func (list *LinkedList) Range(start int, end int) []interface{} {
	if list == nil {
		panic("list is nil")
	}
	if start < 0 || start >= list.size || end < 0 || end > list.size {
		panic("`start` or `end` out of range")
	}

	sliceSize := end - start
	slice := make([]interface{}, sliceSize)

	n := list.first
	i := 0
	sliceIndex := 0

	for n != nil {
		if i >= start && i < end {
			slice[sliceIndex] = n.val
			sliceIndex++
		} else if i >= end {
			break
		}
		i++
		n = n.next
	}
	return slice
}

func Make(vals ...interface{}) *LinkedList {
	list := LinkedList{}
	for _, v := range vals {
		list.Add(v)
	}
	return &list
}
