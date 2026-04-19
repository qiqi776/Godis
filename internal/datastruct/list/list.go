package list

type node struct {
	prev  *node
	next  *node
	value []byte
}

type List struct {
	head *node
	tail *node
	size int
}

func New() *List {
	return &List{}
}

func (l *List) Len() int {
	return l.size
}

func (l *List) PushFront(value []byte) {
	n := &node{value: copyBytes(value)}

	if l.head == nil {
		l.head = n
		l.tail = n
		l.size = 1
		return
	}
	n.next = l.head
	l.head.prev = n
	l.head = n
	l.size++
}

func (l *List) PushBack(value []byte) {
	n := &node{value: copyBytes(value)}

	if l.tail == nil {
		l.head = n
		l.tail = n
		l.size = 1
		return
	}

	n.prev = l.tail
	l.tail.next = n
	l.tail = n
	l.size++
}

func (l *List) PopFront() ([]byte, bool) {
	if l.head == nil {
		return nil, false
	}
	n := l.head
	if l.head == l.tail {
		l.head = nil
		l.tail = nil
		l.size = 0
		return copyBytes(n.value), true
	}

	l.head = n.next
	l.head.prev = nil
	l.size--
	return copyBytes(n.value), true
}

func (l *List) PopBack() ([]byte, bool) {
	if l.tail == nil {
		return nil, false
	}

	n := l.tail
	if l.head == l.tail {
		l.head = nil
		l.tail = nil
		l.size = 0
		return copyBytes(n.value), true
	}

	l.tail = n.prev
	l.tail.next = nil
	l.size--
	return copyBytes(n.value), true
}

func (l *List) Range(start, stop int) [][]byte {
	if l.size == 0 || start > stop {
		return [][]byte{}
	}

	cur := l.nodeAt(start)
	out := make([][]byte, 0, stop-start+1)

	for i := start; i <= stop && cur != nil; i++ {
		out = append(out, copyBytes(cur.value))
		cur = cur.next
	}
	return out
}

func (l *List) nodeAt(index int) *node {
	if index < 0 || index >= l.size {
		return nil
	}

	if index < l.size/2 {
		cur := l.head
		for i := 0; i < index; i++ {
			cur = cur.next
		}
		return cur
	}

	cur := l.tail
	for i := l.size - 1; i > index; i-- {
		cur = cur.prev
	}
	return cur
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}
