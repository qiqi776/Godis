package sortedset

type SortedSet struct {
	dict map[string]*node
	skiplist *skiplist
}

func Make() *SortedSet {
	return &SortedSet{
		dict: make(map[string]*node),
		skiplist: makeskiplist(),
	}
}

func (ss *SortedSet) Add(member string, score float64) bool {
	node, ok := ss.dict[member]
	if ok {
		// 存在则更新：如果分数不同，先删后加
		if score != node.Score {
			ss.skiplist.isremove(member, node.Score)
			newNode := ss.skiplist.insert(member, score)
			ss.dict[member] = newNode
		}
		return false
	}
	// 不存在则新增
	newNode := ss.skiplist.insert(member, score)
	ss.dict[member] = newNode
	return true
}

func (ss *SortedSet) Get(member string) (float64, bool) {
	node, ok := ss.dict[member]
	if !ok {
		return 0, false
	}
	return node.Score, true
}

func (ss *SortedSet) Remove(member string) bool {
	node, ok := ss.dict[member]
	if ok {
		ss.skiplist.isremove(member, node.Score)
		delete(ss.dict, member)
		return true
	}
	return false
}