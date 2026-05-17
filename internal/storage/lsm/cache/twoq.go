package cache

import "container/list"

// queueKind 标记缓存条目当前所在的逻辑队列
type queueKind uint8

const (
	queueRecent   queueKind = iota + 1 // 位于 recent FIFO 队列
	queueFrequent                      // 位于 frequent LRU 队列
)

// cacheEntry 是缓存中存储的实际元素，包含键、值以及所属队列标记
type cacheEntry[K comparable, V any] struct {
	key   K
	value V
	queue queueKind
}

// TwoQueue 是一个 2Q 缓存实现：
//   - 新访问的条目先进入 recent FIFO 队列；
//   - 在 recent 中被多次访问或从 ghost 恢复的条目会提升到 frequent LRU 队列；
//   - 淘汰时，recent 队列尾部条目被移出，其键会进入 ghost 队列；
//   - ghost 队列是一个只记键不记值的“幽灵”列表，用来探测一个键是否值得再次缓存
//
// 该结构本身不加锁，调用方（如 Engine / table cache）应使用外部锁保护
type TwoQueue[K comparable, V any] struct {
	capacity  int // 缓存最大容量（frequent + recent 的总条目数）
	recentCap int // recent 队列的最大容量（默认为 capacity/4，至少为1）
	ghostCap  int // ghost 队列最大容量（与 capacity 相同）

	recent   *list.List // FIFO 队列，存放初次进入或未被频繁访问的条目
	frequent *list.List // LRU 队列，存放被重复访问的条目
	ghost    *list.List // 只存键的队列，记录最近被淘汰的键（幽灵条目）

	items  map[K]*list.Element // 所有在 recent 或 frequent 中的条目索引
	ghosts map[K]*list.Element // ghost 队列的键索引
}

// NewTwoQueue 创建一个新的 2Q 缓存
// capacity 指总容量（recent + frequent），内部会根据启发式规则分配两个队列的大小
func NewTwoQueue[K comparable, V any](capacity int) *TwoQueue[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	// recent 队列大小设为总容量的 1/4，避免一次性扫描污染 frequent 队列
	recentCap := capacity / 4
	if recentCap < 1 {
		recentCap = 1
	}
	return &TwoQueue[K, V]{
		capacity:  capacity,
		recentCap: recentCap,
		ghostCap:  capacity,
		recent:    list.New(),
		frequent:  list.New(),
		ghost:     list.New(),
		items:     make(map[K]*list.Element, capacity),
		ghosts:    make(map[K]*list.Element, capacity),
	}
}

// Get 从缓存中获取键对应的值如果命中，条目会被提升（promote）
// 返回值为 (value, found)
func (c *TwoQueue[K, V]) Get(key K) (V, bool) {
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*cacheEntry[K, V])
		c.promote(elem, entry) // 命中时提升
		return entry.value, true
	}
	var zero V
	return zero, false
}

// Add 添加或更新一个键值对如果键已存在，更新其值并提升队列；
// 如果是新键，则放入合适的队列（可能根据 ghost 信息决定放入 frequent 还是 recent）
func (c *TwoQueue[K, V]) Add(key K, value V) {
	if elem, ok := c.items[key]; ok {
		// 已存在：更新值并提升
		entry := elem.Value.(*cacheEntry[K, V])
		entry.value = value
		c.promote(elem, entry)
		return
	}

	entry := &cacheEntry[K, V]{
		key:   key,
		value: value,
	}
	// 如果键在 ghost 队列中，说明它曾被淘汰但又被重新访问，应直接进入 frequent 队列
	if ghostElem, ok := c.ghosts[key]; ok {
		c.ghost.Remove(ghostElem)
		delete(c.ghosts, key)
		entry.queue = queueFrequent
		c.items[key] = c.frequent.PushFront(entry)
	} else {
		// 否则放入 recent 队列
		entry.queue = queueRecent
		c.items[key] = c.recent.PushFront(entry)
	}
	// 加入后可能需要淘汰一些条目以维持容量
	c.evictIfNeeded()
}

// Remove 删除键对应的条目（包括 resident 和 ghost）返回是否成功删除
func (c *TwoQueue[K, V]) Remove(key K) bool {
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*cacheEntry[K, V])
		c.removeResident(elem, entry)
		return true
	}
	if elem, ok := c.ghosts[key]; ok {
		c.ghost.Remove(elem)
		delete(c.ghosts, key)
		return true
	}
	return false
}

// Len 返回当前缓存中 resident 条目的总数（recent + frequent）
func (c *TwoQueue[K, V]) Len() int {
	return len(c.items)
}

// GhostLen 返回 ghost 队列的条目数
func (c *TwoQueue[K, V]) GhostLen() int {
	return len(c.ghosts)
}

// Clear 清空所有缓存条目和 ghost 队列
func (c *TwoQueue[K, V]) Clear() {
	c.recent.Init()
	c.frequent.Init()
	c.ghost.Init()
	clear(c.items)
	clear(c.ghosts)
}

// promote 在命中时提升条目：recent 中的条目移入 frequent 头部；
// frequent 中的条目移到队首（LRU 策略）
func (c *TwoQueue[K, V]) promote(elem *list.Element, entry *cacheEntry[K, V]) {
	switch entry.queue {
	case queueRecent:
		// recent 命中 → 移入 frequent 并标记为 frequent
		c.recent.Remove(elem)
		entry.queue = queueFrequent
		c.items[entry.key] = c.frequent.PushFront(entry)
	case queueFrequent:
		// frequent 命中 → 移到队首（LRU）
		c.frequent.MoveToFront(elem)
	}
}

// evictIfNeeded 当 resident 条目数超过容量时进行淘汰
// 优先淘汰 recent 队列的条目（除非 recent 未超出其配额且 frequent 不为空）
func (c *TwoQueue[K, V]) evictIfNeeded() {
	for len(c.items) > c.capacity {
		// 如果 recent 超过其容量上限，或者 frequent 为空，则淘汰 recent
		if c.recent.Len() > c.recentCap || c.frequent.Len() == 0 {
			c.evictRecent()
			continue
		}
		// 否则淘汰 frequent 的尾部
		c.evictFrequent()
	}
}

// evictRecent 从 recent 队列尾部淘汰一个条目，并将其键加入 ghost 队列
func (c *TwoQueue[K, V]) evictRecent() {
	elem := c.recent.Back()
	if elem == nil {
		c.evictFrequent()
		return
	}
	entry := elem.Value.(*cacheEntry[K, V])
	c.removeResident(elem, entry)
	c.addGhost(entry.key)
}

// evictFrequent 从 frequent 队列尾部淘汰一个条目（不加入 ghost）
func (c *TwoQueue[K, V]) evictFrequent() {
	elem := c.frequent.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(*cacheEntry[K, V])
	c.removeResident(elem, entry)
}

// removeResident 从队列和索引中移除一个 resident 条目
func (c *TwoQueue[K, V]) removeResident(elem *list.Element, entry *cacheEntry[K, V]) {
	switch entry.queue {
	case queueRecent:
		c.recent.Remove(elem)
	case queueFrequent:
		c.frequent.Remove(elem)
	}
	delete(c.items, entry.key)
}

// addGhost 将一个键加入 ghost 队列，并维持 ghost 容量
func (c *TwoQueue[K, V]) addGhost(key K) {
	if elem, ok := c.ghosts[key]; ok {
		c.ghost.MoveToFront(elem)
		return
	}
	c.ghosts[key] = c.ghost.PushFront(key)
	for len(c.ghosts) > c.ghostCap {
		elem := c.ghost.Back()
		if elem == nil {
			return
		}
		delete(c.ghosts, elem.Value.(K))
		c.ghost.Remove(elem)
	}
}