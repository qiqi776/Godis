package mem

import (
	"bytes"
	"sort"
	"sync"

	"mini-kv/internal/storage"
)

// 键值对条目
type entry struct {
	key   []byte
	value []byte
}

// storage.Engine 接口的内存实现
type Engine struct {
	mu     sync.RWMutex
	closed bool
	data   map[string][]byte
}

// storage.Snapshot 接口的内存实现
type Snapshot struct {
	mu     sync.RWMutex
	closed bool
	data   map[string][]byte
}

// storage.Iterator 接口的内存实现。
// 基于预排序的 entry 切片，支持顺序扫描和随机定位
type Iterator struct {
	entries []entry // 按 key 升序排列的条目切片
	index   int     // 当前指向的条目索引，-1 表示无效
	err     error
	closed  bool
}

var _ storage.Engine = (*Engine)(nil)
var _ storage.Snapshot = (*Snapshot)(nil)
var _ storage.Iterator = (*Iterator)(nil)

// 创建并返回一个新的内存引擎实例
func New() *Engine {
	return &Engine{
		data: make(map[string][]byte),
	}
}

// 从引擎中读取 key 对应的 value
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, false, storage.ErrClosed
	}
	value, ok := e.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(value), true, nil
}

// 将一批变更原子性地应用到引擎
func (e *Engine) Write(batch *storage.WriteBatch, _ storage.WriteOptions) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return storage.ErrClosed
	}
	if batch == nil {
		return nil
	}
	for _, op := range batch.Ops {
		switch op.Type {
		case storage.OpPut:
			e.data[string(op.Key)] = cloneBytes(op.Value)
		case storage.OpDelete:
			delete(e.data, string(op.Key))
		}
	}
	return nil
}

// 创建一个迭代器，限制在 [LowerBound, UpperBound) 区间内
func (e *Engine) NewIterator(options storage.IterOptions) storage.Iterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return &Iterator{err: storage.ErrClosed, index: -1}
	}
	return newIterator(snapshotEntries(e.data, options))
}

// 创建引擎当前状态的完整独立快照
func (e *Engine) Snapshot() (storage.Snapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, storage.ErrClosed
	}

	data := make(map[string][]byte, len(e.data))
	for key, value := range e.data {
		data[key] = cloneBytes(value)
	}
	return &Snapshot{data: data}, nil
}

// 确保所有数据已持久化。内存实现无需刷盘
func (e *Engine) Flush() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return storage.ErrClosed
	}
	return nil
}

// 关闭引擎并释放所有持有的内存资源
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true
	e.data = nil
	return nil
}

// 从快照中读取 key 对应的 value
func (s *Snapshot) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, false, storage.ErrClosed
	}
	value, ok := s.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(value), true, nil
}

// 创建快照的迭代器
func (s *Snapshot) NewIterator(options storage.IterOptions) storage.Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return &Iterator{err: storage.ErrClosed, index: -1}
	}
	return newIterator(snapshotEntries(s.data, options))
}

// 关闭快照并释放其持有的数据
func (s *Snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	s.data = nil
	return nil
}

// 从预排序的条目切片创建迭代器，初始索引为 -1
func newIterator(entries []entry) *Iterator {
	return &Iterator{
		entries: entries,
		index:   -1,
	}
}

// 移动到迭代器的第一个条目，成功返回 true
func (it *Iterator) First() bool {
	if it.closed || len(it.entries) == 0 {
		it.index = -1
		return false
	}
	it.index = 0
	return true
}

// 移动到第一个 >= key 的条目，找到返回 true
// 使用二分查找定位
func (it *Iterator) Seek(key []byte) bool {
	if it.closed || len(it.entries) == 0 {
		it.index = -1
		return false
	}

	pos := sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) >= 0
	})
	if pos >= len(it.entries) {
		it.index = -1
		return false
	}
	it.index = pos
	return true
}

// 移动到下一个条目
func (it *Iterator) Next() bool {
	if it.closed {
		it.index = -1
		return false
	}
	if it.index < 0 {
		return it.First()
	}
	it.index++
	if it.index >= len(it.entries) {
		it.index = -1
		return false
	}
	return true
}

// 检查迭代器当前是否指向有效条目
func (it *Iterator) Valid() bool {
	return !it.closed && it.index >= 0 && it.index < len(it.entries)
}

// 返回当前条目的 key 的独立副本
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return cloneBytes(it.entries[it.index].key)
}

// 返回当前条目的 value 的独立副本
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return cloneBytes(it.entries[it.index].value)
}

// 返回迭代过程中发生的错误
func (it *Iterator) Error() error {
	return it.err
}

// 关闭迭代器并释放资源
func (it *Iterator) Close() error {
	it.closed = true
	it.entries = nil
	it.index = -1
	return nil
}

// 从 map 中筛选符合边界条件的 key，并按升序返回条目切片
func snapshotEntries(data map[string][]byte, options storage.IterOptions) []entry {
	keys := make([]string, 0, len(data))
	for key := range data {
		if inBounds([]byte(key), options) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	entries := make([]entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, entry{
			key:   []byte(key),
			value: cloneBytes(data[key]),
		})
	}
	return entries
}

// 检查 key 是否在 [LowerBound, UpperBound) 区间内
func inBounds(key []byte, options storage.IterOptions) bool {
	if len(options.LowerBound) > 0 && bytes.Compare(key, options.LowerBound) < 0 {
		return false
	}
	if len(options.UpperBound) > 0 && bytes.Compare(key, options.UpperBound) >= 0 {
		return false
	}
	return true
}

// cloneBytes 返回字节切片的独立副本
func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}