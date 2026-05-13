package lsm

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"mini-kv/internal/storage/lsm/record"
)

// 目前仅使用内存 map 实现
type Engine struct {
    mu     sync.RWMutex
    opts   Options
    closed bool
    data   map[string][]byte
	lastSeq atomic.Uint64
}

// 打开指定目录下的引擎
func Open(dir string, opts ...Option) (*Engine, error) {
    return OpenWithContext(context.TODO(), dir, opts...)
}

// 在打开引擎时接受一个 context，用于控制超时或取消
// 当前实现忽略目录参数，仅初始化内存存储
func OpenWithContext(ctx context.Context, _ string, opts ...Option) (*Engine, error) {
    if ctx == nil {
        ctx = context.TODO()
    }
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    options, err := applyOptions(opts)
    if err != nil {
        return nil, err
    }
    return &Engine{
        opts: options,
        data: make(map[string][]byte),
    }, nil
}

// 根据给定的键查询值。
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
    e.mu.RLock()
    defer e.mu.RUnlock()

    if e.closed {
        return nil, false, ErrClosed
    }
    value, ok := e.data[string(key)]
    if !ok {
        return nil, false, nil
    }
    return cloneBytes(value), true, nil
}

// 提交一个写批次，将所有操作原子地应用到内存存储
func (e *Engine) Write(batch *WriteBatch, _ WriteOptions) error {
    if err := validateWriteBatch(batch); err != nil {
        return err
    }

    e.mu.Lock()
    defer e.mu.Unlock()

    if e.closed {
        return ErrClosed
    }
    if batch == nil {
        return nil
    }
    for _, op := range batch.Ops {
        switch op.Type {
        case OpPut:
            e.data[string(op.Key)] = cloneBytes(op.Value)
        case OpDelete:
            delete(e.data, string(op.Key))
        }
    }
    return nil
}

// 创建一个基于引擎当前数据的迭代器，并仅包含符合范围选项的条目
func (e *Engine) NewIterator(options IterOptions) *Iterator {
    e.mu.RLock()
    defer e.mu.RUnlock()

    if e.closed {
        return newErrorIterator(ErrClosed)
    }
	entries, err := e.collectEntries(makeKeyBounds(options))
	if err != nil {
		return newErrorIterator(err)
	}
    return newSliceIterator(entries, nil)
}

// 创建引擎的一份一致性快照
func (e *Engine) Snapshot() (*Snapshot, error) {
    e.mu.RLock()
    defer e.mu.RUnlock()

    if e.closed {
        return nil, ErrClosed
    }
    return newSnapshot(snapshotEntries(e.data, IterOptions{})), nil
}

// 触发内存数据持久化（目前为空）
func (e *Engine) Flush() error {
    e.mu.RLock()
    defer e.mu.RUnlock()

    if e.closed {
        return ErrClosed
    }
    return nil
}

// 关闭引擎，释放资源
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

// 校验 WriteBatch
// 所有操作的键不能为空
// 操作类型必须为已知的 OpPut 或 OpDelete
func validateWriteBatch(batch *WriteBatch) error {
    if batch == nil {
        return nil
    }
    for i, op := range batch.Ops {
        if len(op.Key) == 0 {
            return fmt.Errorf("%w: empty key at op %d", ErrInvalidKey, i)
        }
        switch op.Type {
        case OpPut, OpDelete:
        default:
            return fmt.Errorf("%w: unknown op type %d at op %d", ErrInvalidBatch, op.Type, i)
        }
    }
    return nil
}

// 从 map 中筛选出符合边界条件的键值对，返回有序的 entry 切片
func snapshotEntries(data map[string][]byte, options IterOptions) []entry {
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
            Key:   []byte(key),
            Value: cloneBytes(data[key]),
        })
    }
    return entries
}

// 判断键是否在 [LowerBound, UpperBound) 区间内
func inBounds(key []byte, options IterOptions) bool {
    if len(options.LowerBound) > 0 && bytes.Compare(key, options.LowerBound) < 0 {
        return false
    }
    if len(options.UpperBound) > 0 && bytes.Compare(key, options.UpperBound) >= 0 {
        return false
    }
    return true
}

func makeKeyBounds(options IterOptions) keyBounds {
	return keyBounds{
		Lower: record.CloneBytes(options.LowerBound),
		Upper: record.CloneBytes(options.UpperBound),
	}
}

func (e *Engine) collectEntries(bounds keyBounds) ([]entry, error) {
    var entries []entry
	return entries, nil
}