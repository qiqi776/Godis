package storage

import "errors"

var ErrClosed = errors.New("storage: closed")

// 控制批量写入
type WriteOptions struct {
	Sync bool // true 表示需要立即刷新到磁盘
}

// 将迭代器限定在 [LowerBound, UpperBound) 区间
type IterOptions struct {
	LowerBound []byte
	UpperBound []byte
}

// 标识批量写入中的操作类型
type OpType uint8

const (
	OpPut    OpType = iota + 1
	OpDelete
)

// 批量写入中的一次变更
type WriteOp struct {
	Type  OpType
	Key   []byte
	Value []byte
}

// 将多次变更组合为一次原子写入
type WriteBatch struct {
	Ops []WriteOp
}

func (b *WriteBatch) Put(key, value []byte) {
	b.Ops = append(b.Ops, WriteOp{
		Type:  OpPut,
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
	})
}

func (b *WriteBatch) Delete(key []byte) {
	b.Ops = append(b.Ops, WriteOp{
		Type: OpDelete,
		Key:  cloneBytes(key),
	})
}

func (b *WriteBatch) Len() int {
	return len(b.Ops)
}

func (b *WriteBatch) Reset() {
	b.Ops = b.Ops[:0]
}

// LSM 存储层的字节 KV 接口
type Engine interface {
	Get(key []byte) ([]byte, bool, error)
	Write(batch *WriteBatch, options WriteOptions) error
	NewIterator(options IterOptions) Iterator
	Snapshot() (Snapshot, error)
	Flush() error
	Close() error
}

// 快照
type Snapshot interface {
	Get(key []byte) ([]byte, bool, error)
	NewIterator(options IterOptions) Iterator
	Close() error
}

// 按升序遍历键
type Iterator interface {
	First() bool
	Seek(key []byte) bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}