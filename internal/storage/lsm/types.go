package lsm

// 写入后是否立即刷盘
type WriteOptions struct {
    Sync bool
}

// 迭代器范围
type IterOptions struct {
    LowerBound []byte
    UpperBound []byte
}

// 操作类型
type OpType uint8

const (
    OpPut OpType = iota + 1
    OpDelete
)

// 单次操作
type WriteOp struct {
    Type  OpType
    Key   []byte
    Value []byte
}

// 批量操作容器
type WriteBatch struct {
    Ops []WriteOp
}

// 将键值附加到批次末尾
func (b *WriteBatch) Put(key, value []byte) {
    b.Ops = append(b.Ops, WriteOp{
        Type:  OpPut,
        Key:   cloneBytes(key),
        Value: cloneBytes(value),
    })
}

// 将末尾键值批次删除
func (b *WriteBatch) Delete(key []byte) {
    b.Ops = append(b.Ops, WriteOp{
        Type: OpDelete,
        Key:  cloneBytes(key),
    })
}

// 返回当前操作数量
func (b *WriteBatch) Len() int {
    if b == nil {
        return 0
    }
    return len(b.Ops)
}

// 将数组长度截为0，但保留底层容量，方便复用
func (b *WriteBatch) Reset() {
    if b == nil {
        return
    }
    b.Ops = b.Ops[:0]
}

// 辅助函数，负责深拷贝
func cloneBytes(value []byte) []byte {
    if value == nil {
        return nil
    }
    return append([]byte(nil), value...)
}