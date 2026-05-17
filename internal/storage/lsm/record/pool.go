package record

import "sync"

// BytePool 复用临时字节缓冲区，减少编码过程中的内存分配压力
// 调用方在 Put 缓冲区后不得再使用该切片，若需保留数据请显式拷贝
type BytePool struct {
	pool sync.Pool
}

// NewBytePool 创建一个新的字节池，预分配容量的初始大小为 size
func NewBytePool(size int) *BytePool {
	if size < 0 {
		size = 0
	}
	return &BytePool{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, 0, size)
			},
		},
	}
}

// Get 从池中取出一个缓冲区（长度重置为 0，容量保持不变）
func (p *BytePool) Get() []byte {
	if p == nil {
		return nil
	}
	buf, ok := p.pool.Get().([]byte)
	if !ok {
		return nil
	}
	return buf[:0]
}

// Put 清空缓冲区内容并将其放回池中
func (p *BytePool) Put(buf []byte) {
	if p == nil || buf == nil {
		return
	}
	clear(buf)
	p.pool.Put(buf[:0])
}