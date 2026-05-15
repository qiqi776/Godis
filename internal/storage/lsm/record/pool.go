package record

import "sync"

// BytePool reuses temporary byte buffers for encoders. Callers must not retain
// buffers after Put; returned public data still needs an explicit copy.
type BytePool struct {
	pool sync.Pool
}

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

func (p *BytePool) Put(buf []byte) {
	if p == nil || buf == nil {
		return
	}
	clear(buf)
	p.pool.Put(buf[:0])
}
