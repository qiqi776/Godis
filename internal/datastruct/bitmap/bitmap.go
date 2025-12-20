package bitmap

import (
	"math/bits"
	
)

type BitMap []byte
type BitCallback func(offset int64, val byte) bool
type SetBitCallback func(offset int64) bool
type ByteCallback func(byteIndex int, byteVal byte) bool

const (
	BlockSize = 64
)

func Make() *BitMap {
	b := BitMap(make([]byte, 0, BlockSize))
	return &b
}

// toByteSize 计算对应的字节数
func toByteSize(bitSize int64) int64 {
	if bitSize%8 == 0 {
		return bitSize / 8
	}
	return bitSize/8 + 1
}

// grow 扩容机制
func (b *BitMap) grow(bitSize int64) {
	byteSize := (bitSize + 7) >> 3
	curLen := int64(len(*b))
	if byteSize <= curLen {
		return
	}
	alignedSize := (byteSize + (BlockSize - 1)) &^ (BlockSize - 1)
	gap := alignedSize - curLen
	*b = append(*b, make([]byte, gap)...)
}

// BitSize 获取当前总位数
func (b *BitMap) BitSize() int {
	return len(*b) << 3
}

// FromBytes 使用已有字节数据构造BitMap
func FromBytes(bytes []byte) *BitMap {
	bm := BitMap(bytes)
	return &bm
}

// ToBytes 返回底层字节切片
func (b *BitMap) ToBytes() []byte {
	return *b
}

// SetBit 设置指定 offset 的值
func (b *BitMap) SetBit(offset int64, val byte) {
	Index := offset >> 3
	bit := offset & 7
	b.grow(offset + 1)
	mask := byte(1 << bit)
	if val > 0 {
		(*b)[Index] |= mask
	} else {
		(*b)[Index] &^= mask
	}
}

// GetBit 读取指定 offset 的值
func (b *BitMap) GetBit(offset int64) byte {
	Index := offset >> 3
	bit := offset & 7
	if Index >= int64(len(*b)) {
		return 0
	}
	return ((*b)[Index] >> bit) & 0x01
}

// ForEachBit 完整遍历指定范围内的每一位
func (b *BitMap) ForEachBit(begin int64, end int64, cb BitCallback) {
	length := int64(len(*b))
	startByte := begin >> 3
	for i := startByte; i < length; i++ {
		byteVal := (*b)[i]
		baseOffset := i << 3
		for j := 0; j < 8; j++ {
			curOffset := baseOffset + int64(j)
			if curOffset < begin {
				continue
			}
			if end > 0 && curOffset >= end {
				return
			}
			val := (byteVal >> j) & 0x01
			if !cb(curOffset, val) {
				return
			}
		}
	}
}

// ForEachSetBit 仅遍历值为 1 的位，利用 CPU 指令跳过 0 位，适合稀疏数据
func (b *BitMap) ForEachSetBit(begin int64, end int64, cb SetBitCallback) {
	length := int64(len(*b))
	startByte := begin >> 3
	for i := startByte; i < length; i++ {
		byteVal := (*b)[i]
		if byteVal == 0 {
			continue
		}
		baseOffset := i << 3
		for byteVal != 0 {
			tz := bits.TrailingZeros8(byteVal)
			curOffset := baseOffset + int64(tz)
			bitMask := byte(1 << tz)
			byteVal &^= bitMask 
			if curOffset < begin {
				continue
			}
			if end > 0 && curOffset >= end {
				return
			}
			if !cb(curOffset) {
				return
			}
		}
	}
}

// ForEachByte 按字节遍历并回调处理
func (b *BitMap) ForEachByte(begin int, end int, cb ByteCallback) {
	length := len(*b)
	if end == 0 || end > length {
		end = len(*b)
	}
	for i := begin; i < end; i++ {
		if !cb(i, (*b)[i]) {
			return
		}
	}
}