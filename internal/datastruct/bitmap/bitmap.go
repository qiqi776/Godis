package bitmap

import (
	"errors"
	"math/bits"
)

var (
	ErrBitOffset = errors.New("ERR bit offset is not an integer or out of range")
	ErrBitValue  = errors.New("ERR bit is not an integer or out of range")
)

type Bitmap struct {
	data []byte
}

func New() *Bitmap {
	return &Bitmap{}
}

func (b *Bitmap) SetBit(offset int64, bit int) (int64, error) {
	if offset < 0 {
		return 0, ErrBitOffset
	}
	if bit != 0 && bit != 1 {
		return 0, ErrBitValue
	}

	byteIndex := int(offset / 8)
	b.ensure(byteIndex)

	mask := byte(1 << (7 - (offset % 8)))

	var old int64
	if b.data[byteIndex]&mask != 0 {
		old = 1
	}

	if bit == 1 {
		b.data[byteIndex] |= mask
	} else {
		b.data[byteIndex] &^= mask
	}

	return old, nil
}

func (b *Bitmap) GetBit(offset int64) (int64, error) {
	if offset < 0 {
		return 0, ErrBitOffset
	}

	byteIndex := int(offset / 8)
	if byteIndex >= len(b.data) {
		return 0, nil
	}

	mask := byte(1 << (7 - (offset % 8)))
	if b.data[byteIndex]&mask != 0 {
		return 1, nil
	}
	return 0, nil
}

func (b *Bitmap) Count() int64 {
	var total int64
	for _, item := range b.data {
		total += int64(bits.OnesCount8(item))
	}
	return total
}

func (b *Bitmap) SetBits() []int64 {
	out := make([]int64, 0, b.Count())
	for byteIndex, item := range b.data {
		if item == 0 {
			continue
		}
		for bitIndex := 0; bitIndex < 8; bitIndex++ {
			mask := byte(1 << (7 - bitIndex))
			if item&mask != 0 {
				out = append(out, int64(byteIndex*8+bitIndex))
			}
		}
	}
	return out
}

func (b *Bitmap) ensure(byteIndex int) {
	if byteIndex < len(b.data) {
		return
	}

	next := make([]byte, byteIndex+1)
	copy(next, b.data)
	b.data = next
}