package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const bloomVersion uint32 = 1

var ErrInvalidBloom = errors.New("bloom: invalid filter")

type Bloom struct {
	bits     []uint64
	bitCount uint64
	hashes   uint8
}

type BloomBuilder struct {
	bits     []uint64
	bitCount uint64
	hashes   uint8
}

func NewBloomBuilder(expectedKeys int, bitsPerKey int) *BloomBuilder {
	if expectedKeys < 1 {
		expectedKeys = 1
	}
	if bitsPerKey < 1 {
		bitsPerKey = 10
	}
	bitCount := uint64(expectedKeys * bitsPerKey)
	if bitCount < 64 {
		bitCount = 64
	}
	words := (bitCount + 63) / 64
	bitCount = words * 64

	hashes := uint8(bitsPerKey * 69 / 100)
	if hashes < 1 {
		hashes = 1
	}
	if hashes > 30 {
		hashes = 30
	}

	return &BloomBuilder{
		bits:     make([]uint64, words),
		bitCount: bitCount,
		hashes:   hashes,
	}
}

func (b *BloomBuilder) Add(key []byte) {
	setBits(b.bits, b.bitCount, b.hashes, key)
}

func (b *BloomBuilder) Finish() *Bloom {
	bits := make([]uint64, len(b.bits))
	copy(bits, b.bits)
	return &Bloom{
		bits:     bits,
		bitCount: b.bitCount,
		hashes:   b.hashes,
	}
}

func (b *Bloom) MayContain(key []byte) bool {
	if b == nil || b.bitCount == 0 || b.hashes == 0 || len(b.bits) == 0 {
		return false
	}
	h1 := hash64(key)
	h2 := mix64(h1)
	for i := uint8(0); i < b.hashes; i++ {
		bit := (h1 + uint64(i)*h2) % b.bitCount
		if b.bits[bit/64]&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func (b *Bloom) MarshalBinary() ([]byte, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: nil bloom", ErrInvalidBloom)
	}
	out := make([]byte, 0, 4+1+8+8+len(b.bits)*8)
	out = binary.LittleEndian.AppendUint32(out, bloomVersion)
	out = append(out, b.hashes)
	out = binary.LittleEndian.AppendUint64(out, b.bitCount)
	out = binary.LittleEndian.AppendUint64(out, uint64(len(b.bits)))
	for _, word := range b.bits {
		out = binary.LittleEndian.AppendUint64(out, word)
	}
	return out, nil
}

func DecodeBloom(data []byte) (*Bloom, error) {
	reader := bloomReader{data: data}
	version, ok := reader.u32()
	if !ok || version != bloomVersion {
		return nil, fmt.Errorf("%w: unsupported version", ErrInvalidBloom)
	}
	hashes, ok := reader.u8()
	if !ok || hashes == 0 {
		return nil, fmt.Errorf("%w: invalid hash count", ErrInvalidBloom)
	}
	bitCount, ok := reader.u64()
	if !ok || bitCount == 0 || bitCount%64 != 0 {
		return nil, fmt.Errorf("%w: invalid bit count", ErrInvalidBloom)
	}
	wordCount, ok := reader.u64()
	if !ok || wordCount != bitCount/64 {
		return nil, fmt.Errorf("%w: invalid word count", ErrInvalidBloom)
	}
	if wordCount > uint64(reader.remaining()/8) {
		return nil, fmt.Errorf("%w: truncated words", ErrInvalidBloom)
	}
	bits := make([]uint64, wordCount)
	for i := range bits {
		word, ok := reader.u64()
		if !ok {
			return nil, fmt.Errorf("%w: truncated word", ErrInvalidBloom)
		}
		bits[i] = word
	}
	if reader.remaining() != 0 {
		return nil, fmt.Errorf("%w: trailing bytes", ErrInvalidBloom)
	}
	return &Bloom{
		bits:     bits,
		bitCount: bitCount,
		hashes:   hashes,
	}, nil
}

func setBits(bits []uint64, bitCount uint64, hashes uint8, key []byte) {
	h1 := hash64(key)
	h2 := mix64(h1)
	for i := uint8(0); i < hashes; i++ {
		bit := (h1 + uint64(i)*h2) % bitCount
		bits[bit/64] |= uint64(1) << (bit % 64)
	}
}

func hash64(data []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	hash := uint64(offset)
	for _, b := range data {
		hash ^= uint64(b)
		hash *= prime
	}
	return hash
}

func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	if x == 0 {
		return 0x9e3779b97f4a7c15
	}
	return x
}

type bloomReader struct {
	data []byte
	off  int
}

func (r *bloomReader) remaining() int {
	return len(r.data) - r.off
}

func (r *bloomReader) u8() (uint8, bool) {
	if r.remaining() < 1 {
		return 0, false
	}
	value := r.data[r.off]
	r.off++
	return value, true
}

func (r *bloomReader) u32() (uint32, bool) {
	if r.remaining() < 4 {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(r.data[r.off:])
	r.off += 4
	return value, true
}

func (r *bloomReader) u64() (uint64, bool) {
	if r.remaining() < 8 {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(r.data[r.off:])
	r.off += 8
	return value, true
}
