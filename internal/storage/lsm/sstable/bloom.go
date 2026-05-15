package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const bloomVersion uint32 = 1

var ErrInvalidBloom = errors.New("bloom: invalid filter")

// Bloom 是不可变的布隆过滤器，用于快速判定键是否“可能存在”
type Bloom struct {
	bits     []uint64 // 位数组，每个元素存储 64 位
	bitCount uint64   // 位数组总位数（始终是 64 的倍数）
	hashes   uint8    // 使用的哈希函数个数
}

// BloomBuilder 用于逐步构建布隆过滤器
type BloomBuilder struct {
	bits     []uint64
	bitCount uint64
	hashes   uint8
}

// NewBloomBuilder 根据预期键数量和每个键的位数创建构建器
// expectedKeys: 预期插入的键数量
// bitsPerKey:   每个键平均分配的位数，默认 10，对应约 1% 的假阳性率
func NewBloomBuilder(expectedKeys int, bitsPerKey int) *BloomBuilder {
	if expectedKeys < 1 {
		expectedKeys = 1
	}
	if bitsPerKey < 1 {
		bitsPerKey = 10
	}
	// 计算总位数，并对齐到 64 的倍数
	bitCount := uint64(expectedKeys * bitsPerKey)
	if bitCount < 64 {
		bitCount = 64
	}
	words := (bitCount + 63) / 64
	bitCount = words * 64

	// 最优哈希函数个数 k = (bitsPerKey) * ln(2)，近似 0.69
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

// Add 向构建器中添加一个键，将其对应的 k 个位置置 1
func (b *BloomBuilder) Add(key []byte) {
	setBits(b.bits, b.bitCount, b.hashes, key)
}

// Finish 生成一个不可变的 Bloom 副本，此后对构建器的修改不会影响它
func (b *BloomBuilder) Finish() *Bloom {
	bits := make([]uint64, len(b.bits))
	copy(bits, b.bits)
	return &Bloom{
		bits:     bits,
		bitCount: b.bitCount,
		hashes:   b.hashes,
	}
}

// MayContain 检查键是否“可能存在”返回 false 表示键一定不存在
// 返回 true 表示键可能存在（有假阳性概率）
func (b *Bloom) MayContain(key []byte) bool {
	if b == nil || b.bitCount == 0 || b.hashes == 0 || len(b.bits) == 0 {
		return false
	}
	h1 := hash64(key)
	h2 := mix64(h1)
	for i := uint8(0); i < b.hashes; i++ {
		bit := (h1 + uint64(i)*h2) % b.bitCount
		// 若某一位为 0，则该键一定不存在
		if b.bits[bit/64]&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// MarshalBinary 序列化布隆过滤器为二进制格式：
//
//	version(4B) | hashes(1B) | bitCount(8B) | wordCount(8B) | bits...
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

// DecodeBloom 从二进制数据中反序列化布隆过滤器，并进行严格的完整性校验
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

// setBits 利用双重哈希在布隆过滤器中置位 k 次
func setBits(bits []uint64, bitCount uint64, hashes uint8, key []byte) {
	h1 := hash64(key)
	h2 := mix64(h1)
	for i := uint8(0); i < hashes; i++ {
		bit := (h1 + uint64(i)*h2) % bitCount
		bits[bit/64] |= uint64(1) << (bit % 64)
	}
}

// hash64 基于 FNV-1a 变体的 64 位哈希函数
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

// mix64 对 64 位整数进行混淆，生成第二个哈希值，并确保非零
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	if x == 0 {
		return 0x9e3779b97f4a7c15 // 黄金比常数备用
	}
	return x
}

// bloomReader 是解码布隆过滤器时的简易字节流读取器
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