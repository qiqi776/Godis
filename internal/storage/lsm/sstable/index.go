package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"mini-kv/internal/storage/lsm/record"
)

// 索引数据无效
var ErrInvalidIndex = errors.New("sstable: invalid index")

// BlockHandle 指向 SSTable 中一个数据块的物理位置
type BlockHandle struct {
	Offset uint64 // 数据块在文件中的偏移量
	Length uint32 // 数据块的长度
}

// IndexEntry 是一条索引记录，记录某个键范围对应的数据块句柄
type IndexEntry struct {
	FirstKey []byte      // 该块包含的最小键
	LastKey  []byte      // 该块包含的最大键
	Handle   BlockHandle // 数据块的位置信息
}

// Index 将键范围映射到数据块句柄
//
// 条目必须按键排序且不重叠。SSTable 数据块不可变，
// 因此 Index 在构造和导出条目时会防御性地克隆键
type Index struct {
	entries []IndexEntry
}

// NewIndex 根据给定的条目创建索引，同时校验条目顺序与范围合法性
func NewIndex(entries []IndexEntry) (*Index, error) {
	cloned := make([]IndexEntry, len(entries))
	for i, entry := range entries {
		if len(entry.FirstKey) == 0 || len(entry.LastKey) == 0 {
			return nil, fmt.Errorf("%w: empty key at entry %d", ErrInvalidIndex, i)
		}
		if bytes.Compare(entry.FirstKey, entry.LastKey) > 0 {
			return nil, fmt.Errorf("%w: inverted range at entry %d", ErrInvalidIndex, i)
		}
		if i > 0 && bytes.Compare(cloned[i-1].LastKey, entry.FirstKey) >= 0 {
			return nil, fmt.Errorf("%w: overlapping ranges at entry %d", ErrInvalidIndex, i)
		}
		cloned[i] = IndexEntry{
			FirstKey: record.CloneBytes(entry.FirstKey),
			LastKey:  record.CloneBytes(entry.LastKey),
			Handle:   entry.Handle,
		}
	}
	return &Index{entries: cloned}, nil
}

// Find 使用二分查找定位 key 所在的数据块句柄
func (idx *Index) Find(key []byte) (BlockHandle, bool) {
	pos := sort.Search(len(idx.entries), func(i int) bool {
		return bytes.Compare(idx.entries[i].LastKey, key) >= 0
	})
	if pos >= len(idx.entries) {
		return BlockHandle{}, false
	}
	entry := idx.entries[pos]
	if bytes.Compare(key, entry.FirstKey) < 0 {
		return BlockHandle{}, false
	}
	return entry.Handle, true
}

// Entries 返回索引中所有条目的深拷贝切片
func (idx *Index) Entries() []IndexEntry {
	entries := make([]IndexEntry, len(idx.entries))
	for i, entry := range idx.entries {
		entries[i] = IndexEntry{
			FirstKey: record.CloneBytes(entry.FirstKey),
			LastKey:  record.CloneBytes(entry.LastKey),
			Handle:   entry.Handle,
		}
	}
	return entries
}

// EncodeIndex 将条目序列化为可持久化的字节表示
func EncodeIndex(entries []IndexEntry) ([]byte, error) {
	idx, err := NewIndex(entries)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 4+len(idx.entries)*32)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(idx.entries)))
	for _, entry := range idx.entries {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.FirstKey)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.LastKey)))
		out = binary.LittleEndian.AppendUint64(out, entry.Handle.Offset)
		out = binary.LittleEndian.AppendUint32(out, entry.Handle.Length)
		out = append(out, entry.FirstKey...)
		out = append(out, entry.LastKey...)
	}
	return out, nil
}

// DecodeIndex 从字节切片反序列化出一个索引
func DecodeIndex(data []byte) (*Index, error) {
	reader := indexReader{data: data}
	count, ok := reader.u32()
	if !ok {
		return nil, fmt.Errorf("%w: missing entry count", ErrInvalidIndex)
	}
	if count > uint32(len(data)/20+1) {
		return nil, fmt.Errorf("%w: impossible entry count %d", ErrInvalidIndex, count)
	}
	entries := make([]IndexEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		firstLen, ok := reader.u32()
		if !ok {
			return nil, fmt.Errorf("%w: missing first key length at entry %d", ErrInvalidIndex, i)
		}
		lastLen, ok := reader.u32()
		if !ok {
			return nil, fmt.Errorf("%w: missing last key length at entry %d", ErrInvalidIndex, i)
		}
		offset, ok := reader.u64()
		if !ok {
			return nil, fmt.Errorf("%w: missing offset at entry %d", ErrInvalidIndex, i)
		}
		length, ok := reader.u32()
		if !ok {
			return nil, fmt.Errorf("%w: missing length at entry %d", ErrInvalidIndex, i)
		}
		first, ok := reader.bytes(int(firstLen))
		if !ok {
			return nil, fmt.Errorf("%w: truncated first key at entry %d", ErrInvalidIndex, i)
		}
		last, ok := reader.bytes(int(lastLen))
		if !ok {
			return nil, fmt.Errorf("%w: truncated last key at entry %d", ErrInvalidIndex, i)
		}
		entries = append(entries, IndexEntry{
			FirstKey: first,
			LastKey:  last,
			Handle: BlockHandle{
				Offset: offset,
				Length: length,
			},
		})
	}
	if reader.remaining() != 0 {
		return nil, fmt.Errorf("%w: trailing %d bytes", ErrInvalidIndex, reader.remaining())
	}
	return NewIndex(entries)
}

// indexReader 是一个简单的二进制读取辅助结构，用于从字节切片中顺序解码数据
type indexReader struct {
	data []byte
	off  int
}

// remaining 返回尚未读取的字节数。
func (r *indexReader) remaining() int {
	return len(r.data) - r.off
}

// u32 读取一个 little-endian uint32
func (r *indexReader) u32() (uint32, bool) {
	if r.remaining() < 4 {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(r.data[r.off:])
	r.off += 4
	return value, true
}

// u64 读取一个 little-endian uint64
func (r *indexReader) u64() (uint64, bool) {
	if r.remaining() < 8 {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(r.data[r.off:])
	r.off += 8
	return value, true
}

// bytes 读取 n 个字节，并返回一份拷贝
func (r *indexReader) bytes(n int) ([]byte, bool) {
	if n < 0 || r.remaining() < n {
		return nil, false
	}
	value := record.CloneBytes(r.data[r.off : r.off+n])
	r.off += n
	return value, true
}