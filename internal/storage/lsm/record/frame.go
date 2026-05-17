package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// 帧格式常量
const (
	frameHeaderSize = 8     // 帧头部固定长度：4 字节长度 + 4 字节 CRC32
	batchPayload    = byte(1) // Batch 载荷的类型标记
)

// 编解码过程中的错误哨兵
var (
	ErrBadRecord = errors.New("record: bad record")     // 记录格式无效
	ErrChecksum  = errors.New("record: checksum mismatch") // CRC 校验不匹配
	ErrPartial   = errors.New("record: partial record") // 部分写入（需要更多数据）
)

// EncodeFrame 为载荷添加长度前缀和 CRC32 校验和，返回完整的帧字节切片
func EncodeFrame(payload []byte) []byte {
	out := make([]byte, frameHeaderSize+len(payload))
	// 写入载荷长度
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(payload)))
	// 写入 CRC32 校验和
	binary.LittleEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(payload))
	// 复制载荷内容
	copy(out[frameHeaderSize:], payload)
	return out
}

// DecodeFrame 从字节切片中解码一帧，返回载荷内容、消费的总字节数和可能的错误
func DecodeFrame(data []byte) ([]byte, int, error) {
	if len(data) < frameHeaderSize {
		// 连头部都不完整，返回部分记录错误
		if len(data) == 0 {
			return nil, 0, ErrPartial
		}
		return nil, 0, fmt.Errorf("%w: short header", ErrPartial)
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	total := frameHeaderSize + int(length)
	// 校验长度有效性
	if int(length) < 0 || total < frameHeaderSize {
		return nil, 0, fmt.Errorf("%w: invalid length", ErrBadRecord)
	}
	// 检查载荷是否完整
	if len(data) < total {
		return nil, 0, fmt.Errorf("%w: short payload", ErrPartial)
	}
	payload := data[frameHeaderSize:total]
	// CRC 校验
	if got, want := crc32.ChecksumIEEE(payload), binary.LittleEndian.Uint32(data[4:8]); got != want {
		return nil, 0, ErrChecksum
	}
	// 返回载荷拷贝，防止外部修改影响后续解码
	return CloneBytes(payload), total, nil
}

// EncodeRecord 将单条 Entry 编码为一个完整帧（内部包装为单条目 Batch）
func EncodeRecord(entry Entry) ([]byte, error) {
	return EncodeBatchFrame(Batch{
		SeqStart: entry.Seq,
		Entries:  []Entry{entry},
	})
}

// DecodeRecord 从字节切片中解码出一条 Entry 和消费的字节数
func DecodeRecord(data []byte) (Entry, int, error) {
	batch, consumed, err := DecodeBatchFrame(data)
	if err != nil {
		return Entry{}, 0, err
	}
	if len(batch.Entries) != 1 {
		return Entry{}, 0, fmt.Errorf("%w: expected one entry", ErrBadRecord)
	}
	return batch.Entries[0], consumed, nil
}

// EncodeBatchFrame 将一个 Batch 编码为带有帧头和校验和的完整帧
func EncodeBatchFrame(batch Batch) ([]byte, error) {
	payload, err := EncodeBatchPayload(batch)
	if err != nil {
		return nil, err
	}
	return EncodeFrame(payload), nil
}

// DecodeBatchFrame 从字节切片中解码出 Batch 和消费的字节数
func DecodeBatchFrame(data []byte) (Batch, int, error) {
	payload, consumed, err := DecodeFrame(data)
	if err != nil {
		return Batch{}, 0, err
	}
	batch, err := DecodeBatchPayload(payload)
	if err != nil {
		return Batch{}, 0, err
	}
	return batch, consumed, nil
}

// EncodeBatchPayload 将 Batch 序列化为二进制载荷，不包含帧头部
func EncodeBatchPayload(batch Batch) ([]byte, error) {
	if len(batch.Entries) > int(^uint32(0)) {
		return nil, fmt.Errorf("%w: too many entries", ErrBadRecord)
	}
	// 计算总大小：类型标记(1) + seqStart(8) + count(4) + 每个 entry 的大小
	size := 1 + 8 + 4
	for _, entry := range batch.Entries {
		if len(entry.Key) > int(^uint32(0)) || len(entry.Value) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: entry too large", ErrBadRecord)
		}
		size += 1 + 8 + 4 + 4 + len(entry.Key) + len(entry.Value)
	}
	out := make([]byte, 0, size)
	// 写入批次类型标记
	out = append(out, batchPayload)
	// 写入起始序列号
	out = binary.LittleEndian.AppendUint64(out, batch.SeqStart)
	// 写入条目数量
	out = binary.LittleEndian.AppendUint32(out, uint32(len(batch.Entries)))
	// 依次写入每个条目
	for _, entry := range batch.Entries {
		out = append(out, byte(entry.Kind))
		out = binary.LittleEndian.AppendUint64(out, entry.Seq)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.Key)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.Value)))
		out = append(out, entry.Key...)
		out = append(out, entry.Value...)
	}
	return out, nil
}

// DecodeBatchPayload 从二进制载荷解码出 Batch
func DecodeBatchPayload(data []byte) (Batch, error) {
	reader := payloadReader{data: data}
	// 读取并检查类型标记
	payloadKind, ok := reader.u8()
	if !ok || payloadKind != batchPayload {
		return Batch{}, fmt.Errorf("%w: invalid batch payload", ErrBadRecord)
	}
	// 读取起始序列号
	seqStart, ok := reader.u64()
	if !ok {
		return Batch{}, fmt.Errorf("%w: missing sequence", ErrBadRecord)
	}
	// 读取条目数量
	count, ok := reader.u32()
	if !ok {
		return Batch{}, fmt.Errorf("%w: missing entry count", ErrBadRecord)
	}
	entries := make([]Entry, 0, count)
	// 逐条解码
	for i := uint32(0); i < count; i++ {
		kind, ok := reader.u8()
		if !ok {
			return Batch{}, fmt.Errorf("%w: missing kind", ErrBadRecord)
		}
		seq, ok := reader.u64()
		if !ok {
			return Batch{}, fmt.Errorf("%w: missing sequence", ErrBadRecord)
		}
		keyLen, ok := reader.u32()
		if !ok {
			return Batch{}, fmt.Errorf("%w: missing key length", ErrBadRecord)
		}
		valueLen, ok := reader.u32()
		if !ok {
			return Batch{}, fmt.Errorf("%w: missing value length", ErrBadRecord)
		}
		key, ok := reader.bytes(int(keyLen))
		if !ok {
			return Batch{}, fmt.Errorf("%w: truncated key", ErrBadRecord)
		}
		value, ok := reader.bytes(int(valueLen))
		if !ok {
			return Batch{}, fmt.Errorf("%w: truncated value", ErrBadRecord)
		}
		entries = append(entries, Entry{
			Key:   key,
			Value: value,
			Seq:   seq,
			Kind:  Kind(kind),
		})
	}
	// 不允许残留数据
	if reader.remaining() != 0 {
		return Batch{}, fmt.Errorf("%w: trailing bytes", ErrBadRecord)
	}
	return Batch{SeqStart: seqStart, Entries: entries}, nil
}

// payloadReader 用于从字节切片中顺序读取二进制字段
type payloadReader struct {
	data []byte
	off  int
}

func (r *payloadReader) remaining() int {
	return len(r.data) - r.off
}

func (r *payloadReader) u8() (byte, bool) {
	if r.remaining() < 1 {
		return 0, false
	}
	value := r.data[r.off]
	r.off++
	return value, true
}

func (r *payloadReader) u32() (uint32, bool) {
	if r.remaining() < 4 {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(r.data[r.off:])
	r.off += 4
	return value, true
}

func (r *payloadReader) u64() (uint64, bool) {
	if r.remaining() < 8 {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(r.data[r.off:])
	r.off += 8
	return value, true
}

func (r *payloadReader) bytes(n int) ([]byte, bool) {
	if n < 0 || r.remaining() < n {
		return nil, false
	}
	// 返回克隆后的字节切片，保证安全
	value := CloneBytes(r.data[r.off : r.off+n])
	r.off += n
	return value, true
}