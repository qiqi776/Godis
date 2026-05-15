package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	frameHeaderSize = 8
	batchPayload    = byte(1)
)

var (
	ErrBadRecord = errors.New("record: bad record")
	ErrChecksum  = errors.New("record: checksum mismatch")
	ErrPartial   = errors.New("record: partial record")
)

func EncodeFrame(payload []byte) []byte {
	out := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(payload))
	copy(out[frameHeaderSize:], payload)
	return out
}

func DecodeFrame(data []byte) ([]byte, int, error) {
	if len(data) < frameHeaderSize {
		if len(data) == 0 {
			return nil, 0, ErrPartial
		}
		return nil, 0, fmt.Errorf("%w: short header", ErrPartial)
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	total := frameHeaderSize + int(length)
	if int(length) < 0 || total < frameHeaderSize {
		return nil, 0, fmt.Errorf("%w: invalid length", ErrBadRecord)
	}
	if len(data) < total {
		return nil, 0, fmt.Errorf("%w: short payload", ErrPartial)
	}
	payload := data[frameHeaderSize:total]
	if got, want := crc32.ChecksumIEEE(payload), binary.LittleEndian.Uint32(data[4:8]); got != want {
		return nil, 0, ErrChecksum
	}
	return CloneBytes(payload), total, nil
}

func EncodeRecord(entry Entry) ([]byte, error) {
	return EncodeBatchFrame(Batch{
		SeqStart: entry.Seq,
		Entries:  []Entry{entry},
	})
}

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

func EncodeBatchFrame(batch Batch) ([]byte, error) {
	payload, err := EncodeBatchPayload(batch)
	if err != nil {
		return nil, err
	}
	return EncodeFrame(payload), nil
}

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

func EncodeBatchPayload(batch Batch) ([]byte, error) {
	if len(batch.Entries) > int(^uint32(0)) {
		return nil, fmt.Errorf("%w: too many entries", ErrBadRecord)
	}
	size := 1 + 8 + 4
	for _, entry := range batch.Entries {
		if len(entry.Key) > int(^uint32(0)) || len(entry.Value) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: entry too large", ErrBadRecord)
		}
		size += 1 + 8 + 4 + 4 + len(entry.Key) + len(entry.Value)
	}
	out := make([]byte, 0, size)
	out = append(out, batchPayload)
	out = binary.LittleEndian.AppendUint64(out, batch.SeqStart)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(batch.Entries)))
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

func DecodeBatchPayload(data []byte) (Batch, error) {
	reader := payloadReader{data: data}
	payloadKind, ok := reader.u8()
	if !ok || payloadKind != batchPayload {
		return Batch{}, fmt.Errorf("%w: invalid batch payload", ErrBadRecord)
	}
	seqStart, ok := reader.u64()
	if !ok {
		return Batch{}, fmt.Errorf("%w: missing sequence", ErrBadRecord)
	}
	count, ok := reader.u32()
	if !ok {
		return Batch{}, fmt.Errorf("%w: missing entry count", ErrBadRecord)
	}
	entries := make([]Entry, 0, count)
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
	if reader.remaining() != 0 {
		return Batch{}, fmt.Errorf("%w: trailing bytes", ErrBadRecord)
	}
	return Batch{SeqStart: seqStart, Entries: entries}, nil
}

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
	value := CloneBytes(r.data[r.off : r.off+n])
	r.off += n
	return value, true
}
