package lsm

import (
	"fmt"

	"mini-kv/internal/storage/lsm/record"
)

// validateWriteBatch 校验 WriteBatch 的合法性
// 规则：
//   - 所有操作的键不能为空；
//   - 操作类型必须是已知的 OpPut 或 OpDelete
// 若 batch 为 nil 则视为合法（空操作）
func validateWriteBatch(writeBatch *WriteBatch) error {
	if writeBatch == nil {
		return nil
	}
	for i, op := range writeBatch.Ops {
		if len(op.Key) == 0 {
			return fmt.Errorf("%w: empty key at op %d", ErrInvalidKey, i)
		}
		switch op.Type {
		case OpPut, OpDelete:
		default:
			return fmt.Errorf("%w: unknown op type %d at op %d", ErrInvalidBatch, op.Type, i)
		}
	}
	return nil
}

// makeRecordBatch 将外部的 WriteBatch 转换为内部的 record.Batch 表示
// 参数 seqStart 是分配给该批次中第一个条目的序列号，后续条目序列号依次递增
// 返回的 batch 中包含从 seqStart 开始连续编号的 record.Entry 切片
func makeRecordBatch(writeBatch *WriteBatch, seqStart uint64) (batch, error) {
	if err := validateWriteBatch(writeBatch); err != nil {
		return batch{}, err
	}
	// 空批次或 nil 批次只返回 SeqStart，不包含任何条目
	if writeBatch == nil || len(writeBatch.Ops) == 0 {
		return batch{SeqStart: seqStart}, nil
	}

	entries := make([]entry, 0, len(writeBatch.Ops))
	seq := seqStart
	for _, op := range writeBatch.Ops {
		switch op.Type {
		case OpPut:
			// 创建 Put 条目，使用当前序列号 seq，然后 seq 递增
			entries = append(entries, record.NewPut(op.Key, op.Value, seq))
		case OpDelete:
			// 创建 Delete 条目，同样分配序列号
			entries = append(entries, record.NewDelete(op.Key, seq))
		}
		seq++
	}

	return batch{
		SeqStart: seqStart,
		Entries:  entries,
	}, nil
}