package lsm

import (
	"errors"
	"fmt"
)

// 预定义的哨兵错误，用于精确判断错误类型。
var (
	ErrClosed          = errors.New("lsm: closed")           // 引擎已关闭，操作被拒绝
	ErrInvalidOptions  = errors.New("lsm: invalid options")  // 配置参数非法
	ErrInvalidBatch    = errors.New("lsm: invalid batch")    // WriteBatch 格式错误
	ErrInvalidKey      = errors.New("lsm: invalid key")      // 键为空或无效
	ErrInvalidState    = errors.New("lsm: invalid state")    // 版本状态异常
	ErrCorrupt         = errors.New("lsm: corrupt data")     // 数据损坏（通用）
	ErrIO              = errors.New("lsm: io error")         // 底层文件 I/O 错误
	ErrWALCorrupt      = errors.New("lsm: wal corrupt")      // WAL 文件损坏
	ErrSSTableCorrupt  = errors.New("lsm: sstable corrupt")  // SSTable 文件损坏
	ErrLocked          = errors.New("lsm: directory locked") // 目录已被其他实例锁定
	ErrBackground      = errors.New("lsm: background error") // 后台任务发生错误，引擎不可用
	ErrNotImplemented  = errors.New("lsm: not implemented")  // 功能尚未实现
)

// wrapWAL 将 WAL 操作错误包装为统一格式，包含操作名。
func wrapWAL(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("wal %s: %w", op, err)
}

// wrapWALCorrupt 包装 WAL 损坏错误，若底层错误为空则单独使用 ErrWALCorrupt。
func wrapWALCorrupt(op string, err error) error {
	if err == nil {
		return fmt.Errorf("wal %s: %w", op, ErrWALCorrupt)
	}
	return fmt.Errorf("wal %s: %w", op, errors.Join(ErrWALCorrupt, err))
}

// wrapSSTableCorrupt 包装 SSTable 损坏错误，若底层错误为空则单独使用 ErrSSTableCorrupt。
func wrapSSTableCorrupt(op string, err error) error {
	if err == nil {
		return fmt.Errorf("sstable %s: %w", op, ErrSSTableCorrupt)
	}
	return fmt.Errorf("sstable %s: %w", op, errors.Join(ErrSSTableCorrupt, err))
}

// wrapIO 包装 I/O 错误，将 ErrIO 和底层错误合并。
func wrapIO(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", op, errors.Join(ErrIO, err))
}

// wrapContext 包装上下文取消错误，保留原始错误。
func wrapContext(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", op, err)
}