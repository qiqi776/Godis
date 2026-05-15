package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"mini-kv/internal/storage/lsm/record"
)

const (
	defaultSegmentSize = 64 << 20 // 默认段文件大小 64MB
	filePrefix         = "WAL-"   // 段文件名前缀
)

type Options struct {
	SegmentSize int64 // 单个段文件的最大字节数，若未设置则使用默认值
}

// Store 是预写日志的存储实例，管理分段文件
// 所有写入先追加到当前段文件，超过大小限制时自动创建新段
type Store struct {
	mu          sync.Mutex // 互斥锁，保证所有操作串行化
	dir         string     // WAL 文件存放目录
	segmentSize int64      // 段文件大小阈值
	fileNum     uint64     // 当前正在写入的段编号
	file        *os.File   // 当前段文件句柄
	offset      int64      // 当前段文件的写入偏移量
}

// Open 打开指定目录下的 WAL，准备追加写入
// fileNum 指定期望的起始段编号；若目录中已有编号更大的段则使用实际最大值
func Open(dir string, fileNum uint64, opts Options) (*Store, error) {
	if fileNum == 0 {
		fileNum = 1
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	// 确保目录存在
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	// 列出已有段文件
	files, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	// 如果已有段的编号不小于期望值，则从最大编号继续写入
	if len(files) > 0 && files[len(files)-1].num >= fileNum {
		fileNum = files[len(files)-1].num
	}
	// 打开（或创建）当前段文件，以追加模式写入
	file, err := os.OpenFile(segmentPath(dir, fileNum), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal segment: %w", err)
	}
	// 定位到文件末尾，获取当前偏移量
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("seek wal segment: %w", err)
	}
	return &Store{
		dir:         dir,
		segmentSize: opts.SegmentSize,
		fileNum:     fileNum,
		file:        file,
		offset:      offset,
	}, nil
}

// Append 将一个批次编码后追加到 WAL，并根据 syncWrite 决定是否立即刷盘
func (s *Store) Append(batch record.Batch, syncWrite bool) error {
	encoded, err := record.EncodeBatchFrame(batch)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}
	// 如果当前段已有数据，且追加后会超过段大小，则先创建新段
	if s.offset > 0 && s.offset+int64(len(encoded)) > s.segmentSize {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}
	// 写入编码后的数据
	n, err := s.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("write wal segment: %w", err)
	}
	if n != len(encoded) {
		return io.ErrShortWrite
	}
	s.offset += int64(n)
	// 若要求同步写，则调用 Sync 强制落盘
	if syncWrite {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("sync wal segment: %w", err)
		}
	}
	return nil
}

// Replay 回放所有 WAL 段文件，对每个批次调用 fn 进行恢复（通常写入 MemTable）
func (s *Store) Replay(fn func(record.Batch) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	segments, err := listSegments(s.dir)
	if err != nil {
		return err
	}
	// 按编号顺序回放每个段；最后一个段标记为尾部（可能存在不完整写入）
	for i, segment := range segments {
		if err := replaySegment(segment.path, i == len(segments)-1, fn); err != nil {
			return err
		}
	}
	// 回放完成后重新定位当前文件末尾，更新 offset
	if s.file != nil {
		offset, err := s.file.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("seek wal segment after replay: %w", err)
		}
		s.offset = offset
	}
	return nil
}

// Purge 删除所有最大序列号不超过 flushedSeq 的旧段文件（已刷入 SSTable 的部分）
func (s *Store) Purge(flushedSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	segments, err := listSegments(s.dir)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		// 跳过当前正在写入的段，以及编号更大的段
		if segment.num >= s.fileNum {
			continue
		}
		maxSeq, err := maxSegmentSeq(segment.path)
		if err != nil {
			return err
		}
		// 只有当段内所有条目序列号都不超过已刷盘序列号时，才安全删除
		if maxSeq <= flushedSeq {
			if err := os.Remove(segment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove wal segment: %w", err)
			}
		}
	}
	return nil
}

// Close 关闭当前段文件，释放资源
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// rotateLocked 在持有锁的情况下创建新的 WAL 段文件
func (s *Store) rotateLocked() error {
	if s.file != nil {
		// 确保旧段数据落盘
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("sync wal before rotate: %w", err)
		}
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("close wal before rotate: %w", err)
		}
	}
	s.fileNum++
	// 创建新段文件
	file, err := os.OpenFile(segmentPath(s.dir, s.fileNum), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open rotated wal segment: %w", err)
	}
	s.file = file
	s.offset = 0
	return nil
}

// replaySegment 回放单个 WAL 段文件
// isTail 指示该段是否为最后一个段，若是则允许截断尾部不完整的批次
func replaySegment(path string, isTail bool, fn func(record.Batch) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read wal segment: %w", err)
	}
	offset := 0
	for offset < len(data) {
		batch, consumed, err := record.DecodeBatchFrame(data[offset:])
		if err != nil {
			// 如果是尾部不完整错误，并且是最后一个段，则截断文件到正确位置
			if errors.Is(err, record.ErrPartial) && isTail {
				if truncateErr := os.Truncate(path, int64(offset)); truncateErr != nil {
					return fmt.Errorf("truncate wal tail: %w", truncateErr)
				}
				return nil
			}
			return fmt.Errorf("decode wal segment: %w", err)
		}
		// 将克隆后的批次传递给恢复函数
		if err := fn(batch.Clone()); err != nil {
			return err
		}
		offset += consumed
	}
	return nil
}

// maxSegmentSeq 返回指定段文件中所有条目的最大序列号
func maxSegmentSeq(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read wal segment: %w", err)
	}
	offset := 0
	var maxSeq uint64
	for offset < len(data) {
		batch, consumed, err := record.DecodeBatchFrame(data[offset:])
		if err != nil {
			return 0, fmt.Errorf("decode wal segment: %w", err)
		}
		for _, entry := range batch.Entries {
			if entry.Seq > maxSeq {
				maxSeq = entry.Seq
			}
		}
		offset += consumed
	}
	return maxSeq, nil
}

// segmentFile 记录一个 WAL 段文件的编号和完整路径
type segmentFile struct {
	num  uint64
	path string
}

// listSegments 列出目录下所有 WAL 段文件，并按编号排序
func listSegments(dir string) ([]segmentFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("list wal dir: %w", err)
	}
	files := make([]segmentFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), filePrefix) {
			continue
		}
		// 从文件名提取编号
		num, err := strconv.ParseUint(strings.TrimPrefix(entry.Name(), filePrefix), 10, 64)
		if err != nil {
			continue
		}
		files = append(files, segmentFile{
			num:  num,
			path: filepath.Join(dir, entry.Name()),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].num < files[j].num
	})
	return files, nil
}

// segmentPath 返回指定编号的段文件完整路径（例如 WAL-000001）
func segmentPath(dir string, fileNum uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d", filePrefix, fileNum))
}