package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"mini-kv/internal/storage/lsm/record"
	version "mini-kv/internal/storage/lsm/sstable"
)

const (
	currentFile   = "CURRENT"         // 指向当前 MANIFEST 文件名的指针文件
	manifestFmt   = "MANIFEST-%06d"   // MANIFEST 文件名模板，如 MANIFEST-000001
	defaultNumber = uint64(1)         // 默认起始文件编号
)

// Store 负责持久化和管理 LSM 版本状态
// 所有版本变更（Edit）先追加写入 MANIFEST 文件并立即刷盘，再更新内存中的 State
type Store struct {
	mu      sync.Mutex      // 互斥锁，保证所有操作串行化
	dir     string          // MANIFEST 文件存放目录
	fileNum uint64          // 当前 MANIFEST 文件编号
	file    *os.File        // 当前 MANIFEST 文件句柄
	state   *version.State  // 内存中重放后的最新版本状态
}

// Open 打开或创建 MANIFEST，返回可用的 Store
// 若 CURRENT 文件存在则沿用其指向的 MANIFEST，否则创建新的并写入 CURRENT
func Open(dir string, fileNum uint64) (*Store, error) {
	if fileNum == 0 {
		fileNum = defaultNumber
	}
	// 确保目录存在
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create manifest dir: %w", err)
	}
	// 读取 CURRENT 文件获取当前 MANIFEST 文件名
	name, err := readCurrent(dir)
	if err != nil {
		return nil, err
	}
	// 若 CURRENT 不存在则创建新的 MANIFEST 文件并记录
	if name == "" {
		name = manifestName(fileNum)
		if err := writeCurrent(dir, name); err != nil {
			return nil, err
		}
	}
	// 打开 MANIFEST 文件（追加模式）
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	// 初始化状态并重放 MANIFEST 中的所有 Edit
	store := &Store{
		dir:     dir,
		fileNum: fileNum,
		file:    file,
		state:   (&version.State{NextFileNum: 1}).Clone(),
	}
	if err := store.replayLocked(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	return store, nil
}

// Load 返回当前版本状态的深拷贝，供外部只读使用
func (s *Store) Load() (*version.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Clone(), nil
}

// Apply 将一个版本变更 Edit 持久化到 MANIFEST 并更新内存状态
// Edit 先序列化为 JSON 帧写入文件并 Sync，再调用 state.Apply 更新内存
func (s *Store) Apply(edit version.Edit) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload, err := json.Marshal(edit.Clone())
	if err != nil {
		return fmt.Errorf("marshal manifest edit: %w", err)
	}
	// 写入编码帧
	if _, err := s.file.Write(record.EncodeFrame(payload)); err != nil {
		return fmt.Errorf("write manifest edit: %w", err)
	}
	// 强制刷盘，保证 MANIFEST 和之前的状态一致
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync manifest edit: %w", err)
	}
	s.state = s.state.Apply(edit)
	return nil
}

// Close 关闭 MANIFEST 文件句柄
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

// replayLocked 在持有锁时重放 MANIFEST 文件中的所有 Edit，重建 State
// 遇到不完整帧时截断文件，保证下次打开不会再次失败
func (s *Store) replayLocked(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	offset := 0
	for offset < len(data) {
		payload, consumed, err := record.DecodeFrame(data[offset:])
		if err != nil {
			// 不完整帧在崩溃时可能出现，截断尾部保证数据一致性
			if errors.Is(err, record.ErrPartial) {
				if truncateErr := os.Truncate(path, int64(offset)); truncateErr != nil {
					return fmt.Errorf("truncate manifest tail: %w", truncateErr)
				}
				return nil
			}
			return fmt.Errorf("decode manifest edit: %w", err)
		}
		var edit version.Edit
		if err := json.Unmarshal(payload, &edit); err != nil {
			return fmt.Errorf("unmarshal manifest edit: %w", err)
		}
		s.state = s.state.Apply(edit)
		offset += consumed
	}
	return nil
}

// readCurrent 读取 CURRENT 文件中的 MANIFEST 文件名
func readCurrent(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, currentFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read current manifest: %w", err)
	}
	return string(data), nil
}

// writeCurrent 以原子方式将 MANIFEST 文件名写入 CURRENT 文件
// 通过先写临时文件再 rename 实现原子替换，避免崩溃导致 CURRENT 损坏
func writeCurrent(dir, name string) error {
	path := filepath.Join(dir, currentFile)
	// 在同目录下创建临时文件，保证 rename 是原子操作
	tmp, err := os.CreateTemp(dir, currentFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create current manifest temp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	// 写入内容并刷盘
	if _, err := tmp.Write([]byte(name)); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write current manifest temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync current manifest temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close current manifest temp: %w", err)
	}
	// 原子替换 CURRENT 文件
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename current manifest: %w", err)
	}
	cleanup = false
	// 同步目录，确保 rename 操作落盘
	return syncDir(dir)
}

// manifestName 根据文件编号生成 MANIFEST 文件名
func manifestName(fileNum uint64) string {
	return fmt.Sprintf(manifestFmt, fileNum)
}

// syncDir 对目录执行 fsync，确保元数据（如文件创建/重命名）持久化
func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open manifest dir: %w", err)
	}
	defer func() { _ = file.Close() }()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync manifest dir: %w", err)
	}
	return nil
}