package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const lockFileName = "LOCK"

// directoryLocks 是进程内全局目录锁注册表，防止同一进程多次打开同一目录
var directoryLocks = struct {
	sync.Mutex
	held map[string]struct{} // 记录当前已被锁定的目录绝对路径
}{
	held: make(map[string]struct{}),
}

// directoryLock 表示一个已获取的目录锁
type directoryLock struct {
	path string   // 锁定目录的绝对路径
	file *os.File // LOCK 文件的句柄，用于持有 flock
}

// acquireDirectoryLock 尝试获取指定目录的排他锁
// 成功时返回 directoryLock，调用者应在引擎关闭时调用 Release
// 如果目录已被其他实例或同一进程锁定，则返回 ErrLocked
func acquireDirectoryLock(dir string) (*directoryLock, error) {
	// 确保目录存在
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create lsm dir: %w", err)
	}
	// 转为绝对路径，保证唯一性
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve lsm dir: %w", err)
	}

	// 进程内互斥：防止同一进程多次打开同一目录
	directoryLocks.Lock()
	if _, ok := directoryLocks.held[abs]; ok {
		directoryLocks.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrLocked, abs)
	}
	directoryLocks.held[abs] = struct{}{} // 占位
	directoryLocks.Unlock()

	// 打开或创建 LOCK 文件
	file, err := os.OpenFile(filepath.Join(abs, lockFileName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		releaseDirectoryLockPath(abs) // 回滚进程内记录
		return nil, fmt.Errorf("open lsm lock: %w", err)
	}
	// 尝试获取排他文件锁（非阻塞）
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		releaseDirectoryLockPath(abs)
		// 如果是锁冲突，包装为 ErrLocked
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, abs)
		}
		return nil, fmt.Errorf("lock lsm dir: %w", err)
	}
	return &directoryLock{path: abs, file: file}, nil
}

// releaseDirectoryLockPath 从进程内注册表中移除指定路径，通常在锁释放或获取失败时调用
func releaseDirectoryLockPath(path string) {
	directoryLocks.Lock()
	delete(directoryLocks.held, path)
	directoryLocks.Unlock()
}

// Release 释放目录锁，关闭 LOCK 文件，并从进程内注册表移除
func (l *directoryLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	// 解除 flock
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	releaseDirectoryLockPath(l.path)
	return errors.Join(err, closeErr)
}