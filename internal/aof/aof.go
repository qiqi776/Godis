package aof

import (
	"godis/pkg/logger"
	"godis/pkg/protocol"
	"io"
	"os"
	"sync"
	"time"
)

const (
	FsyncAlways   = "always"
	FsyncEverySec = "everysec"
	FsyncNo   	  = "no"
)

type Aof struct {
	file *os.File
	mu    sync.Mutex
	strategy string			// 当前使用的刷盘策略
	quitChan chan struct{}	// 用于通知后台协程退出
	wg       sync.WaitGroup
}

// 创建AOF处理器,启动刷盘策略
func NewAof(path string, strategy string) (*Aof, error) {
	if strategy == "" {
		strategy = FsyncEverySec
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: 	  f,
		strategy: strategy,
		quitChan: make(chan struct{}),
	}

	if strategy == FsyncEverySec {
		aof.wg.Add(1)
		go aof.bgFsync()
	}
	return aof, nil
}

// 将原始命令字节写入AOF缓冲区
func (a *Aof) Write(payload []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Write(payload)
	if err != nil {
		return err
	}
	
	// 根据策略决定是否立即落盘
	if a.strategy == FsyncAlways {
		return a.file.Sync()
	}

	return nil
}

// 后台定时刷盘协程
func (a *Aof) bgFsync() {
	defer a.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定时触发刷盘
			a.Fsync()
		case <-a.quitChan:
			return
		}
	}
}

// 主动触发刷盘
func (a *Aof) Fsync() {
    a.mu.Lock()
    defer a.mu.Unlock()

    if err := a.file.Sync(); err != nil {
        logger.Error("AOF fsync failed: %v", err)
    }
}

// 关闭文件
func (a *Aof) Close() error {
	if a.strategy == FsyncEverySec {
		close(a.quitChan)
		a.wg.Wait()
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	// 关闭前强制执行最后一次刷盘，确保数据不丢失
	if err := a.file.Sync(); err != nil {
		logger.Error("Final AOF sync failed: %v", err)
	}
	return a.file.Close()
}

// 读取AOF文件进行重放(用于启动时恢复数据)
func (a *Aof) Read(fn func(value protocol.Value)) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Seek(0, 0)
	if err != nil {
		return err
	}

	reader := protocol.NewReader(a.file)
	for {
		val, err := reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Error("AOF parse error: %v", err)
			return err
		}
		fn(val)
	}
	return nil
}