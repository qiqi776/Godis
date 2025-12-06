package aof

import (
	"godis/pkg/logger"
	"godis/pkg/protocol"
	"io"
	"os"
	"sync"
)

type Aof struct {
	file *os.File
	mu    sync.Mutex
}

// 打开或创建文件
func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &Aof{file: f}, nil
}

// 将原始命令字节写入文件
func (a *Aof) Write(payload []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Write(payload)
	if err != nil {
		return err
	}
	// 简单起见，每次写入都落盘 (相当于 appendfsync always)
    // 优化点：后续可以使用 bufio 和定时 flush 实现 everysec
	return a.file.Sync()
}

// 关闭文件
func (a *Aof) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

//读取文件并回调处理每一条命令
func (a *Aof) Read(fn func(value protocol.Value)) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Seek(0, 0)
	if err != nil {
		return nil
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