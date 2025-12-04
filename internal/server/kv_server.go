package server

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"godis/internal/config"
	"godis/pkg/logger"
)

// KVServer 结构体
type KVServer struct {
	config   *config.Config
	listener net.Listener
	mu       sync.RWMutex
	db       map[string]string
}

// 创建kvserver实例
func NewKVServer(cfg *config.Config) *KVServer {
	return &KVServer{
		config: cfg,
		db: make(map[string]string),
	}
}

// 启动服务器
func (s *KVServer) Start() {
	addr := ":" + s.config.Port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}
	s.listener = listener
	logger.Info("Godis listening on %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Accept error: %v", err)
			continue
		}
		// 为每个连接启动一个 Goroutine
		go s.handleConnection(conn)
	}
}

// 停止服务器
func (s *KVServer) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// 处理单个客户端连接
func (s *KVServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	logger.Debug("New connection from %s", conn.RemoteAddr())

	buf := make([]byte, 1024)

	for {
		// 读取数据
		n, err := conn.Read(buf)
		if err != nil {
			break
		}

		// 获取请求字符串
		reqData := buf[:n]
		reqStr := string(reqData)

		// 清理末尾换行符
		reqStr = strings.TrimSpace(reqStr)
		if len(reqStr) == 0 {
			continue
		}

		// 解析指令
		tokens := strings.Fields(reqStr)
		if len(tokens) == 0 {
			continue
		}

		command := strings.ToUpper(tokens[0]) // 忽略大小写
		var res string

		// 执行命令
		switch command {
		case "GET":
			if len(tokens) == 2 {
				key := tokens[1]
				
				s.mu.RLock()
				val, exists := s.db[key]
				s.mu.RUnlock()

				if exists {
					res = fmt.Sprintf("\"%s\"\n", val)
				} else {
					res = "(nil)\n"
				}
			} else {
				res = "Error: wrong number of arguments for 'GET' command\n"
			}
		case "SET":
			if len(tokens) == 3 {
				key := tokens[1]
				val := tokens[2]

				s.mu.Lock()
				s.db[key] = val
				s.mu.Unlock()

				res = "OK\n"
			} else {
				res = "Error: wrong number of arguments for 'SET' command\n"
			}
		default:
			res = fmt.Sprintf("Error: unknown command '%s'\n", command)
		}

		// 发送响应
		_, err = conn.Write([]byte(res))
		if err != nil {
			break
		}
	}
	logger.Debug("Client disconnected: %s", conn.RemoteAddr())
}