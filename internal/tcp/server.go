package tcp

import (
	"godis/internal/commands"
	"godis/internal/config"
	"godis/internal/core"
	"godis/pkg/logger"
	"godis/pkg/protocol"
	"io"
	"net"
	"strings"
	"sync/atomic"
)
type Server struct {
	config *config.Config
	db      core.KVStorage
}

func NewServer(cfg *config.Config, db core.KVStorage) *Server {
	return &Server{
		config: cfg,
		db:     db,
	}
}

func (s *Server) Start() {
	addr := ":" + s.config.Port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}
	logger.Info("Godis listening on %s", addr)
	logger.Info("AOF enabled: %v, Strategy: %s", s.config.AppendOnly, s.config.AppendFsync)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	atomic.AddInt64(&s.db.GetStats().ConnectedClients, 1)
	defer func() {
		conn.Close()
		// 连接断开，计数 -1
		atomic.AddInt64(&s.db.GetStats().ConnectedClients, -1)
	}()

	defer conn.Close()

	reader := protocol.NewReader(conn)
	writer := protocol.NewWriter(conn)

	for {
		// 读取解析,ReadValue 会阻塞直到读到一个完整的RESP报文，从而解决粘包问题
		payload, err := reader.ReadValue()
		
		if err != nil {
			if err != io.EOF {
				logger.Error("Parse error: %v", err)
			}
			return
		}

		if payload.Type != protocol.Array || len(payload.Array) == 0 {
			continue
		}

		// 提取命令
		cmdName := strings.ToUpper(string(payload.Array[0].Bulk))
		args := payload.Array[1:]
		
		atomic.AddInt64(&s.db.GetStats().TotalCommandsProcessed, 1)

		ctx := &core.Context{
			Args: args,
			DB:   s.db,
		}

		result := commands.Execute(cmdName, ctx)
		// 发送响应
		if err := writer.Write(result); err != nil {
			logger.Error("Write response error: %v", err)
			return
		}
	}
}

func (s *Server) Stop() {
	// 实现优雅关闭逻辑
}