package server

import (
	"godis/internal/config"
	"godis/internal/db"
	"godis/pkg/logger"
	"godis/pkg/protocol"
	"io"
	"net"
)

type Server struct {
	config *config.Config
	db     *db.Database
}

func NewServer(cfg *config.Config, database *db.Database) *Server {
	return &Server{
		config: cfg,
		db:     database,
	}
}

func (s *Server) Start() {
	addr := ":" + s.config.Port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}
	logger.Info("Godis listening on %s", addr)

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
	defer conn.Close()

	reader := protocol.NewReader(conn)
	writer := protocol.NewWriter(conn)

	for {
		// 读取解析,ReadValue 会阻塞直到读到一个完整的RESP报文，从而解决粘包问题
		payload, err := reader.ReadValue()
		
		if err != nil {
			if err == io.EOF {
				break // 客户端关闭连接
			}
			logger.Error("Parse error: %v", err)
			return
		}

		// 执行逻辑
		result := s.db.Exec(payload)

		// 发送响应
		err = writer.Write(result)
		if err != nil {
			logger.Error("Write error: %v", err)
			break
		}
	}
}

func (s *Server) Stop() {
	// 实现优雅关闭逻辑
}