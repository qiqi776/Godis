package server

import (
	"godis/internal/config"
	"godis/pkg/logger"
	"net"
)

type Server struct {
	config   *config.Config
	listener net.Listener
	// 这里以后会加入 storage 引擎
}

func NewServer(cfg *config.Config) *Server {
	return &Server{config: cfg}
}

func (s *Server) Start() {
	// 监听端口
	addr := ":" + s.config.Port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}
	s.listener = listener
	logger.Info("Godis listening on %s", addr)

	for {
		// 接受连接 (Accept)
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Accept error: %v", err)
			continue
		}

		// 为每个连接启动一个 Goroutine (替代 IO 多路复用)
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	logger.Debug("New connection from %s", conn.RemoteAddr())

	buf := make([]byte, 1024)
	for {
		// 读取数据
		n, err := conn.Read(buf)
		if err != nil {
			logger.Debug("Connection closed: %s", conn.RemoteAddr())
			return
		}

		// TODO: 这里接入 RESP 协议解析器 (Phase 2)
		// 目前先做 Echo
		data := buf[:n]
		logger.Debug("Received: %s", string(data))

		_, _ = conn.Write(data)
	}
}