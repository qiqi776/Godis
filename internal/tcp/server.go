package tcp

import (
	"errors"
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
	listener net.Listener
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
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Error("Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// Stop 关闭监听器
func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// handleConnection 处理每个客户端连接
func (s *Server) handleConnection(conn net.Conn) {
	atomic.AddInt64(&s.db.GetStats().ConnectedClients, 1)
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.db.GetStats().ConnectedClients, -1)
	}()

	clientConn := core.NewConnection()

	reader := protocol.NewReader(conn)
	writer := protocol.NewWriter(conn)

	for {
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

		cmdName := strings.ToUpper(string(payload.Array[0].Bulk))
		args := payload.Array[1:]

		atomic.AddInt64(&s.db.GetStats().TotalCommandsProcessed, 1)

		ctx := &core.Context{
			Args: args,
			DB:   s.db,
			Conn: clientConn,
		}

		result := commands.Execute(cmdName, ctx)
		if err := writer.Write(result); err != nil {
			logger.Error("Write response error: %v", err)
			return
		}
	}
}