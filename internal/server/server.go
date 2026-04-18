package server

import (
	"context"
	"errors"
	"net"
	"sync"

	"godis/internal/command"
	"godis/internal/common/logger"
	"godis/internal/config"
)

type Server struct {
	cfg      config.Config
	logger   *logger.Logger
	executor *command.Executor
	mu       sync.RWMutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
}

func New(cfg config.Config, l *logger.Logger, e *command.Executor) *Server {
	return &Server{
		cfg:      cfg,
		logger:   l,
		executor: e,
		conns:    make(map[net.Conn]struct{}),
	}
}

func (s *Server) Run(ctx context.Context) error {
	listen, err := net.Listen("tcp", s.cfg.Address())
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = listen
	s.mu.Unlock()

	s.logger.Infof("godis listening on %s", listen.Addr().String())

	go func() {
		<-ctx.Done()
		s.logger.Infof("shutdown signal received")
		_ = s.Close()
	}()

	for {
		conn, err := listen.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			s.logger.Errorf("accept error: %v", err)
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConn(c)
		}(conn)
	}

	s.wg.Wait()
	return nil
}

func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.conns {
		_ = conn.Close()
		delete(s.conns, conn)
	}

	if s.listener == nil {
		return nil
	}
	return s.listener.Close()
}

func (s *Server) registerConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[conn] = struct{}{}
}

func (s *Server) unregisterConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
}
