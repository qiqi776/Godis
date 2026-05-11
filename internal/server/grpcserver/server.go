package grpcserver

import (
	"context"
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc"
	minikvv1 "mini-kv/api/minikv/v1"
	"mini-kv/internal/config"
	"mini-kv/internal/logger"
	"mini-kv/internal/observability"
	"mini-kv/internal/service/minikv"
)

type Server struct {
	cfg      config.Config
	logger   *logger.Logger
	service  minikv.Service
	registry *observability.Registry

	mu         sync.RWMutex
	listener   net.Listener
	grpcServer *grpc.Server
}

func New(cfg config.Config, l *logger.Logger, service minikv.Service, registry *observability.Registry) *Server {
	return &Server{
		cfg:      cfg,
		logger:   l,
		service:  service,
		registry: registry,
	}
}

func (s *Server) Run(ctx context.Context) error {
	listen, err := net.Listen("tcp", s.cfg.Address())
	if err != nil {
		return err
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(observability.UnaryServerInterceptor(s.registry)))
	minikvv1.RegisterKVServer(server, newKVHandler(s.service))

	s.mu.Lock()
	s.listener = listen
	s.grpcServer = server
	s.mu.Unlock()

	s.logger.Infof("mini-kv gRPC listening on %s", listen.Addr().String())

	go func() {
		<-ctx.Done()
		s.logger.Infof("shutdown signal received")
		s.Close()
	}()

	err = server.Serve(listen)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Close() {
	s.mu.Lock()
	server := s.grpcServer
	listener := s.listener
	s.grpcServer = nil
	s.listener = nil
	s.mu.Unlock()

	if server != nil {
		server.GracefulStop()
	}
	if listener != nil {
		_ = listener.Close()
	}
}
