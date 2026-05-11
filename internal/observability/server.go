package observability

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"mini-kv/internal/config"
	"mini-kv/internal/logger"
)

type Server struct {
	cfg      config.DebugConfig
	logger   *logger.Logger
	registry *Registry
	server   *http.Server
}

func NewServer(cfg config.DebugConfig, logger *logger.Logger, registry *Registry) *Server {
	return &Server{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
	}
}

func (s *Server) Run(ctx context.Context) error {
	if s == nil || !s.cfg.Enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/debug/vars", s.handleVars)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{
		Addr:              s.cfg.Address(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.server = server

	s.logger.Infof("mini-kv debug listening on %s", server.Addr)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	err := server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) handleMetrics(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = writer.Write([]byte(s.registry.RenderPrometheus()))
}

func (s *Server) handleVars(writer http.ResponseWriter, _ *http.Request) {
	data, err := s.registry.SnapshotJSON()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = writer.Write(data)
}
