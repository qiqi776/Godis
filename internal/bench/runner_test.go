package bench

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	minikvv1 "mini-kv/api/minikv/v1"
)

type testKVServer struct {
	minikvv1.UnimplementedKVServer

	mu              sync.RWMutex
	values          map[string][]byte
	rejectNotLeader bool
}

func newTestKVServer() *testKVServer {
	return &testKVServer{
		values: make(map[string][]byte),
	}
}

func (s *testKVServer) Get(_ context.Context, req *minikvv1.GetRequest) (*minikvv1.GetResponse, error) {
	if s.rejectNotLeader {
		return nil, errors.New("not leader; leader=node1")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.values[req.GetKey()]
	return &minikvv1.GetResponse{
		Value: append([]byte(nil), value...),
		Found: ok,
	}, nil
}

func (s *testKVServer) Set(_ context.Context, req *minikvv1.SetRequest) (*minikvv1.SetResponse, error) {
	if s.rejectNotLeader {
		return nil, errors.New("not leader; leader=node1")
	}

	s.mu.Lock()
	s.values[req.GetKey()] = append([]byte(nil), req.GetValue()...)
	s.mu.Unlock()
	return &minikvv1.SetResponse{}, nil
}

func (s *testKVServer) Delete(_ context.Context, req *minikvv1.DeleteRequest) (*minikvv1.DeleteResponse, error) {
	if s.rejectNotLeader {
		return nil, errors.New("not leader; leader=node1")
	}

	s.mu.Lock()
	delete(s.values, req.GetKey())
	s.mu.Unlock()
	return &minikvv1.DeleteResponse{}, nil
}

func TestRunSet(t *testing.T) {
	t.Parallel()

	endpoint, cleanup := startTestServer(t, newTestKVServer())
	defer cleanup()

	result, err := Run(context.Background(), Config{
		Endpoints:      []string{endpoint},
		Routing:        RoutingLeader,
		Mode:           ModeSet,
		Concurrency:    4,
		Duration:       120 * time.Millisecond,
		RequestTimeout: time.Second,
		ConnectTimeout: time.Second,
		Keyspace:       16,
		ValueSize:      32,
		Seed:           1,
	})
	if err != nil {
		t.Fatalf("run set bench: %v", err)
	}

	if result.Totals.Success == 0 {
		t.Fatalf("success count = 0, want > 0")
	}
	if result.Totals.Errors != 0 {
		t.Fatalf("error count = %d, want 0", result.Totals.Errors)
	}
}

func TestRunMixedPreload(t *testing.T) {
	t.Parallel()

	endpoint, cleanup := startTestServer(t, newTestKVServer())
	defer cleanup()

	result, err := Run(context.Background(), Config{
		Endpoints:      []string{endpoint},
		Routing:        RoutingLeader,
		Mode:           ModeMixed,
		Concurrency:    3,
		Duration:       120 * time.Millisecond,
		RequestTimeout: time.Second,
		ConnectTimeout: time.Second,
		Keyspace:       12,
		ValueSize:      16,
		ReadPercent:    70,
		WritePercent:   30,
		Seed:           7,
		PreloadKeys:    -1,
	})
	if err != nil {
		t.Fatalf("run mixed bench: %v", err)
	}

	getStats, ok := result.Operations[string(ModeGet)]
	if !ok {
		t.Fatalf("missing get stats")
	}
	if getStats.Total == 0 {
		t.Fatalf("get total = 0, want > 0")
	}
	if getStats.Found == 0 {
		t.Fatalf("get found = 0, want > 0")
	}
}

func TestLeaderAutoDetect(t *testing.T) {
	t.Parallel()

	follower := newTestKVServer()
	follower.rejectNotLeader = true
	followerEndpoint, followerCleanup := startTestServer(t, follower)
	defer followerCleanup()

	leaderEndpoint, leaderCleanup := startTestServer(t, newTestKVServer())
	defer leaderCleanup()

	result, err := Run(context.Background(), Config{
		Endpoints:      []string{followerEndpoint, leaderEndpoint},
		Routing:        RoutingLeader,
		Mode:           ModeSet,
		Concurrency:    2,
		Duration:       120 * time.Millisecond,
		RequestTimeout: time.Second,
		ConnectTimeout: time.Second,
		Keyspace:       8,
		ValueSize:      8,
		Seed:           11,
	})
	if err != nil {
		t.Fatalf("run leader detect bench: %v", err)
	}

	if result.LeaderEndpoint != leaderEndpoint {
		t.Fatalf("leader endpoint = %s, want %s", result.LeaderEndpoint, leaderEndpoint)
	}
	if result.Totals.Success == 0 {
		t.Fatalf("success count = 0, want > 0")
	}
}

func startTestServer(t *testing.T, server minikvv1.KVServer) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	minikvv1.RegisterKVServer(grpcServer, server)

	go func() {
		if serveErr := grpcServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			panic(serveErr)
		}
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
	}

	return listener.Addr().String(), cleanup
}
