//go:build integration

package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"mini-kv/internal/bench"
)

func TestLSMBackedAppBenchMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	endpoints := startLSMCluster(t)
	waitForBenchReady(t, endpoints)

	tests := []bench.Config{
		{
			Label:       "set-small-1",
			Mode:        bench.ModeSet,
			Routing:     bench.RoutingLeader,
			Concurrency: 1,
			Keyspace:    128,
			ValueSize:   64,
			Seed:        1,
		},
		{
			Label:       "set-small-16",
			Mode:        bench.ModeSet,
			Routing:     bench.RoutingLeader,
			Concurrency: 16,
			Keyspace:    256,
			ValueSize:   64,
			Seed:        2,
		},
		{
			Label:       "get-small-16",
			Mode:        bench.ModeGet,
			Routing:     bench.RoutingLeader,
			Concurrency: 16,
			Keyspace:    256,
			ValueSize:   64,
			PreloadKeys: 256,
			Seed:        3,
		},
		{
			Label:         "mixed-medium-32",
			Mode:          bench.ModeMixed,
			Routing:       bench.RoutingLeader,
			Concurrency:   32,
			Keyspace:      256,
			ValueSize:     256,
			PreloadKeys:   256,
			ReadPercent:   70,
			WritePercent:  25,
			DeletePercent: 5,
			Seed:          4,
		},
		{
			Label:       "delete-medium-16",
			Mode:        bench.ModeDelete,
			Routing:     bench.RoutingLeader,
			Concurrency: 16,
			Keyspace:    256,
			ValueSize:   256,
			PreloadKeys: 256,
			Seed:        5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Label, func(t *testing.T) {
			tt.Endpoints = endpoints
			tt.Duration = time.Second
			tt.Warmup = 250 * time.Millisecond
			tt.RequestTimeout = 5 * time.Second
			tt.ConnectTimeout = 2 * time.Second

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			result, err := bench.Run(ctx, tt)
			if err != nil {
				t.Fatalf("bench.Run error = %v", err)
			}
			if result.Totals.Success == 0 {
				t.Fatal("bench completed without successful operations")
			}
			if result.Totals.Errors != 0 {
				t.Fatalf("bench reported %d errors: %+v", result.Totals.Errors, result.ErrorSamples)
			}
			t.Logf("%s: qps=%.1f p99=%.2fms leader=%s ops=%d",
				result.Label,
				result.Totals.SuccessQPS,
				result.Latency.P99MS,
				result.LeaderEndpoint,
				result.Totals.Success,
			)
		})
	}
}

func startLSMCluster(t *testing.T) []string {
	t.Helper()

	root := t.TempDir()
	grpcPorts := []int{freePort(t), freePort(t), freePort(t)}
	raftPorts := []int{freePort(t), freePort(t), freePort(t)}
	endpoints := make([]string, 3)
	peerAddrs := make([]string, 3)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("127.0.0.1:%d", grpcPorts[i])
		peerAddrs[i] = fmt.Sprintf("127.0.0.1:%d", raftPorts[i])
	}

	apps := make([]*App, 0, 3)
	for i := 0; i < 3; i++ {
		cfgPath := writeClusterConfig(t, root, i+1, grpcPorts[i], peerAddrs)
		instance, err := Start(cfgPath)
		if err != nil {
			for _, opened := range apps {
				_ = opened.KVStore.Close()
				_ = opened.RaftTransport.Close()
				_ = opened.RaftStorage.Close()
				_ = opened.RaftNode.Stop()
			}
			t.Fatalf("Start node%d error = %v", i+1, err)
		}
		apps = append(apps, instance)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errCh := make(chan error, len(apps))
	for _, instance := range apps {
		wg.Add(1)
		go func(instance *App) {
			defer wg.Done()
			if err := instance.Run(ctx); err != nil {
				errCh <- fmt.Errorf("%s run: %w", instance.Config.Raft.ID, err)
			}
		}(instance)
	}

	t.Cleanup(func() {
		cancel()
		doneCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneCh)
		}()
		select {
		case <-doneCh:
		case <-time.After(5 * time.Second):
			t.Error("cluster shutdown timed out")
		}
	})

	for _, endpoint := range endpoints {
		waitForTCP(t, endpoint, errCh)
	}
	return endpoints
}

func writeClusterConfig(t *testing.T, root string, node int, grpcPort int, peerAddrs []string) string {
	t.Helper()

	nodeID := fmt.Sprintf("node%d", node)
	nodeDir := filepath.Join(root, nodeID)
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatalf("mkdir node dir: %v", err)
	}

	configPath := filepath.Join(nodeDir, "config.yaml")
	data := fmt.Sprintf(`host: 127.0.0.1
port: %d
log_level: error
debug:
  enabled: false
  host: 127.0.0.1
  port: 0

storage:
  lsm_path: %s

raft:
  id: %s
  peers: [node1, node2, node3]
  listen_addr: %s
  peer_addrs:
    node1: %s
    node2: %s
    node3: %s
  wal_path: %s
  election_timeout_ms: 150
  heartbeat_timeout_ms: 50
  apply_buffer_size: 512
  snapshot_threshold: 64
`,
		grpcPort,
		filepath.Join(nodeDir, "lsm"),
		nodeID,
		peerAddrs[node-1],
		peerAddrs[0],
		peerAddrs[1],
		peerAddrs[2],
		filepath.Join(nodeDir, "raft.wal"),
	)
	if err := os.WriteFile(configPath, []byte(data), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return configPath
}

func waitForBenchReady(t *testing.T, endpoints []string) {
	t.Helper()

	cfg := bench.Config{
		Label:          "ready",
		Endpoints:      endpoints,
		Routing:        bench.RoutingLeader,
		Mode:           bench.ModeSet,
		Concurrency:    1,
		Duration:       100 * time.Millisecond,
		RequestTimeout: 500 * time.Millisecond,
		ConnectTimeout: time.Second,
		Keyspace:       1,
		ValueSize:      1,
		Seed:           99,
	}
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, err := bench.Run(ctx, cfg)
		cancel()
		if err == nil && result.Totals.Success > 0 && result.Totals.Errors == 0 {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("ready probe had %d errors", result.Totals.Errors)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("cluster did not become bench-ready: %v", lastErr)
}

func waitForTCP(t *testing.T, address string, errCh <-chan error) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("app exited before TCP readiness: %v", err)
		default:
		}

		conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", address)
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on free port: %v", err)
	}
	defer func() { _ = listener.Close() }()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("unexpected listener addr %T", listener.Addr())
	}
	return addr.Port
}
