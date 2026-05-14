package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	minikvv1 "mini-kv/api/minikv/v1"
)

type Runner struct {
	cfg     Config
	conns   []*grpc.ClientConn
	router  *router
	keys    []string
	value   []byte
	weights operationWeights
}

type workerResult struct {
	latencies []int64
	errors    map[string]uint64
	ops       map[Mode]workerOperationSummary
}

type workerOperationSummary struct {
	Total   uint64
	Success uint64
	Errors  uint64
	Found   uint64
	Missing uint64
}

type operationWeights struct {
	readLimit   int
	writeLimit  int
	deleteLimit int
}

type router struct {
	endpoints      []string
	clients        []minikvv1.KVClient
	routing        Routing
	requestTimeout time.Duration
	leaderIndex    atomic.Uint64
	rrIndex        atomic.Uint64
	refreshCount   atomic.Uint64

	mu            sync.Mutex
	lastRefreshAt time.Time
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	runner, err := NewRunner(cfg)
	if err != nil {
		return Result{}, err
	}
	defer runner.Close()

	return runner.Run(ctx)
}

func NewRunner(cfg Config) (*Runner, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	conns := make([]*grpc.ClientConn, 0, len(cfg.Endpoints))
	clients := make([]minikvv1.KVClient, 0, len(cfg.Endpoints))
	for _, endpoint := range cfg.Endpoints {
		dialCtx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
		conn, err := grpc.DialContext(
			dialCtx,
			endpoint,
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		cancel()
		if err != nil {
			for _, opened := range conns {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("dial %s: %w", endpoint, err)
		}
		conns = append(conns, conn)
		clients = append(clients, minikvv1.NewKVClient(conn))
	}

	keys := make([]string, cfg.Keyspace)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:%08d", i)
	}

	rt := &router{
		endpoints:      append([]string(nil), cfg.Endpoints...),
		clients:        clients,
		routing:        cfg.Routing,
		requestTimeout: cfg.RequestTimeout,
	}

	if cfg.Routing == RoutingLeader {
		if err := rt.initLeader(cfg.LeaderEndpoint); err != nil {
			for _, opened := range conns {
				_ = opened.Close()
			}
			return nil, err
		}
	}

	return &Runner{
		cfg:     cfg,
		conns:   conns,
		router:  rt,
		keys:    keys,
		value:   bytes.Repeat([]byte("x"), cfg.ValueSize),
		weights: newOperationWeights(cfg),
	}, nil
}

func (r *Runner) Close() {
	for _, conn := range r.conns {
		_ = conn.Close()
	}
}

func (r *Runner) Run(ctx context.Context) (Result, error) {
	if r.cfg.PreloadKeys > 0 {
		if err := r.preload(ctx); err != nil {
			return Result{}, err
		}
	}

	if r.cfg.Warmup > 0 {
		if _, err := r.execute(ctx, r.cfg.Warmup); err != nil {
			return Result{}, err
		}
	}

	startedAt := time.Now()
	measurement, err := r.execute(ctx, r.cfg.Duration)
	if err != nil {
		return Result{}, err
	}
	endedAt := time.Now()

	seconds := endedAt.Sub(startedAt).Seconds()
	result := Result{
		Label:          r.cfg.Label,
		StartedAt:      startedAt,
		EndedAt:        endedAt,
		Duration:       endedAt.Sub(startedAt).String(),
		Warmup:         r.cfg.Warmup.String(),
		Mode:           string(r.cfg.Mode),
		Routing:        string(r.cfg.Routing),
		Endpoints:      append([]string(nil), r.cfg.Endpoints...),
		LeaderEndpoint: r.router.leaderEndpoint(),
		Concurrency:    r.cfg.Concurrency,
		Keyspace:       r.cfg.Keyspace,
		ValueSize:      r.cfg.ValueSize,
		PreloadKeys:    r.cfg.PreloadKeys,
		Seed:           r.cfg.Seed,
		LeaderRefresh:  r.router.refreshCount.Load(),
		Operations:     make(map[string]OperationSummary, 4),
		Latency:        summarizeLatencies(measurement.latencies),
		ErrorSamples:   measurement.errors,
	}

	var totals workerOperationSummary
	for mode, summary := range measurement.ops {
		result.Operations[string(mode)] = summarizeOperation(summary, seconds)
		totals.Total += summary.Total
		totals.Success += summary.Success
		totals.Errors += summary.Errors
		totals.Found += summary.Found
		totals.Missing += summary.Missing
	}
	result.Totals = summarizeOperation(totals, seconds)

	return result, nil
}

func (r *Runner) preload(ctx context.Context) error {
	client, _, err := r.router.leaderClient(ctx)
	if err != nil {
		return err
	}

	for i := 0; i < r.cfg.PreloadKeys; i++ {
		reqCtx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
		_, callErr := client.Set(reqCtx, &minikvv1.SetRequest{
			Key:   r.keys[i],
			Value: r.value,
		})
		cancel()
		if callErr != nil {
			return fmt.Errorf("preload key %d: %w", i, callErr)
		}
	}
	return nil
}

func (r *Runner) execute(ctx context.Context, duration time.Duration) (workerResult, error) {
	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	resultsCh := make(chan workerResult, r.cfg.Concurrency)
	var waitGroup sync.WaitGroup

	for workerIndex := 0; workerIndex < r.cfg.Concurrency; workerIndex++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			resultsCh <- r.runWorker(runCtx, index)
		}(workerIndex)
	}

	waitGroup.Wait()
	close(resultsCh)

	merged := workerResult{
		errors: make(map[string]uint64),
		ops:    make(map[Mode]workerOperationSummary, 4),
	}
	for result := range resultsCh {
		merged.latencies = append(merged.latencies, result.latencies...)
		for label, count := range result.errors {
			merged.errors[label] += count
		}
		for mode, summary := range result.ops {
			current := merged.ops[mode]
			current.Total += summary.Total
			current.Success += summary.Success
			current.Errors += summary.Errors
			current.Found += summary.Found
			current.Missing += summary.Missing
			merged.ops[mode] = current
		}
	}

	return merged, nil
}

func (r *Runner) runWorker(ctx context.Context, workerIndex int) workerResult {
	local := workerResult{
		errors: make(map[string]uint64),
		ops:    make(map[Mode]workerOperationSummary, 4),
	}
	random := rand.New(rand.NewSource(r.cfg.Seed + int64(workerIndex) + 1))

	for {
		select {
		case <-ctx.Done():
			return local
		default:
		}

		mode := r.pickMode(random)
		key := r.keys[random.Intn(len(r.keys))]
		startedAt := time.Now()
		found, err := r.call(ctx, mode, key)
		latency := time.Since(startedAt)

		if shouldIgnoreCancellation(ctx, err) {
			return local
		}

		summary := local.ops[mode]
		summary.Total++
		local.latencies = append(local.latencies, latency.Nanoseconds())

		if err != nil {
			summary.Errors++
			label := classifyError(err)
			local.errors[label]++
			local.ops[mode] = summary
			if isNotLeaderError(err) {
				_, _ = r.router.refreshLeader(context.Background())
			}
			continue
		}

		summary.Success++
		if mode == ModeGet {
			if found {
				summary.Found++
			} else {
				summary.Missing++
			}
		}
		local.ops[mode] = summary
	}
}

func (r *Runner) call(ctx context.Context, mode Mode, key string) (bool, error) {
	client := r.router.client()
	reqCtx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	defer cancel()

	switch mode {
	case ModeSet:
		_, err := client.Set(reqCtx, &minikvv1.SetRequest{
			Key:   key,
			Value: r.value,
		})
		return false, err
	case ModeGet:
		resp, err := client.Get(reqCtx, &minikvv1.GetRequest{Key: key})
		if err != nil {
			return false, err
		}
		return resp.GetFound(), nil
	case ModeDelete:
		_, err := client.Delete(reqCtx, &minikvv1.DeleteRequest{Key: key})
		return false, err
	default:
		return false, fmt.Errorf("unsupported mode %q", mode)
	}
}

func (r *Runner) pickMode(random *rand.Rand) Mode {
	switch r.cfg.Mode {
	case ModeSet, ModeGet, ModeDelete:
		return r.cfg.Mode
	case ModeMixed:
		roll := random.Intn(100)
		switch {
		case roll < r.weights.readLimit:
			return ModeGet
		case roll < r.weights.writeLimit:
			return ModeSet
		default:
			return ModeDelete
		}
	default:
		return ModeSet
	}
}

func newOperationWeights(cfg Config) operationWeights {
	return operationWeights{
		readLimit:   cfg.ReadPercent,
		writeLimit:  cfg.ReadPercent + cfg.WritePercent,
		deleteLimit: 100,
	}
}

func shouldIgnoreCancellation(ctx context.Context, err error) bool {
	if err == nil || !isCancellationError(err) {
		return false
	}
	if ctx.Err() != nil {
		return true
	}

	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

func isCancellationError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if isNotLeaderError(err) {
		return "not_leader"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if message == "" {
		return "unknown"
	}
	return message
}

func isNotLeaderError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not leader")
}

func (r *router) initLeader(explicit string) error {
	if explicit != "" {
		for index, endpoint := range r.endpoints {
			if endpoint == explicit {
				r.leaderIndex.Store(uint64(index))
				return nil
			}
		}
		return fmt.Errorf("leader endpoint %q is not present in endpoints", explicit)
	}

	_, err := r.refreshLeader(context.Background())
	return err
}

func (r *router) client() minikvv1.KVClient {
	if r.routing == RoutingRoundRobin {
		index := int(r.rrIndex.Add(1)-1) % len(r.clients)
		return r.clients[index]
	}
	index := int(r.leaderIndex.Load())
	return r.clients[index]
}

func (r *router) leaderClient(ctx context.Context) (minikvv1.KVClient, string, error) {
	if r.routing != RoutingLeader {
		return r.client(), "", nil
	}
	if len(r.clients) == 0 {
		return nil, "", fmt.Errorf("no clients configured")
	}
	if len(r.clients) == 1 {
		return r.clients[0], r.endpoints[0], nil
	}
	endpoint, err := r.refreshLeader(ctx)
	if err != nil {
		return nil, "", err
	}
	index := int(r.leaderIndex.Load())
	return r.clients[index], endpoint, nil
}

func (r *router) leaderEndpoint() string {
	if r.routing != RoutingLeader || len(r.endpoints) == 0 {
		return ""
	}
	return r.endpoints[int(r.leaderIndex.Load())]
}

func (r *router) refreshLeader(ctx context.Context) (string, error) {
	if r.routing != RoutingLeader {
		return "", nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.lastRefreshAt.IsZero() && time.Since(r.lastRefreshAt) < 200*time.Millisecond {
		return r.endpoints[int(r.leaderIndex.Load())], nil
	}

	probeKey := fmt.Sprintf("__mini_kv_bench_probe__:%d", time.Now().UnixNano())
	var lastErr error
	for index, client := range r.clients {
		reqCtx, cancel := context.WithTimeout(ctx, r.requestTimeout)
		_, err := client.Set(reqCtx, &minikvv1.SetRequest{
			Key:   probeKey,
			Value: []byte("1"),
		})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		reqCtx, cancel = context.WithTimeout(ctx, r.requestTimeout)
		_, _ = client.Delete(reqCtx, &minikvv1.DeleteRequest{Key: probeKey})
		cancel()

		r.leaderIndex.Store(uint64(index))
		r.lastRefreshAt = time.Now()
		r.refreshCount.Add(1)
		return r.endpoints[index], nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no endpoint accepted leader probe")
	}
	return "", fmt.Errorf("discover leader: %w", lastErr)
}
