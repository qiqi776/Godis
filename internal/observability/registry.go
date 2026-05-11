package observability

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var latencyBuckets = []time.Duration{
	1 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
}

type Registry struct {
	startedAt time.Time

	mu            sync.RWMutex
	grpcStats     map[labelKey]*durationStats
	raftOpStats   map[labelKey]*durationStats
	leaderChanges map[string]uint64
	raftStates    map[string]string
	raftLeaders   map[string]string
	commitIndex   map[string]uint64
	appliedIndex  map[string]uint64
}

type labelKey struct {
	A string
	B string
	C string
}

type durationStats struct {
	Count   uint64
	SumNano uint64
	MaxNano uint64
	Buckets []uint64
}

type DebugSnapshot struct {
	StartedAt     time.Time                        `json:"started_at"`
	GRPC          map[string]DurationStatsSnapshot `json:"grpc"`
	RaftOperation map[string]DurationStatsSnapshot `json:"raft_operation"`
	LeaderChanges map[string]uint64                `json:"leader_changes"`
	RaftState     map[string]string                `json:"raft_state"`
	RaftLeader    map[string]string                `json:"raft_leader"`
	CommitIndex   map[string]uint64                `json:"commit_index"`
	AppliedIndex  map[string]uint64                `json:"applied_index"`
}

type DurationStatsSnapshot struct {
	Count     uint64            `json:"count"`
	SumMS     float64           `json:"sum_ms"`
	MaxMS     float64           `json:"max_ms"`
	BucketsMS map[string]uint64 `json:"buckets_ms"`
}

func NewRegistry() *Registry {
	return &Registry{
		startedAt:     time.Now(),
		grpcStats:     make(map[labelKey]*durationStats),
		raftOpStats:   make(map[labelKey]*durationStats),
		leaderChanges: make(map[string]uint64),
		raftStates:    make(map[string]string),
		raftLeaders:   make(map[string]string),
		commitIndex:   make(map[string]uint64),
		appliedIndex:  make(map[string]uint64),
	}
}

func (r *Registry) ObserveGRPC(method string, duration time.Duration, err error) {
	if r == nil {
		return
	}
	statusLabel := strings.ToLower(status.Code(err).String())
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.ensureStats(r.grpcStats, labelKey{A: method, B: statusLabel})
	stats.observe(duration)
}

func (r *Registry) ObserveRaftOperation(nodeID, operation string, duration time.Duration, err error) {
	if r == nil {
		return
	}
	statusLabel := strings.ToLower(statusCodeString(err))
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.ensureStats(r.raftOpStats, labelKey{A: nodeID, B: operation, C: statusLabel})
	stats.observe(duration)
}

func (r *Registry) IncLeaderChange(nodeID string) {
	if r == nil || nodeID == "" {
		return
	}
	r.mu.Lock()
	r.leaderChanges[nodeID]++
	r.mu.Unlock()
}

func (r *Registry) SetRaftState(nodeID, state string) {
	if r == nil || nodeID == "" {
		return
	}
	r.mu.Lock()
	r.raftStates[nodeID] = state
	r.mu.Unlock()
}

func (r *Registry) SetRaftLeader(nodeID, leaderID string) {
	if r == nil || nodeID == "" {
		return
	}
	r.mu.Lock()
	r.raftLeaders[nodeID] = leaderID
	r.mu.Unlock()
}

func (r *Registry) SetCommitIndex(nodeID string, index uint64) {
	if r == nil || nodeID == "" {
		return
	}
	r.mu.Lock()
	r.commitIndex[nodeID] = index
	r.mu.Unlock()
}

func (r *Registry) SetAppliedIndex(nodeID string, index uint64) {
	if r == nil || nodeID == "" {
		return
	}
	r.mu.Lock()
	r.appliedIndex[nodeID] = index
	r.mu.Unlock()
}

func (r *Registry) Snapshot() DebugSnapshot {
	if r == nil {
		return DebugSnapshot{}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshot := DebugSnapshot{
		StartedAt:     r.startedAt,
		GRPC:          make(map[string]DurationStatsSnapshot, len(r.grpcStats)),
		RaftOperation: make(map[string]DurationStatsSnapshot, len(r.raftOpStats)),
		LeaderChanges: cloneUintMap(r.leaderChanges),
		RaftState:     cloneStringMap(r.raftStates),
		RaftLeader:    cloneStringMap(r.raftLeaders),
		CommitIndex:   cloneUintMap(r.commitIndex),
		AppliedIndex:  cloneUintMap(r.appliedIndex),
	}

	for key, stats := range r.grpcStats {
		snapshot.GRPC[fmt.Sprintf("%s|%s", key.A, key.B)] = stats.snapshot()
	}
	for key, stats := range r.raftOpStats {
		snapshot.RaftOperation[fmt.Sprintf("%s|%s|%s", key.A, key.B, key.C)] = stats.snapshot()
	}
	return snapshot
}

func (r *Registry) SnapshotJSON() ([]byte, error) {
	return json.MarshalIndent(r.Snapshot(), "", "  ")
}

func (r *Registry) RenderPrometheus() string {
	if r == nil {
		return ""
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var builder strings.Builder
	builder.WriteString("# HELP mini_kv_grpc_requests_total Total gRPC requests handled by method and status.\n")
	builder.WriteString("# TYPE mini_kv_grpc_requests_total counter\n")
	grpcKeys := sortedLabelKeys(r.grpcStats)
	for _, key := range grpcKeys {
		stats := r.grpcStats[key]
		writeMetric(&builder, "mini_kv_grpc_requests_total",
			map[string]string{"method": key.A, "status": key.B},
			float64(stats.Count))
	}
	writeHistogramFamily(&builder, "mini_kv_grpc_request_duration_ms", "gRPC request duration in milliseconds.", grpcKeys, r.grpcStats,
		func(key labelKey) map[string]string {
			return map[string]string{"method": key.A, "status": key.B}
		})

	builder.WriteString("# HELP mini_kv_raft_operations_total Total raft/runtime operations by node, operation and status.\n")
	builder.WriteString("# TYPE mini_kv_raft_operations_total counter\n")
	raftKeys := sortedLabelKeys(r.raftOpStats)
	for _, key := range raftKeys {
		stats := r.raftOpStats[key]
		writeMetric(&builder, "mini_kv_raft_operations_total",
			map[string]string{"node": key.A, "operation": key.B, "status": key.C},
			float64(stats.Count))
	}
	writeHistogramFamily(&builder, "mini_kv_raft_operation_duration_ms", "Raft/runtime operation duration in milliseconds.", raftKeys, r.raftOpStats,
		func(key labelKey) map[string]string {
			return map[string]string{"node": key.A, "operation": key.B, "status": key.C}
		})

	builder.WriteString("# HELP mini_kv_raft_leader_changes_total Total leader transitions observed by node.\n")
	builder.WriteString("# TYPE mini_kv_raft_leader_changes_total counter\n")
	for _, nodeID := range sortedStringKeys(r.leaderChanges) {
		writeMetric(&builder, "mini_kv_raft_leader_changes_total", map[string]string{"node": nodeID}, float64(r.leaderChanges[nodeID]))
	}

	builder.WriteString("# HELP mini_kv_raft_commit_index Current commit index by node.\n")
	builder.WriteString("# TYPE mini_kv_raft_commit_index gauge\n")
	for _, nodeID := range sortedStringKeys(r.commitIndex) {
		writeMetric(&builder, "mini_kv_raft_commit_index", map[string]string{"node": nodeID}, float64(r.commitIndex[nodeID]))
	}

	builder.WriteString("# HELP mini_kv_raft_applied_index Current applied index by node.\n")
	builder.WriteString("# TYPE mini_kv_raft_applied_index gauge\n")
	for _, nodeID := range sortedStringKeys(r.appliedIndex) {
		writeMetric(&builder, "mini_kv_raft_applied_index", map[string]string{"node": nodeID}, float64(r.appliedIndex[nodeID]))
	}

	builder.WriteString("# HELP mini_kv_raft_state_info Current raft state by node.\n")
	builder.WriteString("# TYPE mini_kv_raft_state_info gauge\n")
	for _, nodeID := range sortedStringMapKeys(r.raftStates) {
		writeMetric(&builder, "mini_kv_raft_state_info", map[string]string{"node": nodeID, "state": r.raftStates[nodeID]}, 1)
	}

	builder.WriteString("# HELP mini_kv_raft_leader_info Current known leader by node.\n")
	builder.WriteString("# TYPE mini_kv_raft_leader_info gauge\n")
	for _, nodeID := range sortedStringMapKeys(r.raftLeaders) {
		writeMetric(&builder, "mini_kv_raft_leader_info", map[string]string{"node": nodeID, "leader": r.raftLeaders[nodeID]}, 1)
	}

	return builder.String()
}

func (r *Registry) ensureStats(target map[labelKey]*durationStats, key labelKey) *durationStats {
	stats, ok := target[key]
	if !ok {
		stats = &durationStats{Buckets: make([]uint64, len(latencyBuckets)+1)}
		target[key] = stats
	}
	return stats
}

func (s *durationStats) observe(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	s.Count++
	s.SumNano += uint64(duration)
	if uint64(duration) > s.MaxNano {
		s.MaxNano = uint64(duration)
	}

	index := len(latencyBuckets)
	for i, bound := range latencyBuckets {
		if duration <= bound {
			index = i
			break
		}
	}
	for i := index; i < len(s.Buckets); i++ {
		s.Buckets[i]++
	}
}

func (s *durationStats) snapshot() DurationStatsSnapshot {
	out := DurationStatsSnapshot{
		Count:     s.Count,
		SumMS:     float64(s.SumNano) / float64(time.Millisecond),
		MaxMS:     float64(s.MaxNano) / float64(time.Millisecond),
		BucketsMS: make(map[string]uint64, len(s.Buckets)),
	}
	for i, count := range s.Buckets {
		if i == len(latencyBuckets) {
			out.BucketsMS["+Inf"] = count
			continue
		}
		label := fmt.Sprintf("%g", float64(latencyBuckets[i])/float64(time.Millisecond))
		out.BucketsMS[label] = count
	}
	return out
}

func writeHistogramFamily(builder *strings.Builder, metricName string, help string, keys []labelKey, source map[labelKey]*durationStats, labels func(labelKey) map[string]string) {
	builder.WriteString("# HELP " + metricName + " " + help + "\n")
	builder.WriteString("# TYPE " + metricName + " histogram\n")
	for _, key := range keys {
		stats := source[key]
		base := labels(key)
		for i, count := range stats.Buckets {
			labelCopy := copyLabels(base)
			if i == len(latencyBuckets) {
				labelCopy["le"] = "+Inf"
			} else {
				labelCopy["le"] = fmt.Sprintf("%g", float64(latencyBuckets[i])/float64(time.Millisecond))
			}
			writeMetric(builder, metricName+"_bucket", labelCopy, float64(count))
		}
		writeMetric(builder, metricName+"_sum", base, float64(stats.SumNano)/float64(time.Millisecond))
		writeMetric(builder, metricName+"_count", base, float64(stats.Count))
	}
}

func writeMetric(builder *strings.Builder, name string, labels map[string]string, value float64) {
	builder.WriteString(name)
	if len(labels) > 0 {
		builder.WriteByte('{')
		keys := make([]string, 0, len(labels))
		for key := range labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for i, key := range keys {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(key)
			builder.WriteString("=\"")
			builder.WriteString(escapeLabel(labels[key]))
			builder.WriteByte('"')
		}
		builder.WriteByte('}')
	}
	builder.WriteByte(' ')
	builder.WriteString(fmt.Sprintf("%.6f", value))
	builder.WriteByte('\n')
}

func sortedLabelKeys(source map[labelKey]*durationStats) []labelKey {
	keys := make([]labelKey, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].A != keys[j].A {
			return keys[i].A < keys[j].A
		}
		if keys[i].B != keys[j].B {
			return keys[i].B < keys[j].B
		}
		return keys[i].C < keys[j].C
	})
	return keys
}

func sortedStringKeys(source map[string]uint64) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStringMapKeys(source map[string]string) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cloneUintMap(source map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func cloneStringMap(source map[string]string) map[string]string {
	out := make(map[string]string, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func copyLabels(source map[string]string) map[string]string {
	out := make(map[string]string, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func escapeLabel(input string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")
	return replacer.Replace(input)
}

func statusCodeString(err error) string {
	if err == nil {
		return codes.OK.String()
	}
	return status.Code(err).String()
}
