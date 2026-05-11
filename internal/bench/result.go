package bench

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Result struct {
	Label          string                      `json:"label,omitempty"`
	StartedAt      time.Time                   `json:"started_at"`
	EndedAt        time.Time                   `json:"ended_at"`
	Duration       string                      `json:"duration"`
	Warmup         string                      `json:"warmup"`
	Mode           string                      `json:"mode"`
	Routing        string                      `json:"routing"`
	Endpoints      []string                    `json:"endpoints"`
	LeaderEndpoint string                      `json:"leader_endpoint,omitempty"`
	Concurrency    int                         `json:"concurrency"`
	Keyspace       int                         `json:"keyspace"`
	ValueSize      int                         `json:"value_size"`
	PreloadKeys    int                         `json:"preload_keys"`
	Seed           int64                       `json:"seed"`
	LeaderRefresh  uint64                      `json:"leader_refresh_count"`
	Totals         OperationSummary            `json:"totals"`
	Operations     map[string]OperationSummary `json:"operations"`
	Latency        LatencySummary              `json:"latency"`
	ErrorSamples   map[string]uint64           `json:"error_samples,omitempty"`
}

type OperationSummary struct {
	Total        uint64  `json:"total"`
	Success      uint64  `json:"success"`
	Errors       uint64  `json:"errors"`
	Found        uint64  `json:"found,omitempty"`
	Missing      uint64  `json:"missing,omitempty"`
	SuccessQPS   float64 `json:"success_qps"`
	AttemptedQPS float64 `json:"attempted_qps"`
}

type LatencySummary struct {
	AverageMS float64 `json:"average_ms"`
	P50MS     float64 `json:"p50_ms"`
	P95MS     float64 `json:"p95_ms"`
	P99MS     float64 `json:"p99_ms"`
	MaxMS     float64 `json:"max_ms"`
}

func (r Result) Summary() string {
	var builder strings.Builder
	if r.Label != "" {
		fmt.Fprintf(&builder, "label=%s\n", r.Label)
	}
	fmt.Fprintf(&builder, "mode=%s routing=%s concurrency=%d duration=%s warmup=%s\n",
		r.Mode, r.Routing, r.Concurrency, r.Duration, r.Warmup)
	fmt.Fprintf(&builder, "endpoints=%s\n", strings.Join(r.Endpoints, ","))
	if r.LeaderEndpoint != "" {
		fmt.Fprintf(&builder, "leader=%s refreshes=%d\n", r.LeaderEndpoint, r.LeaderRefresh)
	}
	fmt.Fprintf(&builder, "totals: success=%d errors=%d total=%d success_qps=%.2f attempted_qps=%.2f\n",
		r.Totals.Success, r.Totals.Errors, r.Totals.Total, r.Totals.SuccessQPS, r.Totals.AttemptedQPS)
	fmt.Fprintf(&builder, "latency_ms: avg=%.3f p50=%.3f p95=%.3f p99=%.3f max=%.3f\n",
		r.Latency.AverageMS, r.Latency.P50MS, r.Latency.P95MS, r.Latency.P99MS, r.Latency.MaxMS)

	names := make([]string, 0, len(r.Operations))
	for name := range r.Operations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		op := r.Operations[name]
		fmt.Fprintf(&builder, "op=%s success=%d errors=%d total=%d success_qps=%.2f attempted_qps=%.2f",
			name, op.Success, op.Errors, op.Total, op.SuccessQPS, op.AttemptedQPS)
		if name == string(ModeGet) {
			fmt.Fprintf(&builder, " found=%d missing=%d", op.Found, op.Missing)
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

func summarizeOperation(total workerOperationSummary, seconds float64) OperationSummary {
	summary := OperationSummary{
		Total:   total.Total,
		Success: total.Success,
		Errors:  total.Errors,
		Found:   total.Found,
		Missing: total.Missing,
	}
	if seconds > 0 {
		summary.SuccessQPS = float64(total.Success) / seconds
		summary.AttemptedQPS = float64(total.Total) / seconds
	}
	return summary
}

func summarizeLatencies(samples []int64) LatencySummary {
	if len(samples) == 0 {
		return LatencySummary{}
	}

	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total int64
	for _, sample := range sorted {
		total += sample
	}

	return LatencySummary{
		AverageMS: nanosToMillis(float64(total) / float64(len(sorted))),
		P50MS:     nanosToMillis(float64(percentile(sorted, 0.50))),
		P95MS:     nanosToMillis(float64(percentile(sorted, 0.95))),
		P99MS:     nanosToMillis(float64(percentile(sorted, 0.99))),
		MaxMS:     nanosToMillis(float64(sorted[len(sorted)-1])),
	}
}

func percentile(sorted []int64, ratio float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(ratio*float64(len(sorted)-1) + 0.5)
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func nanosToMillis(nanos float64) float64 {
	return nanos / float64(time.Millisecond)
}
