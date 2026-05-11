package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"mini-kv/internal/bench"
)

func main() {
	cfg, reportPath, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse flags: %v\n", err)
		os.Exit(2)
	}

	result, err := bench.Run(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run bench: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(result.Summary())
	if reportPath == "" {
		return
	}

	if err := writeReport(reportPath, result); err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() (bench.Config, string, error) {
	var cfg bench.Config
	var endpoints string
	var routing string
	var mode string
	var reportPath string

	flag.StringVar(&cfg.Label, "label", "", "optional workload label for reports")
	flag.StringVar(&endpoints, "endpoints", "127.0.0.1:6380", "comma-separated gRPC endpoints")
	flag.StringVar(&cfg.LeaderEndpoint, "leader-endpoint", "", "explicit leader endpoint override")
	flag.StringVar(&routing, "routing", string(bench.RoutingLeader), "request routing: leader or round_robin")
	flag.StringVar(&mode, "mode", string(bench.ModeSet), "benchmark mode: set, get, delete, mixed")
	flag.IntVar(&cfg.Concurrency, "concurrency", 32, "number of concurrent workers")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "measurement duration")
	flag.DurationVar(&cfg.Warmup, "warmup", 5*time.Second, "warmup duration before measurement")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", 2*time.Second, "per-request timeout")
	flag.DurationVar(&cfg.ConnectTimeout, "connect-timeout", 3*time.Second, "dial timeout per endpoint")
	flag.IntVar(&cfg.Keyspace, "keyspace", 1024, "number of keys used by the workload")
	flag.IntVar(&cfg.ValueSize, "value-size", 256, "value size in bytes for write operations")
	flag.IntVar(&cfg.PreloadKeys, "preload-keys", -1, "keys preloaded before the run; -1 means auto")
	flag.IntVar(&cfg.ReadPercent, "read-percent", 70, "read percentage for mixed mode")
	flag.IntVar(&cfg.WritePercent, "write-percent", 30, "write percentage for mixed mode")
	flag.IntVar(&cfg.DeletePercent, "delete-percent", 0, "delete percentage for mixed mode")
	flag.Int64Var(&cfg.Seed, "seed", 1, "random seed")
	flag.StringVar(&reportPath, "report", "", "optional JSON report path")
	flag.Parse()

	cfg.Endpoints = splitCSV(endpoints)
	cfg.Routing = bench.Routing(routing)
	cfg.Mode = bench.Mode(mode)
	cfg = cfg.Normalize()

	if err := cfg.Validate(); err != nil {
		return bench.Config{}, "", err
	}
	return cfg, reportPath, nil
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func writeReport(path string, result bench.Result) error {
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func filepathDir(path string) string {
	index := strings.LastIndex(path, string(os.PathSeparator))
	if index < 0 {
		return "."
	}
	if index == 0 {
		return string(os.PathSeparator)
	}
	return path[:index]
}
