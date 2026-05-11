package bench

import (
	"fmt"
	"strings"
	"time"
)

type Mode string

const (
	ModeSet    Mode = "set"
	ModeGet    Mode = "get"
	ModeDelete Mode = "delete"
	ModeMixed  Mode = "mixed"
)

type Routing string

const (
	RoutingLeader     Routing = "leader"
	RoutingRoundRobin Routing = "round_robin"
)

type Config struct {
	Label          string
	Endpoints      []string
	LeaderEndpoint string
	Routing        Routing
	Mode           Mode

	Concurrency    int
	Duration       time.Duration
	Warmup         time.Duration
	RequestTimeout time.Duration
	ConnectTimeout time.Duration

	Keyspace    int
	ValueSize   int
	PreloadKeys int

	ReadPercent   int
	WritePercent  int
	DeletePercent int

	Seed int64
}

func (c Config) Normalize() Config {
	out := c
	out.Endpoints = normalizeEndpoints(c.Endpoints)
	if out.Routing == "" {
		out.Routing = RoutingLeader
	}
	if out.Mode == "" {
		out.Mode = ModeSet
	}
	if out.Concurrency <= 0 {
		out.Concurrency = 1
	}
	if out.Duration <= 0 {
		out.Duration = 30 * time.Second
	}
	if out.RequestTimeout <= 0 {
		out.RequestTimeout = 2 * time.Second
	}
	if out.ConnectTimeout <= 0 {
		out.ConnectTimeout = 3 * time.Second
	}
	if out.Keyspace <= 0 {
		out.Keyspace = 1
	}
	if out.ValueSize < 0 {
		out.ValueSize = 0
	}
	if out.ReadPercent == 0 && out.WritePercent == 0 && out.DeletePercent == 0 {
		out.ReadPercent = 70
		out.WritePercent = 30
	}
	if out.PreloadKeys < 0 && out.needsPreload() {
		out.PreloadKeys = out.Keyspace
	}
	if out.PreloadKeys < 0 {
		out.PreloadKeys = 0
	}
	if out.PreloadKeys > out.Keyspace {
		out.PreloadKeys = out.Keyspace
	}
	return out
}

func (c Config) Validate() error {
	if len(c.Endpoints) == 0 {
		return fmt.Errorf("endpoints must not be empty")
	}
	switch c.Routing {
	case RoutingLeader, RoutingRoundRobin:
	default:
		return fmt.Errorf("unsupported routing %q", c.Routing)
	}
	switch c.Mode {
	case ModeSet, ModeGet, ModeDelete, ModeMixed:
	default:
		return fmt.Errorf("unsupported mode %q", c.Mode)
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	if c.Warmup < 0 {
		return fmt.Errorf("warmup must not be negative")
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be positive")
	}
	if c.ConnectTimeout <= 0 {
		return fmt.Errorf("connect timeout must be positive")
	}
	if c.Keyspace <= 0 {
		return fmt.Errorf("keyspace must be positive")
	}
	if c.ValueSize < 0 {
		return fmt.Errorf("value size must not be negative")
	}
	if c.PreloadKeys < 0 {
		return fmt.Errorf("preload keys must not be negative")
	}
	if c.PreloadKeys > c.Keyspace {
		return fmt.Errorf("preload keys must not exceed keyspace")
	}
	if c.Mode == ModeMixed {
		total := c.ReadPercent + c.WritePercent + c.DeletePercent
		if total != 100 {
			return fmt.Errorf("mixed mode percentages must sum to 100, got %d", total)
		}
		if c.ReadPercent < 0 || c.WritePercent < 0 || c.DeletePercent < 0 {
			return fmt.Errorf("mixed mode percentages must not be negative")
		}
	}
	return nil
}

func (c Config) needsPreload() bool {
	switch c.Mode {
	case ModeGet, ModeDelete:
		return true
	case ModeMixed:
		return c.ReadPercent > 0 || c.DeletePercent > 0
	default:
		return false
	}
}

func normalizeEndpoints(endpoints []string) []string {
	out := make([]string, 0, len(endpoints))
	seen := make(map[string]struct{}, len(endpoints))
	for _, endpoint := range endpoints {
		trimmed := strings.TrimSpace(endpoint)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}
