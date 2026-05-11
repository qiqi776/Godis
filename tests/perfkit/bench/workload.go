package bench

import (
	"fmt"
	"math/rand"
	"sync"
)

type Operation string

const (
	OpGet    Operation = "get"
	OpSet    Operation = "set"
	OpDelete Operation = "delete"
)

type Request struct {
	Operation Operation
	Key       string
	Value     []byte
}

type Workload interface {
	Next() Request
}

type Distribution string

const (
	Uniform    Distribution = "uniform"
	Zipfian    Distribution = "zipfian"
	Sequential Distribution = "sequential"
)

type Config struct {
	Distribution  Distribution
	KeyPrefix     string
	Keyspace      int
	ValueSize     int
	ReadPercent   int
	WritePercent  int
	DeletePercent int
	Seed          int64
}

func (c Config) normalize() Config {
	if c.Distribution == "" {
		c.Distribution = Uniform
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = "bench:key:"
	}
	if c.Keyspace <= 0 {
		c.Keyspace = 1024
	}
	if c.ValueSize < 0 {
		c.ValueSize = 0
	}
	if c.ReadPercent == 0 && c.WritePercent == 0 && c.DeletePercent == 0 {
		c.WritePercent = 100
	}
	return c
}

func NewWorkload(cfg Config) (Workload, error) {
	cfg = cfg.normalize()
	if total := cfg.ReadPercent + cfg.WritePercent + cfg.DeletePercent; total != 100 {
		return nil, fmt.Errorf("operation percentages must sum to 100, got %d", total)
	}

	base := workloadBase{
		cfg:    cfg,
		random: rand.New(rand.NewSource(cfg.Seed)),
		value:  makeValue(cfg.ValueSize),
	}
	switch cfg.Distribution {
	case Uniform:
		return &uniformWorkload{base: base}, nil
	case Zipfian:
		if cfg.Keyspace == 1 {
			return &uniformWorkload{base: base}, nil
		}
		return &zipfianWorkload{
			base: base,
			zipf: rand.NewZipf(base.random, 1.2, 1, uint64(cfg.Keyspace-1)),
		}, nil
	case Sequential:
		return &sequentialWorkload{base: base}, nil
	default:
		return nil, fmt.Errorf("unsupported distribution %q", cfg.Distribution)
	}
}

type workloadBase struct {
	mu     sync.Mutex
	cfg    Config
	random *rand.Rand
	value  []byte
}

func (w *workloadBase) request(index int) Request {
	op := w.operation()
	return Request{
		Operation: op,
		Key:       fmt.Sprintf("%s%d", w.cfg.KeyPrefix, index),
		Value:     append([]byte(nil), w.value...),
	}
}

func (w *workloadBase) operation() Operation {
	roll := w.random.Intn(100)
	if roll < w.cfg.ReadPercent {
		return OpGet
	}
	if roll < w.cfg.ReadPercent+w.cfg.WritePercent {
		return OpSet
	}
	return OpDelete
}

type uniformWorkload struct {
	base workloadBase
}

func (w *uniformWorkload) Next() Request {
	w.base.mu.Lock()
	defer w.base.mu.Unlock()

	return w.base.request(w.base.random.Intn(w.base.cfg.Keyspace))
}

type zipfianWorkload struct {
	base workloadBase
	zipf *rand.Zipf
}

func (w *zipfianWorkload) Next() Request {
	w.base.mu.Lock()
	defer w.base.mu.Unlock()

	return w.base.request(int(w.zipf.Uint64()))
}

type sequentialWorkload struct {
	base workloadBase
	next int
}

func (w *sequentialWorkload) Next() Request {
	w.base.mu.Lock()
	defer w.base.mu.Unlock()

	index := w.next
	w.next = (w.next + 1) % w.base.cfg.Keyspace
	return w.base.request(index)
}

func makeValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	return value
}
