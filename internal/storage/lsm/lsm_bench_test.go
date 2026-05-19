package lsm

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

var (
	benchmarkEngineBytesSink []byte
	benchmarkEngineBoolSink  bool
	benchmarkEngineCountSink int
)

func BenchmarkEngineWriteMemOnly(b *testing.B) {
	keys := benchmarkLSMKeys(4096)
	value := bytes.Repeat([]byte("x"), 128)
	engine := newBenchmarkMemOnlyEngine(b, b.TempDir())
	defer func() { _ = engine.Close() }()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%100_000 == 0 {
			b.StopTimer()
			_ = engine.Close()
			engine = newBenchmarkMemOnlyEngine(b, b.TempDir())
			b.StartTimer()
		}

		var batch WriteBatch
		batch.Put(keys[i&(len(keys)-1)], value)
		if err := engine.Write(&batch, WriteOptions{}); err != nil {
			b.Fatalf("Write error = %v", err)
		}
	}
}

func BenchmarkEngineWriteSyncWAL(b *testing.B) {
	keys := benchmarkLSMKeys(4096)
	value := bytes.Repeat([]byte("x"), 128)
	engine, err := Open(b.TempDir(), WithMemTableSize(1<<30), WithL0CompactionTrigger(1<<30))
	if err != nil {
		b.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var batch WriteBatch
		batch.Put(keys[i&(len(keys)-1)], value)
		if err := engine.Write(&batch, WriteOptions{Sync: true}); err != nil {
			b.Fatalf("Write error = %v", err)
		}
	}
}

func BenchmarkEngineWriteFlushBoundary(b *testing.B) {
	keys := benchmarkLSMKeys(256)
	value := bytes.Repeat([]byte("x"), 128)
	engine, err := Open(b.TempDir(), WithMemTableSize(1<<30), WithL0CompactionTrigger(1<<30))
	if err != nil {
		b.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var batch WriteBatch
		for j := range keys {
			batch.Put(keys[j], value)
		}
		if err := engine.Write(&batch, WriteOptions{}); err != nil {
			b.Fatalf("Write error = %v", err)
		}
		if err := engine.Flush(); err != nil {
			b.Fatalf("Flush error = %v", err)
		}
	}
}

func BenchmarkEngineGetMemTable(b *testing.B) {
	keys := benchmarkLSMKeys(8192)
	value := bytes.Repeat([]byte("x"), 128)
	engine := newBenchmarkMemOnlyEngine(b, b.TempDir())
	defer func() { _ = engine.Close() }()
	benchmarkLoadEngine(b, engine, keys, value)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, ok, err := engine.Get(keys[i&(len(keys)-1)])
		if err != nil {
			b.Fatalf("Get error = %v", err)
		}
		benchmarkEngineBytesSink = got
		benchmarkEngineBoolSink = ok
	}
}

func BenchmarkEngineGetSSTable(b *testing.B) {
	keys := benchmarkLSMKeys(8192)
	value := bytes.Repeat([]byte("x"), 128)
	engine := newBenchmarkSSTableEngine(b, len(keys), 1, value)
	defer func() { _ = engine.Close() }()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, ok, err := engine.Get(keys[i&(len(keys)-1)])
		if err != nil {
			b.Fatalf("Get error = %v", err)
		}
		benchmarkEngineBytesSink = got
		benchmarkEngineBoolSink = ok
	}
}

func BenchmarkEngineGetManyL0(b *testing.B) {
	value := bytes.Repeat([]byte("x"), 128)
	engine := newBenchmarkSSTableEngine(b, 128, 16, value)
	defer func() { _ = engine.Close() }()
	missingInRange := []byte("bench-key-00000042x")

	b.ReportAllocs()
	for b.Loop() {
		got, ok, err := engine.Get(missingInRange)
		if err != nil {
			b.Fatalf("Get error = %v", err)
		}
		benchmarkEngineBytesSink = got
		benchmarkEngineBoolSink = ok
	}
}

func BenchmarkIteratorRange(b *testing.B) {
	value := bytes.Repeat([]byte("x"), 128)
	engine := newBenchmarkSSTableEngine(b, 8192, 1, value)
	defer func() { _ = engine.Close() }()

	b.ReportAllocs()
	for b.Loop() {
		iter := engine.NewIterator(IterOptions{
			LowerBound: []byte("bench-key-00001000"),
			UpperBound: []byte("bench-key-00005000"),
		})
		count := 0
		for ok := iter.First(); ok; ok = iter.Next() {
			count++
		}
		if err := iter.Error(); err != nil {
			b.Fatalf("iterator error = %v", err)
		}
		if err := iter.Close(); err != nil {
			b.Fatalf("iterator close error = %v", err)
		}
		benchmarkEngineCountSink = count
	}
}

func BenchmarkSnapshotLarge(b *testing.B) {
	keys := benchmarkLSMKeys(8192)
	value := bytes.Repeat([]byte("x"), 128)
	engine := newBenchmarkMemOnlyEngine(b, b.TempDir())
	defer func() { _ = engine.Close() }()
	benchmarkLoadEngine(b, engine, keys, value)

	b.ReportAllocs()
	for b.Loop() {
		snapshot, err := engine.Snapshot()
		if err != nil {
			b.Fatalf("Snapshot error = %v", err)
		}
		iter := snapshot.NewIterator(IterOptions{})
		count := 0
		for ok := iter.First(); ok; ok = iter.Next() {
			count++
		}
		if err := iter.Close(); err != nil {
			b.Fatalf("iterator close error = %v", err)
		}
		if err := snapshot.Close(); err != nil {
			b.Fatalf("snapshot close error = %v", err)
		}
		benchmarkEngineCountSink = count
	}
}

func BenchmarkReopenReplayWAL(b *testing.B) {
	dir := b.TempDir()
	keys := benchmarkLSMKeys(4096)
	value := bytes.Repeat([]byte("x"), 128)
	engine, err := Open(dir, WithMemTableSize(1<<30), WithL0CompactionTrigger(1<<30))
	if err != nil {
		b.Fatalf("Open error = %v", err)
	}
	benchmarkLoadEngine(b, engine, keys, value)
	if err := engine.Close(); err != nil {
		b.Fatalf("Close setup engine: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		reopened, err := Open(dir, WithMemTableSize(1<<30), WithL0CompactionTrigger(1<<30))
		if err != nil {
			b.Fatalf("reopen error = %v", err)
		}
		if err := reopened.Close(); err != nil {
			b.Fatalf("close reopened engine: %v", err)
		}
	}
}

func BenchmarkCompactionL0ToL1(b *testing.B) {
	value := bytes.Repeat([]byte("x"), 128)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), fmt.Sprintf("case-%06d", i))
		engine := newBenchmarkSSTableEngineAt(b, dir, 256, 8, value)
		engine.opts.L0CompactionTrigger = 1
		b.StartTimer()

		if err := engine.runCompaction(context.Background(), compactionJob{level: 0}); err != nil {
			b.Fatalf("runCompaction error = %v", err)
		}

		b.StopTimer()
		if err := engine.Close(); err != nil {
			b.Fatalf("Close error = %v", err)
		}
	}
}

func benchmarkLSMKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%08d", i))
	}
	return keys
}

func benchmarkLoadEngine(b *testing.B, engine *Engine, keys [][]byte, value []byte) {
	b.Helper()
	for _, key := range keys {
		var batch WriteBatch
		batch.Put(key, value)
		if err := engine.Write(&batch, WriteOptions{}); err != nil {
			b.Fatalf("Write setup key %q: %v", key, err)
		}
	}
}

func newBenchmarkMemOnlyEngine(b *testing.B, dir string) *Engine {
	b.Helper()
	engine, err := openWithComponents(dir, components{
		WALFactory:      benchmarkNoopWALFactory{},
		ManifestFactory: benchmarkNoopManifestFactory{},
	}, WithMemTableSize(1<<30), WithL0CompactionTrigger(1<<30))
	if err != nil {
		b.Fatalf("openWithComponents error = %v", err)
	}
	return engine
}

func newBenchmarkSSTableEngine(b *testing.B, keysPerTable int, tableCount int, value []byte) *Engine {
	b.Helper()
	return newBenchmarkSSTableEngineAt(b, b.TempDir(), keysPerTable, tableCount, value)
}

func newBenchmarkSSTableEngineAt(b *testing.B, dir string, keysPerTable int, tableCount int, value []byte) *Engine {
	b.Helper()
	engine, err := Open(dir, WithMemTableSize(1<<30), WithL0CompactionTrigger(1<<30))
	if err != nil {
		b.Fatalf("Open error = %v", err)
	}
	keys := benchmarkLSMKeys(keysPerTable)
	for i := 0; i < tableCount; i++ {
		var batch WriteBatch
		for _, key := range keys {
			batch.Put(key, value)
		}
		if err := engine.Write(&batch, WriteOptions{}); err != nil {
			_ = engine.Close()
			b.Fatalf("Write table %d: %v", i, err)
		}
		if err := engine.Flush(); err != nil {
			_ = engine.Close()
			b.Fatalf("Flush table %d: %v", i, err)
		}
	}
	return engine
}

type benchmarkNoopWALFactory struct{}

func (benchmarkNoopWALFactory) Open(string, uint64, walOptions) (walStore, error) {
	return benchmarkNoopWAL{}, nil
}

type benchmarkNoopWAL struct{}

func (benchmarkNoopWAL) Append(batch, bool) error {
	return nil
}

func (benchmarkNoopWAL) Replay(func(batch) error) error {
	return nil
}

func (benchmarkNoopWAL) Purge(uint64) error {
	return nil
}

func (benchmarkNoopWAL) Close() error {
	return nil
}

type benchmarkNoopManifestFactory struct{}

func (benchmarkNoopManifestFactory) Open(string, uint64) (manifestStore, error) {
	return &benchmarkNoopManifest{state: (&versionState{NextFileNum: 1}).Clone()}, nil
}

type benchmarkNoopManifest struct {
	state *versionState
}

func (m *benchmarkNoopManifest) Load() (*versionState, error) {
	return m.state.Clone(), nil
}

func (m *benchmarkNoopManifest) Apply(edit versionEdit) error {
	m.state = m.state.Apply(edit)
	return nil
}

func (m *benchmarkNoopManifest) Close() error {
	return nil
}
