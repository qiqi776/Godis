package lsm

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestOpenValidatesOptions(t *testing.T) {
	_, err := Open(t.TempDir(), WithMemTableSize(0))
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Open error = %v, want %v", err, ErrInvalidOptions)
	}
}

func TestEngineCloseIsIdempotent(t *testing.T) {
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("second Close error = %v", err)
	}
	if _, _, err := engine.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close error = %v, want %v", err, ErrClosed)
	}
}

func TestOpenRejectsLockedDirectory(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	_, err = Open(dir)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open error = %v, want %v", err, ErrLocked)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after Close error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close error = %v", err)
	}
}

func TestEngineWriteGetIteratorAndSnapshot(t *testing.T) {
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	var batch WriteBatch
	key := []byte("a")
	value := []byte("1")
	batch.Put(key, value)
	key[0] = 'z'
	value[0] = '9'
	batch.Put([]byte("b"), []byte("2"))
	if err := engine.Write(&batch, WriteOptions{}); err != nil {
		t.Fatalf("Write error = %v", err)
	}

	got, ok, err := engine.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get error = %v", err)
	}
	if !ok || string(got) != "1" {
		t.Fatalf("Get(a) = (%q, %v), want (1, true)", got, ok)
	}
	got[0] = 'x'
	got, ok, err = engine.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after mutation error = %v", err)
	}
	if !ok || string(got) != "1" {
		t.Fatalf("Get(a) after mutation = (%q, %v), want (1, true)", got, ok)
	}

	iter := engine.NewIterator(IterOptions{LowerBound: []byte("a"), UpperBound: []byte("c")})
	defer func() { _ = iter.Close() }()
	if !iter.First() {
		t.Fatal("Iterator First() = false, want true")
	}
	if string(iter.Key()) != "a" || string(iter.Value()) != "1" {
		t.Fatalf("Iterator first = (%q, %q), want (a, 1)", iter.Key(), iter.Value())
	}
	if !iter.Next() || string(iter.Key()) != "b" {
		t.Fatalf("Iterator second key = %q, want b", iter.Key())
	}

	snapshot, err := engine.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error = %v", err)
	}
	defer func() { _ = snapshot.Close() }()

	batch.Reset()
	batch.Put([]byte("a"), []byte("new"))
	if err := engine.Write(&batch, WriteOptions{}); err != nil {
		t.Fatalf("Write after snapshot error = %v", err)
	}
	got, ok, err = snapshot.Get([]byte("a"))
	if err != nil {
		t.Fatalf("snapshot Get error = %v", err)
	}
	if !ok || string(got) != "1" {
		t.Fatalf("snapshot Get(a) = (%q, %v), want (1, true)", got, ok)
	}
}

func TestEngineWriteValidation(t *testing.T) {
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	var invalid WriteBatch
	invalid.Ops = append(invalid.Ops, WriteOp{Type: OpPut})
	if err := engine.Write(&invalid, WriteOptions{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Write invalid key error = %v, want %v", err, ErrInvalidKey)
	}
}

func TestEngineFlushWithCanceledContext(t *testing.T) {
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.FlushWithContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("FlushWithContext error = %v, want context canceled", err)
	}
}

func TestEngineWrapsWALErrors(t *testing.T) {
	walErr := io.ErrClosedPipe
	engine, err := openWithComponents(t.TempDir(), components{
		WALFactory: fakeWALFactory{appendErr: walErr},
	})
	if err != nil {
		t.Fatalf("openWithComponents error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	var batch WriteBatch
	batch.Put([]byte("k"), []byte("v"))
	err = engine.Write(&batch, WriteOptions{})
	if !errors.Is(err, walErr) || !errors.Is(err, ErrIO) {
		t.Fatalf("Write WAL error = %v, want wrapped wal io error", err)
	}
}

func TestConcurrentWriteAndRead(t *testing.T) {
	engine, err := Open(t.TempDir(), WithMemTableSize(128))
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	errCh := make(chan error, 2)
	go func() {
		for i := 0; i < 100; i++ {
			var batch WriteBatch
			batch.Put([]byte{byte('a' + i%10)}, []byte{byte(i)})
			if err := engine.Write(&batch, WriteOptions{}); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	go func() {
		for i := 0; i < 100; i++ {
			if _, _, err := engine.Get([]byte("a")); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent operation error = %v", err)
		}
	}
}

func TestEngineFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir, WithMemTableSize(1), WithL0CompactionTrigger(10))
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	var batch WriteBatch
	batch.Put([]byte("a"), []byte("1"))
	batch.Put([]byte("b"), []byte("2"))
	if err := engine.Write(&batch, WriteOptions{}); err != nil {
		t.Fatalf("Write error = %v", err)
	}
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	engine, err = Open(dir, WithMemTableSize(1), WithL0CompactionTrigger(10))
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = engine.Close() }()
	got, ok, err := engine.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after reopen error = %v", err)
	}
	if !ok || string(got) != "1" {
		t.Fatalf("Get(a) after reopen = (%q, %v), want (1, true)", got, ok)
	}
}

func TestEngineWALRecoveryAfterUnflushedClose(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	var batch WriteBatch
	batch.Put([]byte("a"), []byte("1"))
	if err := engine.Write(&batch, WriteOptions{Sync: true}); err != nil {
		t.Fatalf("Write error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	recovered, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = recovered.Close() }()
	got, ok, err := recovered.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get recovered error = %v", err)
	}
	if !ok || string(got) != "1" {
		t.Fatalf("Get(a) recovered = (%q, %v), want (1, true)", got, ok)
	}
}

func TestEngineCompactionKeepsLatestValue(t *testing.T) {
	engine, err := Open(t.TempDir(), WithMemTableSize(1), WithL0CompactionTrigger(100))
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	for _, value := range []string{"1", "2", "3"} {
		var batch WriteBatch
		batch.Put([]byte("a"), []byte(value))
		if err := engine.Write(&batch, WriteOptions{}); err != nil {
			t.Fatalf("Write %s error = %v", value, err)
		}
		if err := engine.Flush(); err != nil {
			t.Fatalf("Flush %s error = %v", value, err)
		}
	}
	engine.opts.L0CompactionTrigger = 1
	if err := engine.runCompaction(context.Background(), compactionJob{level: 0}); err != nil {
		t.Fatalf("runCompaction error = %v", err)
	}
	got, ok, err := engine.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after compaction error = %v", err)
	}
	if !ok || string(got) != "3" {
		t.Fatalf("Get(a) after compaction = (%q, %v), want (3, true)", got, ok)
	}
}

func TestEngineFlushPurgesOldWALSegments(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir, WithWALSegmentSize(96), WithL0CompactionTrigger(100))
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	for i := 0; i < 8; i++ {
		var batch WriteBatch
		batch.Put([]byte{byte('a' + i)}, []byte("value"))
		if err := engine.Write(&batch, WriteOptions{Sync: true}); err != nil {
			t.Fatalf("Write %d error = %v", i, err)
		}
	}
	before := countFiles(t, dir, "WAL-*")
	if before < 2 {
		t.Fatalf("wal segment count before flush = %d, want at least 2", before)
	}
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush error = %v", err)
	}
	after := countFiles(t, dir, "WAL-*")
	if after >= before {
		t.Fatalf("wal segment count after flush = %d, want less than %d", after, before)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	reopened, err := Open(dir, WithWALSegmentSize(96), WithL0CompactionTrigger(100))
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = reopened.Close() }()
	got, ok, err := reopened.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after reopen error = %v", err)
	}
	if !ok || string(got) != "value" {
		t.Fatalf("Get(a) after reopen = (%q, %v), want (value, true)", got, ok)
	}
}

func TestEngineCompactionRemovesObsoleteSSTables(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir, WithMemTableSize(1), WithL0CompactionTrigger(100))
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	for _, value := range []string{"1", "2", "3"} {
		var batch WriteBatch
		batch.Put([]byte("a"), []byte(value))
		if err := engine.Write(&batch, WriteOptions{}); err != nil {
			t.Fatalf("Write %s error = %v", value, err)
		}
		if err := engine.Flush(); err != nil {
			t.Fatalf("Flush %s error = %v", value, err)
		}
	}
	before := countFiles(t, dir, "*.sst")
	if before != 3 {
		t.Fatalf("sstable count before compaction = %d, want 3", before)
	}
	engine.opts.L0CompactionTrigger = 1
	if err := engine.runCompaction(context.Background(), compactionJob{level: 0}); err != nil {
		t.Fatalf("runCompaction error = %v", err)
	}
	after := countFiles(t, dir, "*.sst")
	if after != 1 {
		t.Fatalf("sstable count after compaction = %d, want 1", after)
	}
}

func TestEngineBackgroundErrorIsObservable(t *testing.T) {
	engine, err := openWithComponents(t.TempDir(), components{
		TableManager: &failingTableManager{buildErr: io.ErrClosedPipe},
	}, WithMemTableSize(1))
	if err != nil {
		t.Fatalf("openWithComponents error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	var batch WriteBatch
	batch.Put([]byte("a"), []byte("1"))
	if err := engine.Write(&batch, WriteOptions{}); err != nil {
		t.Fatalf("Write error = %v", err)
	}
	err = engine.Flush()
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Flush error = %v, want wrapped build error", err)
	}
	err = engine.Write(&batch, WriteOptions{})
	if !errors.Is(err, ErrBackground) || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write after background error = %v, want background build error", err)
	}
	if _, _, err := engine.Get([]byte("a")); !errors.Is(err, ErrBackground) {
		t.Fatalf("Get after background error = %v, want background error", err)
	}
}

func TestEngineBasicContract(t *testing.T) {
	engine, err := Open(t.TempDir(), WithMemTableSize(1), WithL0CompactionTrigger(100))
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	var batch WriteBatch
	batch.Put([]byte("a"), []byte("1"))
	batch.Put([]byte("b"), []byte("2"))
	batch.Delete([]byte("b"))
	if err := engine.Write(&batch, WriteOptions{}); err != nil {
		t.Fatalf("Write error = %v", err)
	}
	value, ok, err := engine.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get(a) error = %v", err)
	}
	if !ok || string(value) != "1" {
		t.Fatalf("Get(a) = (%q, %v), want (1, true)", value, ok)
	}
	if value, ok, err := engine.Get([]byte("b")); err != nil || ok || value != nil {
		t.Fatalf("Get(b) = (%q, %v, %v), want nil false nil", value, ok, err)
	}
	snapshot, err := engine.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error = %v", err)
	}
	defer func() { _ = snapshot.Close() }()
	batch.Reset()
	batch.Put([]byte("a"), []byte("2"))
	if err := engine.Write(&batch, WriteOptions{}); err != nil {
		t.Fatalf("Write overwrite error = %v", err)
	}
	value, ok, err = snapshot.Get([]byte("a"))
	if err != nil {
		t.Fatalf("snapshot Get(a) error = %v", err)
	}
	if !ok || string(value) != "1" {
		t.Fatalf("snapshot Get(a) = (%q, %v), want (1, true)", value, ok)
	}
}

func countFiles(t *testing.T, dir, pattern string) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatalf("Glob(%s) error = %v", pattern, err)
	}
	count := 0
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			t.Fatalf("Stat(%s) error = %v", match, err)
		}
		if !info.IsDir() {
			count++
		}
	}
	return count
}

type fakeWALFactory struct {
	appendErr error
}

func (f fakeWALFactory) Open(string, uint64, walOptions) (walStore, error) {
	return &fakeWAL{appendErr: f.appendErr}, nil
}

type fakeWAL struct {
	appendErr error
	batches   []batch
}

func (w *fakeWAL) Append(batch batch, _ bool) error {
	if w.appendErr != nil {
		return w.appendErr
	}
	w.batches = append(w.batches, batch.Clone())
	return nil
}

func (w *fakeWAL) Replay(fn func(batch) error) error {
	for _, batch := range slices.Clone(w.batches) {
		if err := fn(batch.Clone()); err != nil {
			return err
		}
	}
	return nil
}

func (w *fakeWAL) Purge(uint64) error {
	return nil
}

func (w *fakeWAL) Close() error {
	return nil
}

type failingTableManager struct {
	buildErr error
}

func (m *failingTableManager) Build(context.Context, uint64, int, []entry) (tableMeta, error) {
	return tableMeta{}, m.buildErr
}

func (m *failingTableManager) Open(tableMeta) (tableReader, error) {
	return nil, m.buildErr
}

func (m *failingTableManager) Remove(uint64) error {
	return nil
}
