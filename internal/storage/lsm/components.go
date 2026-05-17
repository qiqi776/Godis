package lsm

import (
	"context"
	"time"

	"mini-kv/internal/storage/lsm/manifest"
	"mini-kv/internal/storage/lsm/memtable"
	"mini-kv/internal/storage/lsm/sstable"
	"mini-kv/internal/storage/lsm/wal"
)

// walOptions 是传递给 WAL 工厂的配置参数。
type walOptions struct {
	SegmentSize int64 // 单个 WAL 段文件的最大字节数
}

// walStore 抽象了预写日志的必需操作。
type walStore interface {
	Append(b batch, syncWrite bool) error
	Replay(func(batch) error) error
	Purge(flushedSeq uint64) error
	Close() error
}

// walFactory 用于创建 walStore 实例，支持依赖注入。
type walFactory interface {
	Open(dir string, fileNum uint64, opts walOptions) (walStore, error)
}

// mutableMemTable 定义可读写内存表的接口。
type mutableMemTable interface {
	Get(key []byte, seq uint64) (entry, bool)
	Apply(entries []entry) error
	NewIterator(seq uint64, bounds keyBounds) internalIterator
	ApproximateSize() int64
	Freeze() immutableMemTable
	Entries() []entry
}

// immutableMemTable 定义只读内存表的接口，由 Freeze 生成。
type immutableMemTable interface {
	Get(key []byte, seq uint64) (entry, bool)
	NewIterator(seq uint64, bounds keyBounds) internalIterator
	ApproximateSize() int64
	Entries() []entry
}

// memTableFactory 用于创建新的可读写内存表。
type memTableFactory interface {
	NewMutable() mutableMemTable
}

// tableManager 管理 SSTable 文件的构建、打开和删除。
type tableManager interface {
	Build(ctx context.Context, fileNum uint64, level int, entries []entry) (tableMeta, error)
	Open(meta tableMeta) (tableReader, error)
	Remove(fileNum uint64) error
}

// tableReader 提供对单个 SSTable 的读取能力。
type tableReader interface {
	Get(key []byte, seq uint64) (entry, bool, error)
	NewIterator(seq uint64, bounds keyBounds) (internalIterator, error)
	Entries() ([]entry, error)
	Close() error
}

// manifestStore 持久化并管理 LSM 版本状态。
type manifestStore interface {
	Load() (*versionState, error)
	Apply(edit versionEdit) error
	Close() error
}

// manifestFactory 用于创建 manifestStore 实例。
type manifestFactory interface {
	Open(dir string, fileNum uint64) (manifestStore, error)
}

// clock 抽象时间获取，便于测试时注入模拟时钟。
type clock interface {
	Now() time.Time
}

// internalIterator 是内部组件使用的迭代器接口，支持定位和遍历。
type internalIterator interface {
	First() bool
	Seek(key []byte) bool
	Next() bool
	Valid() bool
	Entry() entry
	Err() error
	Close() error
}

// components 聚合了所有可替换的底层组件工厂，实现依赖注入。
// 若某项为 nil，则在 withDefaults 中使用默认实现。
type components struct {
	WALFactory      walFactory
	MemTableFactory memTableFactory
	TableManager    tableManager
	ManifestFactory manifestFactory
	Clock           clock
}

// defaultComponents 返回基于真实实现的默认组件集合。
func defaultComponents(dir string, opts Options) components {
	return components{
		WALFactory: walFactoryFunc(func(dir string, fileNum uint64, opts walOptions) (walStore, error) {
			return wal.Open(dir, fileNum, wal.Options{SegmentSize: opts.SegmentSize})
		}),
		MemTableFactory: memTableFactoryFunc(func() mutableMemTable {
			return &memTableAdapter{table: memtable.New()}
		}),
		TableManager:    &tableManagerAdapter{manager: sstable.NewManager(dir, sstable.Options{BlockSize: opts.BlockSize})},
		ManifestFactory: manifestFactoryFunc(func(dir string, fileNum uint64) (manifestStore, error) {
			return manifest.Open(dir, fileNum)
		}),
		Clock: systemClock{},
	}
}

// withDefaults 用默认值填充 components 中未设置的字段。
func (c components) withDefaults(dir string, opts Options) components {
	defaults := defaultComponents(dir, opts)
	if c.WALFactory == nil {
		c.WALFactory = defaults.WALFactory
	}
	if c.MemTableFactory == nil {
		c.MemTableFactory = defaults.MemTableFactory
	}
	if c.TableManager == nil {
		c.TableManager = defaults.TableManager
	}
	if c.ManifestFactory == nil {
		c.ManifestFactory = defaults.ManifestFactory
	}
	if c.Clock == nil {
		c.Clock = defaults.Clock
	}
	return c
}

// memTableFactoryFunc 是一个函数适配器，实现 memTableFactory 接口。
type memTableFactoryFunc func() mutableMemTable

func (f memTableFactoryFunc) NewMutable() mutableMemTable {
	return f()
}

// walFactoryFunc 是一个函数适配器，实现 walFactory 接口。
type walFactoryFunc func(dir string, fileNum uint64, opts walOptions) (walStore, error)

func (f walFactoryFunc) Open(dir string, fileNum uint64, opts walOptions) (walStore, error) {
	return f(dir, fileNum, opts)
}

// manifestFactoryFunc 是一个函数适配器，实现 manifestFactory 接口。
type manifestFactoryFunc func(dir string, fileNum uint64) (manifestStore, error)

func (f manifestFactoryFunc) Open(dir string, fileNum uint64) (manifestStore, error) {
	return f(dir, fileNum)
}

// tableManagerAdapter 将 *sstable.Manager 适配为 tableManager 接口。
type tableManagerAdapter struct {
	manager *sstable.Manager
}

func (m *tableManagerAdapter) Build(ctx context.Context, fileNum uint64, level int, entries []entry) (tableMeta, error) {
	return m.manager.Build(ctx, fileNum, level, entries)
}

func (m *tableManagerAdapter) Open(meta tableMeta) (tableReader, error) {
	reader, err := m.manager.Open(meta)
	if err != nil {
		return nil, err
	}
	return &tableReaderAdapter{reader: reader}, nil
}

func (m *tableManagerAdapter) Remove(fileNum uint64) error {
	return m.manager.Remove(fileNum)
}

// tableReaderAdapter 将 *sstable.Reader 适配为 tableReader 接口。
type tableReaderAdapter struct {
	reader *sstable.Reader
}

func (r *tableReaderAdapter) Get(key []byte, seq uint64) (entry, bool, error) {
	return r.reader.Get(key, seq)
}

func (r *tableReaderAdapter) NewIterator(seq uint64, bounds keyBounds) (internalIterator, error) {
	return r.reader.NewIterator(seq, bounds)
}

func (r *tableReaderAdapter) Entries() ([]entry, error) {
	return r.reader.Entries()
}

func (r *tableReaderAdapter) Close() error {
	return r.reader.Close()
}

// memTableAdapter 将 *memtable.Table 适配为 mutableMemTable 接口。
type memTableAdapter struct {
	table *memtable.Table
}

func (m *memTableAdapter) Get(key []byte, seq uint64) (entry, bool) {
	return m.table.Get(key, seq)
}

func (m *memTableAdapter) Apply(entries []entry) error {
	return m.table.Apply(entries)
}

func (m *memTableAdapter) NewIterator(seq uint64, bounds keyBounds) internalIterator {
	return m.table.NewIterator(seq, bounds)
}

func (m *memTableAdapter) ApproximateSize() int64 {
	return m.table.ApproximateSize()
}

func (m *memTableAdapter) Freeze() immutableMemTable {
	return &immutableMemTableAdapter{table: m.table.Freeze()}
}

func (m *memTableAdapter) Entries() []entry {
	return m.table.Entries()
}

// immutableMemTableAdapter 将 *memtable.Immutable 适配为 immutableMemTable 接口。
type immutableMemTableAdapter struct {
	table *memtable.Immutable
}

func (m *immutableMemTableAdapter) Get(key []byte, seq uint64) (entry, bool) {
	return m.table.Get(key, seq)
}

func (m *immutableMemTableAdapter) NewIterator(seq uint64, bounds keyBounds) internalIterator {
	return m.table.NewIterator(seq, bounds)
}

func (m *immutableMemTableAdapter) ApproximateSize() int64 {
	return m.table.ApproximateSize()
}

func (m *immutableMemTableAdapter) Entries() []entry {
	return m.table.Entries()
}

// systemClock 使用标准库 time.Now 的真实时钟。
type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}