package lsm

import "fmt"

// 默认配置常量
const (
	defaultMemTableSize        = 64 << 20 // 活跃 MemTable 的刷写阈值，默认 64MB
	defaultBlockSize           = 32 << 10 // SSTable 数据块大小，默认 32KB
	defaultMaxImmutableTables  = 2        // 最大不可变 MemTable 数量，防止写入阻塞
	defaultL0CompactionTrigger = 4        // Level 0 文件数达到该值时触发合并
	defaultMaxLevels           = 4        // 最大层级数
)

// Options 包含引擎所有可配置项。
type Options struct {
	MemTableSize        int64 // 活跃 MemTable 大小阈值（字节）
	WALSegmentSize      int64 // 单个 WAL 段文件的最大字节数
	BlockSize           int   // SSTable 数据块大小（字节）
	MaxImmutableTables  int   // 最大不可变 MemTable 数量
	L0CompactionTrigger int   // 触发 Level 0 合并的文件数阈值
	MaxLevels           int   // 最大层级深度
	SyncWrites          bool  // 是否每次写入均同步刷盘（保证持久性）
}

// Option 是用于修改 Options 的函数选项类型。
type Option func(*Options) error

// WithMemTableSize 设置活跃 MemTable 的大小阈值。
func WithMemTableSize(size int64) Option {
	return func(opts *Options) error {
		opts.MemTableSize = size
		return nil
	}
}

// WithBlockSize 设置 SSTable 数据块大小。
func WithBlockSize(size int) Option {
	return func(opts *Options) error {
		opts.BlockSize = size
		return nil
	}
}

// WithWALSegmentSize 设置单个 WAL 段文件的大小。
func WithWALSegmentSize(size int64) Option {
	return func(opts *Options) error {
		opts.WALSegmentSize = size
		return nil
	}
}

// WithMaxImmutableTables 设置最大不可变 MemTable 数量。
func WithMaxImmutableTables(n int) Option {
	return func(opts *Options) error {
		opts.MaxImmutableTables = n
		return nil
	}
}

// WithL0CompactionTrigger 设置触发 Level 0 合并的文件数量阈值。
func WithL0CompactionTrigger(n int) Option {
	return func(opts *Options) error {
		opts.L0CompactionTrigger = n
		return nil
	}
}

// WithMaxLevels 设置 LSM 树的最大层级数。
func WithMaxLevels(n int) Option {
	return func(opts *Options) error {
		opts.MaxLevels = n
		return nil
	}
}

// WithSyncWrites 控制每次写入是否立即刷盘。
func WithSyncWrites(enabled bool) Option {
	return func(opts *Options) error {
		opts.SyncWrites = enabled
		return nil
	}
}

// defaultOptions 返回所有配置项的默认值。
func defaultOptions() Options {
	return Options{
		MemTableSize:        defaultMemTableSize,
		WALSegmentSize:      defaultMemTableSize, // WAL 段大小与 MemTable 相同
		BlockSize:           defaultBlockSize,
		MaxImmutableTables:  defaultMaxImmutableTables,
		L0CompactionTrigger: defaultL0CompactionTrigger,
		MaxLevels:           defaultMaxLevels,
	}
}

// applyOptions 应用一组 Option 函数并返回经过校验的配置。
func applyOptions(options []Option) (Options, error) {
	opts := defaultOptions()
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&opts); err != nil {
			return Options{}, err
		}
	}
	if err := validateOptions(opts); err != nil {
		return Options{}, err
	}
	return opts, nil
}

// validateOptions 检查配置项的合法性，确保所有值均为正数。
func validateOptions(opts Options) error {
	switch {
	case opts.MemTableSize <= 0:
		return fmt.Errorf("%w: memtable size must be positive", ErrInvalidOptions)
	case opts.WALSegmentSize <= 0:
		return fmt.Errorf("%w: wal segment size must be positive", ErrInvalidOptions)
	case opts.BlockSize <= 0:
		return fmt.Errorf("%w: block size must be positive", ErrInvalidOptions)
	case opts.MaxImmutableTables <= 0:
		return fmt.Errorf("%w: max immutable tables must be positive", ErrInvalidOptions)
	case opts.L0CompactionTrigger <= 0:
		return fmt.Errorf("%w: l0 compaction trigger must be positive", ErrInvalidOptions)
	case opts.MaxLevels <= 0:
		return fmt.Errorf("%w: max levels must be positive", ErrInvalidOptions)
	default:
		return nil
	}
}