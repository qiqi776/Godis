package lsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"mini-kv/internal/storage/lsm/record"
)

// Engine 是完整的 LSM 存储引擎实现
type Engine struct {
	dir  string         // 数据目录
	opts Options        // 合并后的配置
	deps components     // 依赖组件（工厂等）
	lock *directoryLock // 目录文件锁，防止多实例冲突

	writeMu sync.Mutex   // 写操作互斥锁，保证写入串行化
	memMu   sync.RWMutex // 内存表结构锁，保护 mem 与 imm 切片

	walFactory      walFactory              // WAL 工厂，可注入
	memTableFactory memTableFactory         // MemTable 工厂
	wal             walStore                // 当前 WAL 实例
	mem             mutableMemTable         // 活跃内存表，接收写入
	imm             []immutableMemTable     // 不可变内存表队列，等待刷盘
	tables          tableManager            // SSTable 管理器
	manifest        manifestStore           // MANIFEST 持久化
	clock           clock                   // 时钟，便于测试
	view            atomic.Pointer[memView] // 原子指针，指向最新的内存视图
	lastSeq         atomic.Uint64           // 已分配的最大序列号
	nextFileNum     atomic.Uint64           // 下一个可用的文件编号
	versionMu       sync.RWMutex            // 版本状态锁
	version         *versionState           // 当前文件版本状态

	lifecycleMu sync.RWMutex           // 生命周期锁，控制关闭、后台错误等
	isClosed    bool                   // 是否已关闭
	errMu       sync.RWMutex           // 保护后台错误
	fatalErr    error                  // 后台致命错误，引擎不可用
	doneCh      chan struct{}          // 关闭通知通道
	cancel      context.CancelFunc     // 后台协程取消函数
	wg          sync.WaitGroup         // 后台协程等待
	flushCh     chan flushRequest      // 刷写请求通道，容量为1，非阻塞发送
	compactCh   chan compactionRequest // 合并请求通道，容量为1
}

// memView 是内存表的快照视图，供读取使用
type memView struct {
	mutable   mutableMemTable     // 活跃内存表
	immutable []immutableMemTable // 不可变内存表列表
}

// Open 使用默认上下文打开引擎
func Open(dir string, opts ...Option) (*Engine, error) {
	return OpenWithContext(context.TODO(), dir, opts...)
}

// OpenWithContext 带上下文打开引擎
func OpenWithContext(ctx context.Context, dir string, opts ...Option) (*Engine, error) {
	return openWithComponentsContext(ctx, dir, components{}, opts...)
}

// openWithComponents 使用注入的组件（无上下文）
func openWithComponents(dir string, deps components, opts ...Option) (*Engine, error) {
	return openWithComponentsContext(context.TODO(), dir, deps, opts...)
}

// openWithComponentsContext 是真正的构造入口，接收上下文与组件
func openWithComponentsContext(ctx context.Context, dir string, deps components, opts ...Option) (*Engine, error) {
	return newEngine(ctx, dir, deps, opts...)
}

// newEngine 初始化引擎并启动后台协程
func newEngine(ctx context.Context, dir string, deps components, opts ...Option) (_ *Engine, retErr error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	// 解析并校验配置
	options, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}
	// 用默认值填充未设置的依赖
	deps = deps.withDefaults(dir, options)
	if deps.MemTableFactory == nil {
		return nil, fmt.Errorf("%w: missing memtable factory", ErrInvalidState)
	}
	// 获取目录排他锁
	lock, err := acquireDirectoryLock(dir)
	if err != nil {
		return nil, err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	engine := &Engine{
		dir:             dir,
		opts:            options,
		deps:            deps,
		lock:            lock,
		walFactory:      deps.WALFactory,
		memTableFactory: deps.MemTableFactory,
		tables:          deps.TableManager,
		clock:           deps.Clock,
		doneCh:          make(chan struct{}),
		cancel:          cancel,
		flushCh:         make(chan flushRequest, 1),
		compactCh:       make(chan compactionRequest, 1),
	}
	openOK := false
	defer func() {
		if !openOK {
			// 若打开失败，清理已分配资源
			retErr = errors.Join(retErr, engine.closeOpenResources())
		}
	}()

	// 创建活跃内存表
	engine.mem = engine.memTableFactory.NewMutable()
	if engine.mem == nil {
		return nil, fmt.Errorf("%w: nil memtable", ErrInvalidState)
	}

	// 初始化 WAL（如果工厂存在）
	if engine.walFactory != nil {
		wal, err := engine.walFactory.Open(dir, 1, walOptions{SegmentSize: options.WALSegmentSize})
		if err != nil {
			return nil, wrapWAL("open", err)
		}
		engine.wal = wal
	}

	// 初始化 MANIFEST
	if deps.ManifestFactory != nil {
		manifest, err := deps.ManifestFactory.Open(dir, 1)
		if err != nil {
			return nil, fmt.Errorf("manifest open: %w", err)
		}
		engine.manifest = manifest
	}

	// 加载或初始化版本状态
	state := (&versionState{NextFileNum: 1}).Clone()
	if engine.manifest != nil {
		loaded, err := engine.manifest.Load()
		if err != nil {
			return nil, fmt.Errorf("manifest load: %w", err)
		}
		state = loaded.Clone()
	}
	engine.version = state
	engine.nextFileNum.Store(state.NextFileNum)
	engine.lastSeq.Store(state.LastSeq)

	// 回放 WAL 恢复 MemTable
	if engine.wal != nil {
		if err := engine.wal.Replay(func(replayed batch) error {
			entries := make([]entry, 0, len(replayed.Entries))
			for _, item := range replayed.Entries {
				if item.Seq > engine.lastSeq.Load() {
					entries = append(entries, item.Clone())
				}
			}
			if len(entries) == 0 {
				return nil
			}
			if err := engine.mem.Apply(entries); err != nil {
				return fmt.Errorf("replay memtable apply: %w", err)
			}
			for _, item := range entries {
				if item.Seq > engine.lastSeq.Load() {
					engine.lastSeq.Store(item.Seq)
				}
			}
			return nil
		}); err != nil {
			return nil, wrapWAL("replay", err)
		}
	}

	// 发布初始内存视图并启动后台任务
	engine.publishViewLocked()
	engine.startWorkers(workerCtx)
	openOK = true
	return engine, nil
}

// Get 根据键查询值，执行快照读
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	e.lifecycleMu.RLock()
	defer e.lifecycleMu.RUnlock()
	if e.isClosed {
		return nil, false, ErrClosed
	}
	if err := e.backgroundError(); err != nil {
		return nil, false, err
	}

	readSeq := e.lastSeq.Load() // 获取读取快照的序列号
	view := e.loadView()        // 加载当前内存视图
	e.memMu.RLock()
	value, ok, matched, err := getFromView(view, key, readSeq) // 先在内存中查找
	e.memMu.RUnlock()
	if err != nil || matched {
		return value, ok, err
	}
	return e.getFromTables(key, readSeq) // 未命中则查询 SSTable
}

// Write 将 WriteBatch 原子写入引擎
func (e *Engine) Write(writeBatch *WriteBatch, options WriteOptions) error {
	if err := validateWriteBatch(writeBatch); err != nil {
		return err
	}

	e.lifecycleMu.RLock()
	defer e.lifecycleMu.RUnlock()
	if e.isClosed {
		return ErrClosed
	}
	if err := e.backgroundError(); err != nil {
		return err
	}
	if writeBatch == nil || len(writeBatch.Ops) == 0 {
		return nil
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	// 分配序列号
	seqStart := e.lastSeq.Load() + 1
	recordBatch, err := makeRecordBatch(writeBatch, seqStart)
	if err != nil {
		return err
	}
	shouldSync := options.Sync || e.opts.SyncWrites
	// 先写 WAL
	if e.wal != nil {
		if err := e.wal.Append(recordBatch, shouldSync); err != nil {
			return wrapWAL("append", wrapIO("append record", err))
		}
	}

	// 再写 MemTable
	e.memMu.Lock()
	if err := e.mem.Apply(recordBatch.Entries); err != nil {
		e.memMu.Unlock()
		return fmt.Errorf("memtable apply: %w", err)
	}
	e.lastSeq.Store(seqStart + uint64(len(recordBatch.Entries)) - 1)
	// 检查是否需要冻结 MemTable
	rotated := e.rotateMemTableLocked()
	e.memMu.Unlock()

	// 若发生了冻结，触发刷写
	if rotated {
		e.requestFlush()
	}
	return nil
}

// NewIterator 创建一个范围迭代器，返回可见的键值对
func (e *Engine) NewIterator(options IterOptions) *Iterator {
	e.lifecycleMu.RLock()
	defer e.lifecycleMu.RUnlock()
	if e.isClosed {
		return newErrorIterator(ErrClosed)
	}
	if err := e.backgroundError(); err != nil {
		return newErrorIterator(err)
	}

	// 收集所有满足条件的可见条目
	entries, err := e.collectEntries(makeKeyBounds(options))
	if err != nil {
		return newErrorIterator(err)
	}
	return newSliceIterator(entries, nil)
}

// Snapshot 创建引擎的一致性快照，可反复查询
func (e *Engine) Snapshot() (*Snapshot, error) {
	e.lifecycleMu.RLock()
	defer e.lifecycleMu.RUnlock()
	if e.isClosed {
		return nil, ErrClosed
	}
	if err := e.backgroundError(); err != nil {
		return nil, err
	}

	// 收集所有条目，无键范围限制
	entries, err := e.collectEntries(keyBounds{})
	if err != nil {
		return nil, err
	}
	return newSnapshot(entries), nil
}

// Flush 手动触发刷写（同步等待完成）
func (e *Engine) Flush() error {
	return e.FlushWithContext(context.TODO())
}

// FlushWithContext 带上下文的刷写，可取消等待
func (e *Engine) FlushWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.TODO()
	}
	if err := ctx.Err(); err != nil {
		return wrapContext("flush canceled", err)
	}

	e.lifecycleMu.RLock()
	defer e.lifecycleMu.RUnlock()
	if e.isClosed {
		return ErrClosed
	}
	if err := e.backgroundError(); err != nil {
		return err
	}

	// 构造刷写请求，包含错误通道
	errCh := make(chan error, 1)
	request := flushRequest{errCh: errCh}
	select {
	case e.flushCh <- request:
	case <-e.doneCh:
		return ErrClosed
	case <-ctx.Done():
		return wrapContext("flush canceled", ctx.Err())
	}

	// 等待刷写完成或上下文取消
	select {
	case err := <-errCh:
		return err
	case <-e.doneCh:
		return ErrClosed
	case <-ctx.Done():
		return wrapContext("flush canceled", ctx.Err())
	}
}

// Close 优雅关闭引擎：
// 1. 标记引擎已关闭，关闭 doneCh 并取消后台协程
// 2. 等待所有后台协程退出
// 3. 依次关闭 WAL、MANIFEST，释放目录锁
func (e *Engine) Close() error {
	e.lifecycleMu.Lock()
	if e.isClosed {
		e.lifecycleMu.Unlock()
		return nil
	}
	e.isClosed = true
	close(e.doneCh)    // 通知所有阻塞在 doneCh 上的操作
	cancel := e.cancel // 保存取消函数
	e.lifecycleMu.Unlock()

	if cancel != nil {
		cancel() // 取消后台协程的 context
	}
	e.wg.Wait() // 等待所有后台协程结束

	var err error
	if e.wal != nil {
		err = errors.Join(err, wrapWAL("close", e.wal.Close()))
	}
	if e.manifest != nil {
		if closeErr := e.manifest.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("manifest close: %w", closeErr))
		}
	}
	if e.lock != nil {
		if releaseErr := e.lock.Release(); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release lsm lock: %w", releaseErr))
		}
	}
	return err
}

// closeOpenResources 在引擎打开失败时清理已初始化的资源，不等待后台协程（因为还未启动）
func (e *Engine) closeOpenResources() error {
	if e == nil {
		return nil
	}
	if e.cancel != nil {
		e.cancel()
	}
	var err error
	if e.wal != nil {
		err = errors.Join(err, wrapWAL("close", e.wal.Close()))
	}
	if e.manifest != nil {
		if closeErr := e.manifest.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("manifest close: %w", closeErr))
		}
	}
	if e.lock != nil {
		if releaseErr := e.lock.Release(); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release lsm lock: %w", releaseErr))
		}
	}
	return err
}

// backgroundError 返回引擎后台任务中发生的致命错误（如果有），调用者应据此拒绝后续操作
func (e *Engine) backgroundError() error {
	e.errMu.RLock()
	defer e.errMu.RUnlock()
	return e.fatalErr
}

// setBackgroundError 记录后台致命错误，忽略 context 取消和引擎关闭，只记录第一个致命错误
func (e *Engine) setBackgroundError(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) {
		return
	}
	e.errMu.Lock()
	defer e.errMu.Unlock()
	if e.fatalErr != nil {
		return // 已存在致命错误，只保留第一个
	}
	if errors.Is(err, ErrBackground) {
		e.fatalErr = err
		return
	}
	// 包装为 ErrBackground，保留原始错误链
	e.fatalErr = fmt.Errorf("%w: %w", ErrBackground, err)
}

// loadView 以原子方式加载当前内存视图快照，若尚未发布则返回空视图
func (e *Engine) loadView() *memView {
	view := e.view.Load()
	if view != nil {
		return view
	}
	return &memView{}
}

// publishViewLocked 在持有 memMu 时更新原子内存视图，供读取路径使用
func (e *Engine) publishViewLocked() {
	immutable := make([]immutableMemTable, len(e.imm))
	copy(immutable, e.imm) // 复制一份不可变表切片，避免共享底层数组
	e.view.Store(&memView{
		mutable:   e.mem,
		immutable: immutable,
	})
}

// rotateMemTableLocked 检查是否需要冻结活跃 MemTable，若大小超过阈值且未超过最大不可变表数量则执行冻结
// 返回 true 表示发生了冻结（可能需要触发刷写）
func (e *Engine) rotateMemTableLocked() bool {
	if e.mem == nil || e.mem.ApproximateSize() < e.opts.MemTableSize {
		return false
	}
	if len(e.imm) >= e.opts.MaxImmutableTables {
		return false // 不可变表已满，暂时不冻结
	}
	e.imm = append(e.imm, e.mem.Freeze())
	e.mem = e.memTableFactory.NewMutable()
	e.publishViewLocked()
	return true
}

// collectEntries 收集所有满足 readSeq 和 bounds 的可见条目，先收集内存视图，再收集 SSTable，最后合并
// 用于 Snapshot 和 NewIterator 构建一致性视图
func (e *Engine) collectEntries(bounds keyBounds) ([]entry, error) {
	readSeq := e.lastSeq.Load() // 确定快照序列号
	view := e.loadView()        // 内存视图
	e.memMu.RLock()
	defer e.memMu.RUnlock()
	entries, err := collectVisibleEntries(view, readSeq, bounds) // 内存层可见条目
	if err != nil {
		return nil, err
	}
	tableEntries, err := e.collectTableEntries(readSeq, bounds) // 磁盘层可见条目
	if err != nil {
		return nil, err
	}
	return mergeVisibleEntries(entries, tableEntries) // 合并两层的条目
}

// getFromView 在内存视图中查找指定键的可见版本，返回原始值或 nil
// 遍历顺序：活跃 MemTable → 不可变 MemTable（从新到旧）
func getFromView(view *memView, key []byte, readSeq uint64) ([]byte, bool, bool, error) {
	if view == nil {
		return nil, false, false, nil
	}
	// 先查活跃表（最新写入）
	if view.mutable != nil {
		if entry, ok := view.mutable.Get(key, readSeq); ok {
			value, visible, err := visibleValue(entry)
			return value, visible, true, err
		}
	}
	// 再从新到旧查不可变表
	for i := len(view.immutable) - 1; i >= 0; i-- {
		if view.immutable[i] == nil {
			continue
		}
		if entry, ok := view.immutable[i].Get(key, readSeq); ok {
			value, visible, err := visibleValue(entry)
			return value, visible, true, err
		}
	}
	return nil, false, false, nil
}

// visibleValue 根据 entry 的种类返回用户可见的值
// Put 返回实际值，Delete 返回不存在，未知类型返回错误
func visibleValue(entry entry) ([]byte, bool, error) {
	switch entry.Kind {
	case record.KindPut:
		return record.CloneBytes(entry.Value), true, nil
	case record.KindDelete:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("%w: unknown entry kind", ErrCorrupt)
	}
}

// makeKeyBounds 将外部的 IterOptions 转换为内部使用的 keyBounds，克隆边界键以保证安全
func makeKeyBounds(options IterOptions) keyBounds {
	return keyBounds{
		Lower: record.CloneBytes(options.LowerBound),
		Upper: record.CloneBytes(options.UpperBound),
	}
}

// currentVersion 返回当前版本状态的深拷贝，线程安全
func (e *Engine) currentVersion() *versionState {
	e.versionMu.RLock()
	defer e.versionMu.RUnlock()
	return e.version.Clone()
}

// publishVersion 应用一次版本编辑并更新内存中的版本状态及下一个文件编号
func (e *Engine) publishVersion(edit versionEdit) {
	e.versionMu.Lock()
	defer e.versionMu.Unlock()
	e.version = e.version.Apply(edit)
	e.nextFileNum.Store(e.version.NextFileNum)
}

// allocateFileNum 原子递增并返回下一个可用的 SSTable/WAL 文件编号
func (e *Engine) allocateFileNum() uint64 {
	return e.nextFileNum.Add(1) - 1
}

// getFromTables 在 SSTable 层查找指定键，遍历可能包含该键的所有文件（由 FilesForKey 提供），
// 找到第一个可见版本即返回
func (e *Engine) getFromTables(key []byte, readSeq uint64) ([]byte, bool, error) {
	if e.tables == nil {
		return nil, false, nil
	}
	state := e.currentVersion()
	for _, meta := range state.FilesForKey(key) { // 可能包含该键的文件列表
		reader, err := e.tables.Open(meta)
		if err != nil {
			return nil, false, wrapSSTableCorrupt("open", err)
		}
		entry, ok, getErr := reader.Get(key, readSeq)
		closeErr := reader.Close()
		if getErr != nil {
			return nil, false, wrapSSTableCorrupt("get", getErr)
		}
		if closeErr != nil {
			return nil, false, wrapSSTableCorrupt("close", closeErr)
		}
		if ok {
			return visibleValue(entry)
		}
	}
	return nil, false, nil
}

// collectTableEntries 从所有 SSTable 中收集满足 readSeq 和 bounds 的可见条目，用于构建快照/迭代器
func (e *Engine) collectTableEntries(readSeq uint64, bounds keyBounds) ([]entry, error) {
	if e.tables == nil {
		return nil, nil
	}
	state := e.currentVersion()
	files := state.AllFiles() // 获取全部文件元数据
	entries := make([]entry, 0)
	for _, meta := range files {
		// 快速排除键范围完全无交集的 SSTable
		if len(bounds.Upper) > 0 && bytes.Compare(meta.Smallest, bounds.Upper) >= 0 {
			continue
		}
		if len(bounds.Lower) > 0 && bytes.Compare(meta.Largest, bounds.Lower) < 0 {
			continue
		}
		reader, err := e.tables.Open(meta)
		if err != nil {
			return nil, wrapSSTableCorrupt("open", err)
		}
		tableEntries, err := reader.Entries() // 读取所有条目（当前实现一次性加载）
		closeErr := reader.Close()
		if err != nil {
			return nil, wrapSSTableCorrupt("entries", err)
		}
		if closeErr != nil {
			return nil, wrapSSTableCorrupt("close", closeErr)
		}
		// 过滤出可见且落在范围内的条目
		for _, item := range tableEntries {
			if item.Seq <= readSeq && bounds.Contains(item.Key) {
				entries = append(entries, item.Clone())
			}
		}
	}
	return entries, nil
}
