package lsm

import (
	"context"
	"errors"
	"fmt"

	"mini-kv/internal/storage/lsm/compact"
	"mini-kv/internal/storage/lsm/record"
)

// flushRequest 表示一次刷写请求，可选地携带用于接收结果的错误通道。
type flushRequest struct {
	errCh chan<- error
}

// compactionRequest 表示一次合并请求，可选地携带用于接收结果的错误通道。
type compactionRequest struct {
	errCh chan<- error
}

// compactionJob 描述一次合并任务的目标层级。
type compactionJob struct {
	level int
}

// startWorkers 启动后台刷写和合并协程。
func (e *Engine) startWorkers(ctx context.Context) {
	e.wg.Add(2)
	go e.flushWorker(ctx)
	go e.compactionWorker(ctx)
}

// requestFlush 向刷写通道发送一个非阻塞请求，若通道已满或引擎已停止则忽略。
func (e *Engine) requestFlush() {
	if e.backgroundError() != nil {
		return
	}
	select {
	case <-e.doneCh:
		return
	case e.flushCh <- flushRequest{}:
	default:
	}
}

// requestCompaction 向合并通道发送一个非阻塞请求，若通道已满或引擎已停止则忽略。
func (e *Engine) requestCompaction() {
	if e.backgroundError() != nil {
		return
	}
	select {
	case <-e.doneCh:
		return
	case e.compactCh <- compactionRequest{}:
	default:
	}
}

// flushWorker 是后台刷写协程，循环处理刷写请求并执行 flushMemTables。
func (e *Engine) flushWorker(ctx context.Context) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-e.flushCh:
			err := e.flushMemTables(ctx)
			e.setBackgroundError(err)
			if request.errCh != nil {
				select {
				case request.errCh <- err:
				case <-ctx.Done():
				}
			}
		}
	}
}

// compactionWorker 是后台合并协程，循环处理合并请求并执行 runCompaction。
func (e *Engine) compactionWorker(ctx context.Context) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-e.compactCh:
			err := e.runCompaction(ctx, compactionJob{level: 0})
			e.setBackgroundError(err)
			if request.errCh != nil {
				select {
				case request.errCh <- err:
				case <-ctx.Done():
				}
			}
		}
	}
}

// flushMemTables 将当前活跃 MemTable 冻结并刷写到 Level 0 的 SSTable。
// 刷写完成后更新 MANIFEST，清理 WAL，并在必要时触发合并。
func (e *Engine) flushMemTables(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return wrapContext("flush canceled", err)
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	// 冻结活跃 MemTable，若未超过不可变表数量限制则生成新的活跃表
	e.memMu.Lock()
	if e.mem != nil && e.mem.ApproximateSize() > 0 && len(e.imm) < e.opts.MaxImmutableTables {
		e.imm = append(e.imm, e.mem.Freeze())
		e.mem = e.memTableFactory.NewMutable()
		e.publishViewLocked()
	}
	if len(e.imm) == 0 {
		e.memMu.Unlock()
		return nil
	}
	// 取最早的不可变表进行刷写
	immutable := e.imm[0]
	entries := immutable.Entries()
	e.memMu.Unlock()

	if len(entries) == 0 {
		// 空表直接移除
		e.memMu.Lock()
		if len(e.imm) > 0 && e.imm[0] == immutable {
			e.imm = e.imm[1:]
			e.publishViewLocked()
		}
		e.memMu.Unlock()
		return nil
	}

	if e.tables == nil || e.manifest == nil {
		return ErrNotImplemented
	}

	// 分配文件编号并构建 SSTable
	fileNum := e.allocateFileNum()
	meta, err := e.tables.Build(ctx, fileNum, 0, entries)
	if err != nil {
		return wrapSSTableCorrupt("build", err)
	}

	// 生成版本变更记录
	edit := versionEdit{
		NextFileNum: e.nextFileNum.Load(),
		LastSeq:     maxSeq(entries),
		Added:       []tableMeta{meta},
	}
	if err := e.manifest.Apply(edit); err != nil {
		// MANIFEST 写入失败，删除已生成的 SSTable
		removeErr := e.tables.Remove(fileNum)
		if removeErr != nil {
			return errors.Join(fmt.Errorf("manifest apply flush: %w", err), wrapSSTableCorrupt("remove uncommitted flush output", removeErr))
		}
		return fmt.Errorf("manifest apply flush: %w", err)
	}
	e.publishVersion(edit)

	// 刷写成功后清理对应序列号之前的 WAL
	if e.wal != nil {
		if err := e.wal.Purge(edit.LastSeq); err != nil {
			return wrapWAL("purge", err)
		}
	}

	// 从不可变表列表中移除已刷写的表
	e.memMu.Lock()
	if len(e.imm) > 0 && e.imm[0] == immutable {
		e.imm = e.imm[1:]
		e.publishViewLocked()
	}
	e.memMu.Unlock()

	// 检查 Level 0 文件数是否达到触发合并的阈值
	if l0Count := len(e.currentVersion().FilesInRange(0, nil, nil)); l0Count >= e.opts.L0CompactionTrigger {
		e.requestCompaction()
	}
	return nil
}

// runCompaction 执行一次合并任务，将 Level 0 的全部文件合并到 Level 1。
// 合并过程：读取所有输入文件条目 → 去重保留最新可见版本 → 生成新 SSTable → 更新 MANIFEST → 删除旧文件。
func (e *Engine) runCompaction(ctx context.Context, job compactionJob) error {
	if err := ctx.Err(); err != nil {
		return wrapContext("compaction canceled", err)
	}
	if e.tables == nil || e.manifest == nil {
		return ErrNotImplemented
	}
	if job.level < 0 {
		return fmt.Errorf("%w: negative compaction level", ErrInvalidState)
	}

	state := e.currentVersion()
	// 使用 Picker 选择需要合并的 Level 0 文件
	picked, ok := (compact.Picker{L0Trigger: e.opts.L0CompactionTrigger}).Pick(state)
	if !ok || picked.Level != job.level {
		return nil
	}

	inputs := make([]tableMeta, len(picked.Inputs))
	for i := range picked.Inputs {
		inputs[i] = picked.Inputs[i].Clone()
	}

	// 读取所有输入文件的条目
	entries := make([]entry, 0)
	for _, meta := range inputs {
		reader, err := e.tables.Open(meta)
		if err != nil {
			return wrapSSTableCorrupt("open compaction input", err)
		}
		tableEntries, err := reader.Entries()
		closeErr := reader.Close()
		if err != nil {
			return wrapSSTableCorrupt("read compaction input", err)
		}
		if closeErr != nil {
			return wrapSSTableCorrupt("close compaction input", closeErr)
		}
		entries = append(entries, tableEntries...)
	}

	// 合并条目，去重并保留最新可见版本
	merged, err := mergeVisibleEntries(entries)
	if err != nil {
		return err
	}
	if len(merged) == 0 {
		return nil
	}

	// 构建合并后的新 SSTable，放入下一层
	fileNum := e.allocateFileNum()
	outputLevel := job.level + 1
	meta, err := e.tables.Build(ctx, fileNum, outputLevel, merged)
	if err != nil {
		return wrapSSTableCorrupt("build compaction output", err)
	}

	// 准备被删除的旧文件列表
	deleted := make([]uint64, 0, len(inputs))
	for _, input := range inputs {
		deleted = append(deleted, input.FileNum)
	}

	edit := versionEdit{
		NextFileNum: e.nextFileNum.Load(),
		LastSeq:     maxSeq(merged),
		Added:       []tableMeta{meta},
		Deleted:     deleted,
	}
	if err := e.manifest.Apply(edit); err != nil {
		// MANIFEST 写入失败，删除新生成的 SSTable
		removeErr := e.tables.Remove(fileNum)
		if removeErr != nil {
			return errors.Join(fmt.Errorf("manifest apply compaction: %w", err), wrapSSTableCorrupt("remove uncommitted compaction output", removeErr))
		}
		return fmt.Errorf("manifest apply compaction: %w", err)
	}
	e.publishVersion(edit)

	// 删除旧的 SSTable 文件
	if err := e.removeTables(deleted); err != nil {
		return err
	}
	return nil
}

// removeTables 批量删除指定的 SSTable 文件，收集所有错误并合并返回。
func (e *Engine) removeTables(fileNums []uint64) error {
	var err error
	for _, fileNum := range fileNums {
		if removeErr := e.tables.Remove(fileNum); removeErr != nil {
			err = errors.Join(err, wrapSSTableCorrupt("remove obsolete table", removeErr))
		}
	}
	return err
}

// maxSeq 返回条目切片中的最大序列号。
func maxSeq(entries []record.Entry) uint64 {
	var max uint64
	for _, entry := range entries {
		if entry.Seq > max {
			max = entry.Seq
		}
	}
	return max
}