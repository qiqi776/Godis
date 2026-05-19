package sstable

import (
	"bytes"
	"sort"
)

// TableMeta 保存一个 SSTable 的元数据
type TableMeta struct {
	FileNum  uint64 // 唯一文件编号
	Level    int    // 合并层级
	Smallest []byte // 最小内部键
	Largest  []byte // 最大内部键
	MinSeq   uint64 // 最小序列号
	MaxSeq   uint64 // 最大序列号
	Size     int64  // 文件大小
}

// Edit 记录一次版本状态变更
type Edit struct {
	NextFileNum uint64      // 下一个可分配的文件编号
	LastSeq     uint64      // 最新序列号
	Added       []TableMeta // 本次新增的表
	Deleted     []uint64    // 要删除的文件编号
}

// State 是一个不可变、写时复制的层级集合
type State struct {
	NextFileNum uint64        // 下一个可分配的文件编号
	LastSeq     uint64        // 已消费的最新序列号
	Levels      [][]TableMeta // 各层级有序（L0 按文件号，其他层按键）
}

// 深拷贝元数据，复制键切片
func (m TableMeta) Clone() TableMeta {
	return TableMeta{
		FileNum:  m.FileNum,
		Level:    m.Level,
		Smallest: cloneBytes(m.Smallest),
		Largest:  cloneBytes(m.Largest),
		MinSeq:   m.MinSeq,
		MaxSeq:   m.MaxSeq,
		Size:     m.Size,
	}
}

// 深拷贝编辑记录
func (e Edit) Clone() Edit {
	added := make([]TableMeta, len(e.Added))
	for i := range e.Added {
		added[i] = e.Added[i].Clone()
	}
	return Edit{
		NextFileNum: e.NextFileNum,
		LastSeq:     e.LastSeq,
		Added:       added,
		Deleted:     append([]uint64(nil), e.Deleted...),
	}
}

// 深拷贝，保证 NextFileNum 至少为 1
func (s *State) Clone() *State {
	if s == nil {
		return &State{NextFileNum: 1}
	}
	levels := make([][]TableMeta, len(s.Levels))
	for i := range s.Levels {
		levels[i] = make([]TableMeta, len(s.Levels[i]))
		for j := range s.Levels[i] {
			levels[i][j] = s.Levels[i][j].Clone()
		}
	}
	nextFileNum := s.NextFileNum
	if nextFileNum == 0 {
		nextFileNum = 1
	}
	return &State{
		NextFileNum: nextFileNum,
		LastSeq:     s.LastSeq,
		Levels:      levels,
	}
}

// 将一次编辑应用到当前状态，返回新状态（不可变）
func (s *State) Apply(edit Edit) *State {
	next := s.Clone()
	if edit.NextFileNum > next.NextFileNum {
		next.NextFileNum = edit.NextFileNum
	}
	if edit.LastSeq > next.LastSeq {
		next.LastSeq = edit.LastSeq
	}
	// 删除指定文件
	for _, deleted := range edit.Deleted {
		for level := range next.Levels {
			next.Levels[level] = removeFile(next.Levels[level], deleted)
		}
	}
	// 添加新文件，并按层级排序
	for _, meta := range edit.Added {
		for len(next.Levels) <= meta.Level {
			next.Levels = append(next.Levels, nil)
		}
		next.Levels[meta.Level] = append(next.Levels[meta.Level], meta.Clone())
		sortLevel(next.Levels[meta.Level])
		if meta.FileNum >= next.NextFileNum {
			next.NextFileNum = meta.FileNum + 1
		}
		if meta.MaxSeq > next.LastSeq {
			next.LastSeq = meta.MaxSeq
		}
	}
	if next.NextFileNum == 0 {
		next.NextFileNum = 1
	}
	return next
}

// 返回所有可能包含指定键的文件（L0 按倒序，其他层顺序）
func (s *State) FilesForKey(key []byte) []TableMeta {
	if s == nil {
		return nil
	}
	var files []TableMeta
	for level, metas := range s.Levels {
		if level == 0 {
			// L0 文件可能重叠，从新到旧检查
			for i := len(metas) - 1; i >= 0; i-- {
				if contains(metas[i], key) {
					files = append(files, metas[i].Clone())
				}
			}
			continue
		}
		levelFiles := make([]TableMeta, 0)
		for _, meta := range metas {
			if contains(meta, key) {
				levelFiles = append(levelFiles, meta.Clone())
			}
		}
		sort.Slice(levelFiles, func(i, j int) bool {
			if levelFiles[i].MaxSeq != levelFiles[j].MaxSeq {
				return levelFiles[i].MaxSeq > levelFiles[j].MaxSeq
			}
			return levelFiles[i].FileNum > levelFiles[j].FileNum
		})
		files = append(files, levelFiles...)
	}
	return files
}

// 返回指定层级中与 [lower, upper] 范围重叠的所有文件
func (s *State) FilesInRange(level int, lower, upper []byte) []TableMeta {
	if s == nil || level < 0 || level >= len(s.Levels) {
		return nil
	}
	files := make([]TableMeta, 0)
	for _, meta := range s.Levels[level] {
		if overlaps(meta, lower, upper) {
			files = append(files, meta.Clone())
		}
	}
	return files
}

// 返回所有层级的所有文件
func (s *State) AllFiles() []TableMeta {
	if s == nil {
		return nil
	}
	files := make([]TableMeta, 0)
	for _, level := range s.Levels {
		for _, meta := range level {
			files = append(files, meta.Clone())
		}
	}
	return files
}

// 从文件切片中删除指定文件编号的记录
func removeFile(files []TableMeta, fileNum uint64) []TableMeta {
	out := files[:0]
	for _, meta := range files {
		if meta.FileNum != fileNum {
			out = append(out, meta)
		}
	}
	return out
}

// 对层级内文件排序（L0 按文件号，其它层先按最小键再按文件号）
func sortLevel(files []TableMeta) {
	for i := 1; i < len(files); i++ {
		for j := i; j > 0 && compareMeta(files[j-1], files[j]) > 0; j-- {
			files[j-1], files[j] = files[j], files[j-1]
		}
	}
}

// 比较两个 TableMeta 用于排序
func compareMeta(a, b TableMeta) int {
	if a.Level == 0 {
		switch {
		case a.FileNum < b.FileNum:
			return -1
		case a.FileNum > b.FileNum:
			return 1
		default:
			return 0
		}
	}
	if cmp := bytes.Compare(a.Smallest, b.Smallest); cmp != 0 {
		return cmp
	}
	switch {
	case a.FileNum < b.FileNum:
		return -1
	case a.FileNum > b.FileNum:
		return 1
	default:
		return 0
	}
}

// 判断 key 是否在 TableMeta 的键范围内
func contains(meta TableMeta, key []byte) bool {
	return bytes.Compare(key, meta.Smallest) >= 0 && bytes.Compare(key, meta.Largest) <= 0
}

// 判断 TableMeta 的键范围是否与 [lower, upper] 重叠
func overlaps(meta TableMeta, lower, upper []byte) bool {
	if len(upper) > 0 && bytes.Compare(meta.Smallest, upper) >= 0 {
		return false
	}
	if len(lower) > 0 && bytes.Compare(meta.Largest, lower) < 0 {
		return false
	}
	return true
}

// 复制字节切片
func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
