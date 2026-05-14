package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"mini-kv/internal/storage/lsm/record"
)

const (
	tableMagic  = uint64(0x4d4b565353543031) // SSTable 文件的魔数，用于格式校验
	footerSize  = 48						 // tableMagic 是 SSTable 文件的魔数，用于格式校验
	filePattern = "%06d.sst"				 // footerSize 是 SSTable 文件末尾固定区域的字节长度
)

type Manager struct {
    dir  string   // SSTable 文件存放目录
    opts Options
}

type Options struct {
    BlockSize  int  // Data Block 目标大小，默认 32KB
    BitsPerKey int  // 布隆过滤器每个 Key 的位数，默认 10
}

// NewManager 创建一个新的 SSTable 管理器，指定存储目录和配置选项
func NewManager(dir string, opts Options) *Manager {
	if opts.BlockSize <= 0 {
		opts.BlockSize = 32 << 10
	}
	if opts.BitsPerKey <= 0 {
		opts.BitsPerKey = 10
	}
	return &Manager{dir: dir, opts: opts}
}

// Build 将一组有序记录构建为新的 SSTable 文件，并返回表元数据
func (m *Manager) Build(ctx context.Context, fileNum uint64, level int, entries []record.Entry) (TableMeta, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if err := ctx.Err(); err != nil {
		return TableMeta{}, err
	}
	if len(entries) == 0 {
		return TableMeta{}, fmt.Errorf("%w: empty table", ErrInvalidIndex)
	}
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return TableMeta{}, fmt.Errorf("create sstable dir: %w", err)
	}
	// 确保输入记录按键排序
	sort.Slice(entries, func(i, j int) bool {
		return record.Compare(entries[i], entries[j]) < 0
	})

	writer, err := Create(filepath.Join(m.dir, FileName(fileNum)), fileNum, level, m.opts)
	if err != nil {
		return TableMeta{}, err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			_ = writer.Close()
			return TableMeta{}, err
		}
		if err := writer.Add(entry); err != nil {
			_ = writer.Close()
			return TableMeta{}, err
		}
	}
	return writer.Finish()
}

// Open 根据元数据打开现有的 SSTable 文件，返回一个读取器
func (m *Manager) Open(meta TableMeta) (*Reader, error) {
	return Open(filepath.Join(m.dir, FileName(meta.FileNum)), meta)
}

// Remove 删除指定文件编号的 SSTable 文件若文件不存在则视为成功
func (m *Manager) Remove(fileNum uint64) error {
	err := os.Remove(filepath.Join(m.dir, FileName(fileNum)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// FileName 根据文件编号返回标准的 SSTable 文件名
func FileName(fileNum uint64) string {
	return fmt.Sprintf(filePattern, fileNum)
}

// Writer 负责构建一个 SSTable 文件，支持顺序写入记录
type Writer struct {
	file     *os.File
	path     string
	fileNum  uint64
	level    int
	opts     Options
	block    []record.Entry   // 当前正在构建的数据块
	blockLen int              // 当前数据块的估算字节数
	index    []IndexEntry     // 已写入块的索引
	bloom    *BloomBuilder // 布隆过滤器构建器
	smallest []byte
	largest  []byte
	minSeq   uint64
	maxSeq   uint64
	count    int
	offset   uint64            // 已写入文件的总字节数
	closed   bool
}

// Create 创建一个新的 SSTable 文件并返回写入器
func Create(path string, fileNum uint64, level int, opts Options) (*Writer, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = 32 << 10
	}
	if opts.BitsPerKey <= 0 {
		opts.BitsPerKey = 10
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create sstable: %w", err)
	}
	return &Writer{
		file:    file,
		path:    path,
		fileNum: fileNum,
		level:   level,
		opts:    opts,
		bloom:   NewBloomBuilder(1024, opts.BitsPerKey),
	}, nil
}

// Add 向当前 SSTable 追加一条记录记录必须按内部键顺序添加
func (w *Writer) Add(entry record.Entry) error {
	if w.closed {
		return os.ErrClosed
	}
	entry = entry.Clone()
	// 检查顺序
	if len(w.block) > 0 && record.Compare(w.block[len(w.block)-1], entry) > 0 {
		return fmt.Errorf("%w: entries out of order", ErrInvalidIndex)
	}
	// 更新表的全局边界
	if len(w.smallest) == 0 {
		w.smallest = record.CloneBytes(entry.Key)
		w.minSeq = entry.Seq
	}
	w.largest = record.CloneBytes(entry.Key)
	if w.minSeq == 0 || entry.Seq < w.minSeq {
		w.minSeq = entry.Seq
	}
	if entry.Seq > w.maxSeq {
		w.maxSeq = entry.Seq
	}
	w.bloom.Add(entry.Key)
	w.block = append(w.block, entry)
	w.blockLen += encodedEntryLen(entry)
	w.count++
	if w.blockLen >= w.opts.BlockSize {
		return w.flushBlock()
	}
	return nil
}

// Finish 完成 SSTable 的构建，写入索引、布隆过滤器和页脚，返回表元数据
func (w *Writer) Finish() (TableMeta, error) {
	if w.closed {
		return TableMeta{}, os.ErrClosed
	}
	// 将最后一个数据块刷出
	if err := w.flushBlock(); err != nil {
		_ = w.Close()
		return TableMeta{}, err
	}
	// 写入索引区
	indexBytes, err := EncodeIndex(w.index)
	if err != nil {
		_ = w.Close()
		return TableMeta{}, err
	}
	indexOffset := w.offset
	if err := w.write(indexBytes); err != nil {
		_ = w.Close()
		return TableMeta{}, err
	}
	// 写入布隆过滤器区
	bloom := w.bloom.Finish()
	bloomBytes, err := bloom.MarshalBinary()
	if err != nil {
		_ = w.Close()
		return TableMeta{}, err
	}
	bloomOffset := w.offset
	if err := w.write(bloomBytes); err != nil {
		_ = w.Close()
		return TableMeta{}, err
	}
	// 构造页脚
	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], tableMagic)
	binary.LittleEndian.PutUint64(footer[8:16], indexOffset)
	binary.LittleEndian.PutUint32(footer[16:20], uint32(len(indexBytes)))
	binary.LittleEndian.PutUint64(footer[20:28], bloomOffset)
	binary.LittleEndian.PutUint32(footer[28:32], uint32(len(bloomBytes)))
	binary.LittleEndian.PutUint32(footer[32:36], uint32(w.count))
	if err := w.write(footer); err != nil {
		_ = w.Close()
		return TableMeta{}, err
	}
	// 强制刷盘
	if err := w.file.Sync(); err != nil {
		_ = w.Close()
		return TableMeta{}, fmt.Errorf("sync sstable: %w", err)
	}
	size := int64(w.offset)
	if err := w.Close(); err != nil {
		return TableMeta{}, err
	}
	return TableMeta{
		FileNum:  w.fileNum,
		Level:    w.level,
		Smallest: record.CloneBytes(w.smallest),
		Largest:  record.CloneBytes(w.largest),
		MinSeq:   w.minSeq,
		MaxSeq:   w.maxSeq,
		Size:     size,
	}, nil
}

// Close 关闭写入器，释放文件描述符
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.file.Close()
}

// flushBlock 将当前积累的数据块编码并写入文件
func (w *Writer) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	data, err := encodeBlock(w.block)
	if err != nil {
		return err
	}
	first := w.block[0].Key
	last := w.block[len(w.block)-1].Key
	entry := IndexEntry{
		FirstKey: record.CloneBytes(first),
		LastKey:  record.CloneBytes(last),
		Handle: BlockHandle{
			Offset: w.offset,
			Length: uint32(len(data)),
		},
	}
	if err := w.write(data); err != nil {
		return err
	}
	w.index = append(w.index, entry)
	w.block = w.block[:0]
	w.blockLen = 0
	return nil
}

// write 是文件写入的包装，更新写入偏移量
func (w *Writer) write(data []byte) error {
	n, err := w.file.Write(data)
	if err != nil {
		return fmt.Errorf("write sstable: %w", err)
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	w.offset += uint64(n)
	return nil
}

// Reader 用于读取已存在的 SSTable
type Reader struct {
	path  string
	meta  TableMeta
	index *Index
	bloom *Bloom
}

// Open 打开一个 SSTable 文件，解析元数据、索引和布隆过滤器，返回读取器
func Open(path string, meta TableMeta) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sstable: %w", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat sstable: %w", err)
	}
	if info.Size() < footerSize {
		return nil, fmt.Errorf("%w: short sstable", ErrInvalidIndex)
	}
	// 读取页脚
	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, info.Size()-footerSize); err != nil {
		return nil, fmt.Errorf("read sstable footer: %w", err)
	}
	// 验证魔数
	if binary.LittleEndian.Uint64(footer[0:8]) != tableMagic {
		return nil, fmt.Errorf("%w: bad sstable magic", ErrInvalidIndex)
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[8:16]))
	indexLength := int(binary.LittleEndian.Uint32(footer[16:20]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[20:28]))
	bloomLength := int(binary.LittleEndian.Uint32(footer[28:32]))

	// 读取索引
	indexBytes := make([]byte, indexLength)
	if _, err := file.ReadAt(indexBytes, indexOffset); err != nil {
		return nil, fmt.Errorf("read sstable index: %w", err)
	}
	index, err := DecodeIndex(indexBytes)
	if err != nil {
		return nil, err
	}
	// 读取布隆过滤器（若存在）
	var bloom *Bloom
	if bloomLength > 0 {
		bloomBytes := make([]byte, bloomLength)
		if _, err := file.ReadAt(bloomBytes, bloomOffset); err != nil {
			return nil, fmt.Errorf("read sstable bloom: %w", err)
		}
		bloom, err = DecodeBloom(bloomBytes)
		if err != nil {
			return nil, err
		}
	}
	return &Reader{path: path, meta: meta.Clone(), index: index, bloom: bloom}, nil
}

// Get 在 SSTable 中查找指定键首先通过布隆过滤器快速排除，然后使用索引定位数据块
func (r *Reader) Get(key []byte, readSeq uint64) (record.Entry, bool, error) {
	if r.bloom != nil && !r.bloom.MayContain(key) {
		return record.Entry{}, false, nil
	}
	handle, ok := r.index.Find(key)
	if !ok {
		return record.Entry{}, false, nil
	}
	entries, err := r.readBlock(handle)
	if err != nil {
		return record.Entry{}, false, err
	}
	for _, entry := range entries {
		if bytes.Equal(entry.Key, key) && entry.Seq <= readSeq {
			return entry.Clone(), true, nil
		}
	}
	return record.Entry{}, false, nil
}

// NewIterator 创建一个迭代器，仅返回序列号不超过 readSeq 且在键范围内的可见条目
func (r *Reader) NewIterator(readSeq uint64, bounds record.KeyBounds) (*Iterator, error) {
	entries, err := r.Entries()
	if err != nil {
		return nil, err
	}
	visible := make([]record.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Seq <= readSeq && bounds.Contains(entry.Key) {
			visible = append(visible, entry)
		}
	}
	return &Iterator{entries: visible, index: -1}, nil
}

// Entries 返回 SSTable 中所有记录（不进行序列号过滤）
func (r *Reader) Entries() ([]record.Entry, error) {
	indexEntries := r.index.Entries()
	entries := make([]record.Entry, 0)
	for _, indexEntry := range indexEntries {
		blockEntries, err := r.readBlock(indexEntry.Handle)
		if err != nil {
			return nil, err
		}
		entries = append(entries, blockEntries...)
	}
	return entries, nil
}

// Close 释放 Reader 的资源（当前为无操作，文件已关闭）
func (r *Reader) Close() error {
	return nil
}

// readBlock 从文件中读取并解码一个数据块
func (r *Reader) readBlock(handle BlockHandle) ([]record.Entry, error) {
	file, err := os.Open(r.path)
	if err != nil {
		return nil, fmt.Errorf("open sstable block: %w", err)
	}
	defer func() { _ = file.Close() }()
	data := make([]byte, handle.Length)
	if _, err := file.ReadAt(data, int64(handle.Offset)); err != nil {
		return nil, fmt.Errorf("read sstable block: %w", err)
	}
	return decodeBlock(data)
}

// Iterator 提供对 SSTable 记录的顺序访问
type Iterator struct {
	entries []record.Entry
	index   int
}

func (it *Iterator) First() bool {
	if len(it.entries) == 0 {
		it.index = -1
		return false
	}
	it.index = 0
	return true
}

func (it *Iterator) Seek(key []byte) bool {
	pos := sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].Key, key) >= 0
	})
	if pos >= len(it.entries) {
		it.index = -1
		return false
	}
	it.index = pos
	return true
}

func (it *Iterator) Next() bool {
	if it.index < 0 {
		return it.First()
	}
	it.index++
	if it.index >= len(it.entries) {
		it.index = -1
		return false
	}
	return true
}

func (it *Iterator) Valid() bool {
	return it.index >= 0 && it.index < len(it.entries)
}

func (it *Iterator) Entry() record.Entry {
	if !it.Valid() {
		return record.Entry{}
	}
	return it.entries[it.index].Clone()
}

func (it *Iterator) Err() error {
	return nil
}

func (it *Iterator) Close() error {
	it.entries = nil
	it.index = -1
	return nil
}

// encodeBlock 将记录切片编码为数据块的字节表示
func encodeBlock(entries []record.Entry) ([]byte, error) {
	size := 4 // 条目计数
	for _, entry := range entries {
		size += encodedEntryLen(entry)
	}
	out := make([]byte, 0, size)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(entries)))
	for _, entry := range entries {
		out = append(out, byte(entry.Kind))
		out = binary.LittleEndian.AppendUint64(out, entry.Seq)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.Key)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.Value)))
		out = append(out, entry.Key...)
		out = append(out, entry.Value...)
	}
	return out, nil
}

// decodeBlock 从字节切片解码出一个数据块的记录集合
func decodeBlock(data []byte) ([]record.Entry, error) {
	reader := blockReader{data: data}
	count, ok := reader.u32()
	if !ok {
		return nil, fmt.Errorf("%w: missing block count", ErrInvalidIndex)
	}
	entries := make([]record.Entry, 0, count)
	for i := uint32(0); i < count; i++ {
		kind, ok := reader.u8()
		if !ok {
			return nil, fmt.Errorf("%w: missing kind", ErrInvalidIndex)
		}
		seq, ok := reader.u64()
		if !ok {
			return nil, fmt.Errorf("%w: missing sequence", ErrInvalidIndex)
		}
		keyLen, ok := reader.u32()
		if !ok {
			return nil, fmt.Errorf("%w: missing key length", ErrInvalidIndex)
		}
		valueLen, ok := reader.u32()
		if !ok {
			return nil, fmt.Errorf("%w: missing value length", ErrInvalidIndex)
		}
		key, ok := reader.bytes(int(keyLen))
		if !ok {
			return nil, fmt.Errorf("%w: truncated key", ErrInvalidIndex)
		}
		value, ok := reader.bytes(int(valueLen))
		if !ok {
			return nil, fmt.Errorf("%w: truncated value", ErrInvalidIndex)
		}
		entries = append(entries, record.Entry{Kind: record.Kind(kind), Seq: seq, Key: key, Value: value})
	}
	if reader.remaining() != 0 {
		return nil, fmt.Errorf("%w: trailing block bytes", ErrInvalidIndex)
	}
	return entries, nil
}

// encodedEntryLen 返回一条记录编码后占用的字节数
func encodedEntryLen(entry record.Entry) int {
	return 1 + 8 + 4 + 4 + len(entry.Key) + len(entry.Value)
}

// blockReader 是数据块二进制解码的辅助结构
type blockReader struct {
	data []byte
	off  int
}

func (r *blockReader) remaining() int {
	return len(r.data) - r.off
}

func (r *blockReader) u8() (byte, bool) {
	if r.remaining() < 1 {
		return 0, false
	}
	value := r.data[r.off]
	r.off++
	return value, true
}

func (r *blockReader) u32() (uint32, bool) {
	if r.remaining() < 4 {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(r.data[r.off:])
	r.off += 4
	return value, true
}

func (r *blockReader) u64() (uint64, bool) {
	if r.remaining() < 8 {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(r.data[r.off:])
	r.off += 8
	return value, true
}

func (r *blockReader) bytes(n int) ([]byte, bool) {
	if n < 0 || r.remaining() < n {
		return nil, false
	}
	value := record.CloneBytes(r.data[r.off : r.off+n])
	r.off += n
	return value, true
}