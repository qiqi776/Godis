package command

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"godis/internal/resp"
)

type AOFLog struct {
	mu       sync.Mutex
	file     *os.File
	policy   FsyncPolicy
	syncFile func() error
	now      func() time.Time
	lastSync time.Time
	lastDB   int
	hasDB    bool
}

type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

func ParseFsyncPolicy(value string) (FsyncPolicy, error) {
	if value == "" {
		return FsyncEverySec, nil
	}

	switch FsyncPolicy(strings.ToLower(value)) {
	case FsyncAlways:
		return FsyncAlways, nil
	case FsyncEverySec:
		return FsyncEverySec, nil
	case FsyncNo:
		return FsyncNo, nil
	default:
		return "", fmt.Errorf("invalid aof fsync policy: %s", value)
	}
}

func OpenAOF(path string, policies ...FsyncPolicy) (*AOFLog, error) {
	if path == "" {
		return nil, fmt.Errorf("aof path is empty")
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	policy := FsyncEverySec
	if len(policies) > 0 {
		policy = policies[0]
	}

	return &AOFLog{
		file:     file,
		policy:   policy,
		syncFile: file.Sync,
		now:      time.Now,
	}, nil
}

func (l *AOFLog) Append(dbIndex int, tokens [][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.hasDB || dbIndex != l.lastDB {
		if _, err := l.file.Write(encodeCommand([][]byte{
			[]byte("SELECT"),
			[]byte(strconv.Itoa(dbIndex)),
		})); err != nil {
			return err
		}
		l.lastDB = dbIndex
		l.hasDB = true
	}

	if _, err := l.file.Write(encodeCommand(tokens)); err != nil {
		return err
	}

	return l.fsyncLocked()
}

func (l *AOFLog) Rewrite(snapshot func() [][][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return fmt.Errorf("aof is closed")
	}

	commands := snapshot()
	name := l.file.Name()
	tmpName := name + ".tmp"

	tmp, err := os.OpenFile(tmpName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	for _, tokens := range commands {
		if _, err := tmp.Write(encodeCommand(tokens)); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return err
		}
	}

	if l.policy != FsyncNo {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return err
		}
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	if err := l.file.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	if err := os.Rename(tmpName, name); err != nil {
		l.file = nil
		_ = os.Remove(tmpName)
		return err
	}

	file, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		l.file = nil
		return err
	}

	l.file = file
	l.syncFile = file.Sync
	l.hasDB = false
	l.lastDB = 0
	l.lastSync = time.Time{}
	return nil
}

func (l *AOFLog) Replay(exec *Executor) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(l.file)
	session := &replaySession{}

	for {
		tokens, err := resp.ReadCommand(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		reply := exec.Execute(session, tokens)
		if len(reply) > 0 && reply[0] == '-' {
			return fmt.Errorf("replay command failed: %q -> %s", tokens, bytes.TrimSpace(reply))
		}
	}

	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	l.hasDB = false
	l.lastDB = 0
	return nil
}

func (l *AOFLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}

	if l.policy != FsyncNo {
		if err := l.syncLocked(); err != nil {
			return err
		}
	}

	err := l.file.Close()
	l.file = nil
	return err
}

func (l *AOFLog) fsyncLocked() error {
	switch l.policy {
	case FsyncAlways:
		return l.syncLocked()
	case FsyncEverySec:
		now := l.now()
		if l.lastSync.IsZero() || now.Sub(l.lastSync) >= time.Second {
			if err := l.syncLocked(); err != nil {
				return err
			}
			l.lastSync = now
		}
		return nil
	case FsyncNo:
		return nil
	default:
		return fmt.Errorf("invalid aof fsync policy: %s", l.policy)
	}
}

func (l *AOFLog) syncLocked() error {
	if l.syncFile == nil {
		return nil
	}
	return l.syncFile()
}

func encodeCommand(tokens [][]byte) []byte {
	var builder strings.Builder
	builder.WriteString("*")
	builder.WriteString(strconv.Itoa(len(tokens)))
	builder.WriteString("\r\n")

	for _, token := range tokens {
		builder.WriteString("$")
		builder.WriteString(strconv.Itoa(len(token)))
		builder.WriteString("\r\n")
		builder.Write(token)
		builder.WriteString("\r\n")
	}

	return []byte(builder.String())
}

type replaySession struct {
	dbIndex int
}

func (s *replaySession) GetDBIndex() int {
	return s.dbIndex
}

func (s *replaySession) SetDBIndex(index int) {
	s.dbIndex = index
}

func (s *replaySession) InMulti() bool {
	return false
}

func (s *replaySession) StartMulti() bool {
	return false
}

func (s *replaySession) Queue(_ [][]byte) {}

func (s *replaySession) Queued() [][][]byte {
	return nil
}

func (s *replaySession) ClearMulti() {}

func (s *replaySession) Watch(_ int, _ string, _ uint64) {}

func (s *replaySession) Watched() map[int]map[string]uint64 {
	return nil
}

func (s *replaySession) ClearWatch() {}
