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

	"godis/internal/resp"
)

type AOFLog struct {
	mu     sync.Mutex
	file   *os.File
	lastDB int
	hasDB  bool
}

func OpenAOF(path string) (*AOFLog, error) {
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

	return &AOFLog{file: file}, nil
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

	_, err := l.file.Write(encodeCommand(tokens))
	return err
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

	err := l.file.Close()
	l.file = nil
	return err
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
