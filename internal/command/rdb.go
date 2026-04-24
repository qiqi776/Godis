package command

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"godis/internal/resp"
)

const rdbMagic = "GODISRDB1\n"

type RDBFile struct {
	path string
}

func NewRDBFile(path string) (*RDBFile, error) {
	if path == "" {
		return nil, fmt.Errorf("rdb path is empty")
	}
	return &RDBFile{path: path}, nil
}

func (r *RDBFile) Dump(snapshot func() [][][]byte) error {
	commands := snapshot()
	return WriteRDB(r.path, commands)
}

func (r *RDBFile) Load(exec *Executor) error {
	return LoadRDB(r.path, exec)
}

func WriteRDB(path string, commands [][][]byte) error {
	if path == "" {
		return fmt.Errorf("rdb path is empty")
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	tmpName := path + ".tmp"
	tmp, err := os.OpenFile(tmpName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	if err := writeRDBCommands(tmp, commands); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

func LoadRDB(path string, exec *Executor) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	session := &replaySession{}
	return replayRDBPreamble(reader, exec, session)
}

func writeRDBCommands(writer io.Writer, commands [][][]byte) error {
	if _, err := writer.Write([]byte(rdbMagic)); err != nil {
		return err
	}
	return gob.NewEncoder(writer).Encode(commands)
}

func replayRDBPreamble(reader *bufio.Reader, exec *Executor, session *replaySession) error {
	prefix, err := reader.Peek(len(rdbMagic))
	if err != nil {
		if err == io.EOF || err == bufio.ErrBufferFull {
			return nil
		}
		return err
	}
	if string(prefix) != rdbMagic {
		return nil
	}
	if _, err := reader.Discard(len(rdbMagic)); err != nil {
		return err
	}

	var commands [][][]byte
	if err := gob.NewDecoder(reader).Decode(&commands); err != nil {
		return err
	}
	return replayCommands(exec, session, commands)
}

func replayCommands(exec *Executor, session *replaySession, commands [][][]byte) error {
	for _, tokens := range commands {
		reply := exec.Execute(session, tokens)
		if len(reply) > 0 && reply[0] == '-' {
			return fmt.Errorf("replay command failed: %q -> %s", tokens, bytes.TrimSpace(reply))
		}
	}
	return nil
}

func (e *Executor) execSave(_ Session, _ [][]byte) []byte {
	if e.dumper == nil {
		return resp.Error("ERR RDB is not enabled")
	}
	if err := e.dumper.Dump(e.engine.AOFRewriteCommands); err != nil {
		return resp.Error("ERR RDB dump failed")
	}
	return resp.SimpleString("OK")
}
