package server

import (
	"net"
	"time"
)

type Session struct {
	Conn       net.Conn
	RemoteAddr string
	CreatedAt  time.Time
	dbIndex    int
	inMulti    bool
	queued     [][][]byte
	watched    map[int]map[string]uint64
}

func NewSession(conn net.Conn) *Session {
	return &Session{
		Conn:       conn,
		RemoteAddr: conn.RemoteAddr().String(),
		CreatedAt:  time.Now(),
	}
}

func (s *Session) GetDBIndex() int {
	return s.dbIndex
}

func (s *Session) SetDBIndex(index int) {
	s.dbIndex = index
}

func (s *Session) InMulti() bool {
	return s.inMulti
}

func (s *Session) StartMulti() bool {
	if s.inMulti {
		return false
	}
	s.inMulti = true
	s.queued = nil
	return true
}

func (s *Session) Queue(tokens [][]byte) {
	s.queued = append(s.queued, cloneTokens(tokens))
}

func (s *Session) Queued() [][][]byte {
	return s.queued
}

func (s *Session) ClearMulti() {
	s.inMulti = false
	s.queued = nil
}

func (s *Session) Watch(dbIndex int, key string, rev uint64) {
    if s.watched == nil {
        s.watched = make(map[int]map[string]uint64)
    }
    if s.watched[dbIndex] == nil {
        s.watched[dbIndex] = make(map[string]uint64)
    }
    s.watched[dbIndex][key] = rev
}

func (s *Session) Watched() map[int]map[string]uint64 {
    return s.watched
}

func (s *Session) ClearWatch() {
    s.watched = nil
}

func cloneTokens(tokens [][]byte) [][]byte {
	out := make([][]byte, 0, len(tokens))
	for _, token := range tokens {
		out = append(out, append([]byte(nil), token...))
	}
	return out
}
