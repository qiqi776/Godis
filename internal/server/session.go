package server

import (
	"net"
	"sort"
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
	channels   map[string]struct{}
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

func (s *Session) Subscribe(channels ...string) int {
	if s.channels == nil {
		s.channels = make(map[string]struct{})
	}
	added := 0
	for _, channel := range channels {
		if _, ok := s.channels[channel]; ok {
			continue
		}
		s.channels[channel] = struct{}{}
		added++
	}
	return added
}

func (s *Session) Unsubscribe(channels ...string) int {
	i := 0
	for _, channel := range channels {
		if _, ok := s.channels[channel]; !ok {
			continue
		}
		delete(s.channels, channel)
		i++
	}
	return i
}

func (s *Session) UnsubscribeAll() []string {
    out := make([]string, 0, len(s.channels))
    for channel := range s.channels {
        out = append(out, channel)
    }
	sort.Strings(out)
    s.channels = nil
    return out
}

func (s *Session) SubCount() int {
    return len(s.channels)
}