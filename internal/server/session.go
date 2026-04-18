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
