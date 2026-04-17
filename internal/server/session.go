package server

import (
	"net"
	"time"
)

type Session struct {
	Conn 	   net.Conn
	RemoteAddr string
	CreatedAt  time.Time
	index    int
}

func NewSession(conn net.Conn) *Session {
	return &Session{
		Conn: conn,
		RemoteAddr: conn.RemoteAddr().String(),
		CreatedAt: time.Now(),
	}
}

func (s *Session) GetIndex() int {
	return s.index
}

func (s *Session) SetIndex(index int) {
	s.index = index
}