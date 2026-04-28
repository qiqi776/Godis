package raft

import "time"

type Config struct {
	ID               string
	Peers            []string
	Storage          Storage
	Transport        Transport
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	ApplyBufferSize  int
}

func (c Config) validate() error {
	if c.ID == "" {
		return ErrInvalidConfig
	}
	if c.Storage == nil {
		return ErrInvalidConfig
	}
	if c.Transport == nil {
		return ErrInvalidConfig
	}
	if len(c.Peers) == 0 {
		return ErrInvalidConfig
	}
	if c.ElectionTimeout <= 0 {
		return ErrInvalidConfig
	}
	if c.HeartbeatTimeout <= 0 {
		return ErrInvalidConfig
	}
	if c.ApplyBufferSize <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

func (c Config) quorum() int {
	return len(c.Peers)/2 + 1
}
