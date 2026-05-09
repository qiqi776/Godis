package raft

import (
	"errors"
	"testing"
	"time"
)

func TestNoSelf(t *testing.T) {
	_, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node2", "node3"},
		Storage:          &failingHardStateStorage{},
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("new node error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestDupPeer(t *testing.T) {
	_, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2", "node2"},
		Storage:          &failingHardStateStorage{},
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("new node error = %v, want %v", err, ErrInvalidConfig)
	}
}
