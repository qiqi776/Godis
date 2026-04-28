package raft

import (
	"context"
	"sync"
)

type RPCHandler interface {
	HandleRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error)
	HandleAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

type FakeTransport struct {
	mu       sync.RWMutex
	handlers map[string]RPCHandler
}

func NewFakeTransport() *FakeTransport {
	return &FakeTransport{
		handlers: make(map[string]RPCHandler),
	}
}

func (t *FakeTransport) Register(id string, handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.handlers[id] = handler
}

func (t *FakeTransport) Unregister(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.handlers, id)
}

func (t *FakeTransport) RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error) {
	t.mu.RLock()
	handler := t.handlers[target]
	t.mu.RUnlock()

	if handler == nil {
		return RequestVoteResponse{}, ErrNodeStopped
	}
	return handler.HandleRequestVote(ctx, req)
}

func (t *FakeTransport) AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.RLock()
	handler := t.handlers[target]
	t.mu.RUnlock()

	if handler == nil {
		return AppendEntriesResponse{}, ErrNodeStopped
	}
	return handler.HandleAppendEntries(ctx, req)
}
