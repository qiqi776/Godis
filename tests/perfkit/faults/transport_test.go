package faults

import (
	"context"
	"errors"
	"testing"
	"time"

	"mini-kv/internal/raft"
)

func TestTransportInjectsDelay(t *testing.T) {
	controller := NewController(1)
	controller.Delay("node1", "node2", 20*time.Millisecond)
	transport := controller.Wrap("node1", stubTransport{})

	startedAt := time.Now()
	_, err := transport.AppendEntries(context.Background(), "node2", raft.AppendEntriesRequest{})
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < 20*time.Millisecond {
		t.Fatalf("elapsed = %s, want at least injected delay", elapsed)
	}
}

func TestTransportBlocksPartitionUntilContextDone(t *testing.T) {
	controller := NewController(1)
	controller.Block("node1", "node2")
	transport := controller.Wrap("node1", stubTransport{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := transport.AppendEntries(ctx, "node2", raft.AppendEntriesRequest{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
}

func TestTransportDropsMessages(t *testing.T) {
	controller := NewController(1)
	controller.DropRate("node1", "node2", 1)
	transport := controller.Wrap("node1", stubTransport{})

	_, err := transport.AppendEntries(context.Background(), "node2", raft.AppendEntriesRequest{})
	if !errors.Is(err, ErrDropped) {
		t.Fatalf("error = %v, want dropped", err)
	}
}

type stubTransport struct{}

func (stubTransport) RequestVote(context.Context, string, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{VoteGranted: true}, nil
}

func (stubTransport) AppendEntries(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{Success: true}, nil
}

func (stubTransport) InstallSnapshot(context.Context, string, raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	return raft.InstallSnapshotResponse{}, nil
}
