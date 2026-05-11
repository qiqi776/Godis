package faults

import (
	"context"

	"mini-kv/internal/raft"
)

type Transport struct {
	localID    string
	transport  raft.Transport
	controller *Controller
}

func (t *Transport) RequestVote(ctx context.Context, target string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if err := t.controller.beforeSend(ctx, t.localID, target); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return t.transport.RequestVote(ctx, target, req)
}

func (t *Transport) AppendEntries(ctx context.Context, target string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if err := t.controller.beforeSend(ctx, t.localID, target); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return t.transport.AppendEntries(ctx, target, req)
}

func (t *Transport) InstallSnapshot(ctx context.Context, target string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	if err := t.controller.beforeSend(ctx, t.localID, target); err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return t.transport.InstallSnapshot(ctx, target, req)
}
