package transport

import (
	"context"
	"fmt"
	"testing"
	"time"

	"mini-kv/internal/raft"
)

var (
	benchmarkAppendEntriesResponse raft.AppendEntriesResponse
	benchmarkRequestVoteResponse   raft.RequestVoteResponse
	benchmarkInstallSnapResponse   raft.InstallSnapshotResponse
)

func BenchmarkTransportAppendEntries(b *testing.B) {
	client, cleanup := newBenchmarkTransportPair(b)
	defer cleanup()

	req := benchmarkAppendEntriesRequest()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		resp, err := client.AppendEntries(ctx, "node1", req)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkAppendEntriesResponse = resp
	}
}

func BenchmarkTransportAppendEntriesParallel(b *testing.B) {
	client, cleanup := newBenchmarkTransportPair(b)
	defer cleanup()

	req := benchmarkAppendEntriesRequest()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			resp, err := client.AppendEntries(ctx, "node1", req)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkAppendEntriesResponse = resp
		}
	})
}

func BenchmarkTransportRequestVote(b *testing.B) {
	client, cleanup := newBenchmarkTransportPair(b)
	defer cleanup()

	req := raft.RequestVoteRequest{
		Term:         7,
		CandidateID:  "node2",
		LastLogIndex: 1024,
		LastLogTerm:  6,
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		resp, err := client.RequestVote(ctx, "node1", req)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkRequestVoteResponse = resp
	}
}

func BenchmarkTransportInstallSnapshot(b *testing.B) {
	client, cleanup := newBenchmarkTransportPair(b)
	defer cleanup()

	req := raft.InstallSnapshotRequest{
		Term:              7,
		LeaderID:          "node1",
		LastIncludedIndex: 4096,
		LastIncludedTerm:  6,
		Data:              make([]byte, 64*1024),
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		resp, err := client.InstallSnapshot(ctx, "node1", req)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkInstallSnapResponse = resp
	}
}

func newBenchmarkTransportPair(b *testing.B) (*Transport, func()) {
	b.Helper()

	server, err := New("node1", "127.0.0.1:0", nil)
	if err != nil {
		b.Fatalf("new server transport: %v", err)
	}
	if err := server.Start(&stubHandler{}); err != nil {
		b.Fatalf("start server transport: %v", err)
	}

	client, err := New("node2", "127.0.0.1:0", map[string]string{
		"node1": server.Addr(),
	})
	if err != nil {
		_ = server.Close()
		b.Fatalf("new client transport: %v", err)
	}

	return client, func() {
		_ = client.Close()
		_ = server.Close()
	}
}

func benchmarkAppendEntriesRequest() raft.AppendEntriesRequest {
	entries := make([]raft.LogEntry, 8)
	for i := range entries {
		entries[i] = raft.LogEntry{
			Index: uint64(i + 1),
			Term:  7,
			Type:  raft.EntryNormal,
			Data:  []byte(fmt.Sprintf("bench-entry-%02d", i)),
		}
	}
	return raft.AppendEntriesRequest{
		Term:         7,
		LeaderID:     "node1",
		PrevLogIndex: 4096,
		PrevLogTerm:  6,
		Entries:      entries,
		LeaderCommit: 4104,
		ReadContext:  uint64(time.Now().UnixNano()),
	}
}
