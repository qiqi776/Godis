package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"mini-kv/internal/raft"
)

func TestProtocolRoundTrip(t *testing.T) {
	appendReq := raft.AppendEntriesRequest{
		Term:         9,
		LeaderID:     "node1",
		PrevLogIndex: 7,
		PrevLogTerm:  8,
		Entries: []raft.LogEntry{
			{Index: 8, Term: 9, Type: raft.EntryNormal, Data: []byte("one")},
			{Index: 9, Term: 9, Type: raft.EntryNoop},
		},
		LeaderCommit: 9,
		ReadContext:  77,
	}
	appendPayload, err := encodeAppendEntriesRequest(appendReq)
	if err != nil {
		t.Fatalf("encode append request: %v", err)
	}
	gotAppendReq, err := decodeAppendEntriesRequest(appendPayload)
	if err != nil {
		t.Fatalf("decode append request: %v", err)
	}
	if gotAppendReq.Term != appendReq.Term || gotAppendReq.LeaderID != appendReq.LeaderID || gotAppendReq.ReadContext != appendReq.ReadContext || len(gotAppendReq.Entries) != len(appendReq.Entries) {
		t.Fatalf("append request round trip = %+v, want %+v", gotAppendReq, appendReq)
	}
	if !bytes.Equal(gotAppendReq.Entries[0].Data, appendReq.Entries[0].Data) {
		t.Fatalf("append entry data = %q, want %q", gotAppendReq.Entries[0].Data, appendReq.Entries[0].Data)
	}

	snapshotReq := raft.InstallSnapshotRequest{
		Term:              11,
		LeaderID:          "node1",
		LastIncludedIndex: 100,
		LastIncludedTerm:  10,
		Data:              bytes.Repeat([]byte("x"), 4096),
	}
	snapshotPayload, err := encodeInstallSnapshotRequest(snapshotReq)
	if err != nil {
		t.Fatalf("encode snapshot request: %v", err)
	}
	gotSnapshotReq, err := decodeInstallSnapshotRequest(snapshotPayload)
	if err != nil {
		t.Fatalf("decode snapshot request: %v", err)
	}
	if gotSnapshotReq.Term != snapshotReq.Term || gotSnapshotReq.LastIncludedIndex != snapshotReq.LastIncludedIndex || !bytes.Equal(gotSnapshotReq.Data, snapshotReq.Data) {
		t.Fatalf("snapshot request round trip = %+v, want %+v", gotSnapshotReq, snapshotReq)
	}
}

func TestProtocolRejectsBadFrame(t *testing.T) {
	var header [frameHeaderLen]byte
	binary.LittleEndian.PutUint32(header[0:4], 0xdeadbeef)
	header[4] = protocolVersion

	if _, err := readFrame(bytes.NewReader(header[:])); err == nil {
		t.Fatal("expected bad magic error")
	}

	payload, err := encodeAppendEntriesRequest(raft.AppendEntriesRequest{
		Term:     1,
		LeaderID: "node1",
		Entries:  []raft.LogEntry{{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("x")}},
	})
	if err != nil {
		t.Fatalf("encode append request: %v", err)
	}
	if _, err := decodeAppendEntriesRequest(payload[:len(payload)-1]); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated append error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}
