package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"

	"mini-kv/internal/raft"
)

const (
	protocolMagic       uint32 = 0x52564b4d // MKVR, little-endian.
	protocolVersion     byte   = 1
	frameHeaderLen             = 18
	maxFramePayloadSize        = 256 << 20
)

type messageType byte

const (
	messageRequestVote messageType = iota + 1
	messageRequestVoteResponse
	messageAppendEntries
	messageAppendEntriesResponse
	messageInstallSnapshot
	messageInstallSnapshotResponse
	messageErrorResponse
)

type protocolFrame struct {
	typ     messageType
	id      uint64
	payload []byte
}

func writeFrame(conn net.Conn, typ messageType, id uint64, payload []byte) error {
	if len(payload) > math.MaxUint32 {
		return errors.New("raftnet: frame payload too large")
	}

	header := make([]byte, frameHeaderLen)
	binary.LittleEndian.PutUint32(header[0:4], protocolMagic)
	header[4] = protocolVersion
	header[5] = byte(typ)
	binary.LittleEndian.PutUint64(header[6:14], id)
	binary.LittleEndian.PutUint32(header[14:18], uint32(len(payload)))

	if err := writeFull(conn, header); err != nil {
		return err
	}
	return writeFull(conn, payload)
}

func readFrame(reader io.Reader) (protocolFrame, error) {
	var header [frameHeaderLen]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return protocolFrame{}, err
	}
	if binary.LittleEndian.Uint32(header[0:4]) != protocolMagic {
		return protocolFrame{}, errors.New("raftnet: invalid frame magic")
	}
	if header[4] != protocolVersion {
		return protocolFrame{}, fmt.Errorf("raftnet: unsupported protocol version %d", header[4])
	}

	payloadLen := binary.LittleEndian.Uint32(header[14:18])
	if payloadLen > maxFramePayloadSize {
		return protocolFrame{}, errors.New("raftnet: frame payload exceeds limit")
	}
	payload := make([]byte, int(payloadLen))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return protocolFrame{}, err
	}
	return protocolFrame{
		typ:     messageType(header[5]),
		id:      binary.LittleEndian.Uint64(header[6:14]),
		payload: payload,
	}, nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

type frameEncoder struct {
	buf []byte
	err error
}

func newFrameEncoder(size int) frameEncoder {
	return frameEncoder{buf: make([]byte, 0, size)}
}

func (e *frameEncoder) u64(value uint64) {
	e.buf = binary.LittleEndian.AppendUint64(e.buf, value)
}

func (e *frameEncoder) u32(value uint32) {
	e.buf = binary.LittleEndian.AppendUint32(e.buf, value)
}

func (e *frameEncoder) u8(value byte) {
	e.buf = append(e.buf, value)
}

func (e *frameEncoder) bool(value bool) {
	if value {
		e.u8(1)
		return
	}
	e.u8(0)
}

func (e *frameEncoder) string(value string) {
	e.bytes([]byte(value))
}

func (e *frameEncoder) bytes(value []byte) {
	if len(value) > math.MaxUint32 {
		e.err = errors.New("raftnet: byte field too large")
		return
	}
	e.u32(uint32(len(value)))
	e.buf = append(e.buf, value...)
}

type frameDecoder struct {
	data []byte
	err  error
}

func newFrameDecoder(data []byte) frameDecoder {
	return frameDecoder{data: data}
}

func (d *frameDecoder) u64() uint64 {
	if d.err != nil {
		return 0
	}
	if len(d.data) < 8 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	value := binary.LittleEndian.Uint64(d.data[:8])
	d.data = d.data[8:]
	return value
}

func (d *frameDecoder) u32() uint32 {
	if d.err != nil {
		return 0
	}
	if len(d.data) < 4 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	value := binary.LittleEndian.Uint32(d.data[:4])
	d.data = d.data[4:]
	return value
}

func (d *frameDecoder) u8() byte {
	if d.err != nil {
		return 0
	}
	if len(d.data) < 1 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	value := d.data[0]
	d.data = d.data[1:]
	return value
}

func (d *frameDecoder) bool() bool {
	return d.u8() != 0
}

func (d *frameDecoder) string() string {
	return string(d.bytes())
}

func (d *frameDecoder) bytes() []byte {
	size := d.u32()
	if d.err != nil {
		return nil
	}
	if uint32(len(d.data)) < size {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	value := d.data[:size]
	d.data = d.data[size:]
	return value
}

func (d *frameDecoder) done() error {
	if d.err != nil {
		return d.err
	}
	if len(d.data) != 0 {
		return errors.New("raftnet: trailing payload bytes")
	}
	return nil
}

func encodeRequestVoteRequest(req raft.RequestVoteRequest) ([]byte, error) {
	enc := newFrameEncoder(32 + len(req.CandidateID))
	enc.u64(req.Term)
	enc.string(req.CandidateID)
	enc.u64(req.LastLogIndex)
	enc.u64(req.LastLogTerm)
	return enc.buf, enc.err
}

func decodeRequestVoteRequest(payload []byte) (raft.RequestVoteRequest, error) {
	dec := newFrameDecoder(payload)
	req := raft.RequestVoteRequest{
		Term:         dec.u64(),
		CandidateID:  dec.string(),
		LastLogIndex: dec.u64(),
		LastLogTerm:  dec.u64(),
	}
	return req, dec.done()
}

func encodeRequestVoteResponse(resp raft.RequestVoteResponse) ([]byte, error) {
	enc := newFrameEncoder(9)
	enc.u64(resp.Term)
	enc.bool(resp.VoteGranted)
	return enc.buf, enc.err
}

func decodeRequestVoteResponse(payload []byte) (raft.RequestVoteResponse, error) {
	dec := newFrameDecoder(payload)
	resp := raft.RequestVoteResponse{
		Term:        dec.u64(),
		VoteGranted: dec.bool(),
	}
	return resp, dec.done()
}

func encodeAppendEntriesRequest(req raft.AppendEntriesRequest) ([]byte, error) {
	enc := newFrameEncoder(appendEntriesRequestSize(req))
	enc.u64(req.Term)
	enc.string(req.LeaderID)
	enc.u64(req.PrevLogIndex)
	enc.u64(req.PrevLogTerm)
	enc.u64(req.LeaderCommit)
	enc.u64(req.ReadContext)
	enc.u32(uint32(len(req.Entries)))
	for _, entry := range req.Entries {
		enc.u64(entry.Index)
		enc.u64(entry.Term)
		enc.u8(byte(entry.Type))
		enc.bytes(entry.Data)
	}
	return enc.buf, enc.err
}

func decodeAppendEntriesRequest(payload []byte) (raft.AppendEntriesRequest, error) {
	dec := newFrameDecoder(payload)
	req := raft.AppendEntriesRequest{
		Term:         dec.u64(),
		LeaderID:     dec.string(),
		PrevLogIndex: dec.u64(),
		PrevLogTerm:  dec.u64(),
		LeaderCommit: dec.u64(),
		ReadContext:  dec.u64(),
	}
	count := dec.u32()
	if count > maxFramePayloadSize {
		return raft.AppendEntriesRequest{}, errors.New("raftnet: too many append entries")
	}
	req.Entries = make([]raft.LogEntry, 0, int(count))
	for i := uint32(0); i < count; i++ {
		entry := raft.LogEntry{
			Index: dec.u64(),
			Term:  dec.u64(),
			Type:  raft.EntryType(dec.u8()),
			Data:  cloneBytes(dec.bytes()),
		}
		req.Entries = append(req.Entries, entry)
	}
	return req, dec.done()
}

func encodeAppendEntriesResponse(resp raft.AppendEntriesResponse) ([]byte, error) {
	enc := newFrameEncoder(33)
	enc.u64(resp.Term)
	enc.bool(resp.Success)
	enc.u64(resp.ReadContext)
	enc.u64(resp.ConflictIndex)
	enc.u64(resp.ConflictTerm)
	return enc.buf, enc.err
}

func decodeAppendEntriesResponse(payload []byte) (raft.AppendEntriesResponse, error) {
	dec := newFrameDecoder(payload)
	resp := raft.AppendEntriesResponse{
		Term:          dec.u64(),
		Success:       dec.bool(),
		ReadContext:   dec.u64(),
		ConflictIndex: dec.u64(),
		ConflictTerm:  dec.u64(),
	}
	return resp, dec.done()
}

func encodeInstallSnapshotRequest(req raft.InstallSnapshotRequest) ([]byte, error) {
	enc := newFrameEncoder(32 + len(req.LeaderID) + len(req.Data))
	enc.u64(req.Term)
	enc.string(req.LeaderID)
	enc.u64(req.LastIncludedIndex)
	enc.u64(req.LastIncludedTerm)
	enc.bytes(req.Data)
	return enc.buf, enc.err
}

func decodeInstallSnapshotRequest(payload []byte) (raft.InstallSnapshotRequest, error) {
	dec := newFrameDecoder(payload)
	req := raft.InstallSnapshotRequest{
		Term:              dec.u64(),
		LeaderID:          dec.string(),
		LastIncludedIndex: dec.u64(),
		LastIncludedTerm:  dec.u64(),
		Data:              cloneBytes(dec.bytes()),
	}
	return req, dec.done()
}

func encodeInstallSnapshotResponse(resp raft.InstallSnapshotResponse) ([]byte, error) {
	enc := newFrameEncoder(8)
	enc.u64(resp.Term)
	return enc.buf, enc.err
}

func decodeInstallSnapshotResponse(payload []byte) (raft.InstallSnapshotResponse, error) {
	dec := newFrameDecoder(payload)
	resp := raft.InstallSnapshotResponse{Term: dec.u64()}
	return resp, dec.done()
}

func encodeErrorResponse(err error) ([]byte, error) {
	enc := newFrameEncoder(32)
	enc.string(err.Error())
	return enc.buf, enc.err
}

func decodeErrorResponse(payload []byte) (string, error) {
	dec := newFrameDecoder(payload)
	message := dec.string()
	return message, dec.done()
}

func appendEntriesRequestSize(req raft.AppendEntriesRequest) int {
	size := 8 + 4 + len(req.LeaderID) + 8 + 8 + 8 + 8 + 4
	for _, entry := range req.Entries {
		size += 8 + 8 + 1 + 4 + len(entry.Data)
	}
	return size
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
