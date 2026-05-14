package raftstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"mini-kv/internal/kv"
	"mini-kv/internal/observability"
	"mini-kv/internal/raft"
)

const (
	commandVersion       = 1
	commandBinaryVersion = 1
)

var commandBinaryMagic = [...]byte{'M', 'K', 'V', 'C'}

var ErrProposalMismatch = errors.New("raftkv: proposed log entry was overwritten")

type NotLeaderError struct {
	LeaderID string
}

func (e NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "not leader"
	}
	return fmt.Sprintf("not leader; leader=%s", e.LeaderID)
}

type commandEnvelope struct {
	Version int        `json:"version"`
	Command kv.Command `json:"command"`
}

func EncodeCommand(command kv.Command) ([]byte, error) {
	out := make([]byte, 0, encodedCommandSize(command))
	out = append(out, commandBinaryMagic[:]...)
	out = append(out, commandBinaryVersion, byte(command.Type))
	out = appendString(out, command.Key)
	out = appendBytes(out, command.Value)
	out = appendString(out, command.ClientID)
	out = binary.AppendUvarint(out, command.RequestID)
	return out, nil
}

func DecodeCommand(data []byte) (kv.Command, error) {
	if isBinaryCommand(data) {
		return decodeBinaryCommand(data)
	}

	var envelope commandEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return kv.Command{}, err
	}
	if envelope.Version != commandVersion {
		return kv.Command{}, fmt.Errorf("unsupported command version: %d", envelope.Version)
	}
	return envelope.Command, nil
}

func encodedCommandSize(command kv.Command) int {
	return len(commandBinaryMagic) +
		2 +
		uvarintSize(uint64(len(command.Key))) + len(command.Key) +
		uvarintSize(uint64(len(command.Value))) + len(command.Value) +
		uvarintSize(uint64(len(command.ClientID))) + len(command.ClientID) +
		uvarintSize(command.RequestID)
}

func appendString(out []byte, value string) []byte {
	out = binary.AppendUvarint(out, uint64(len(value)))
	return append(out, value...)
}

func appendBytes(out []byte, value []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(value)))
	return append(out, value...)
}

func uvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func isBinaryCommand(data []byte) bool {
	return len(data) >= len(commandBinaryMagic) && bytes.Equal(data[:len(commandBinaryMagic)], commandBinaryMagic[:])
}

func decodeBinaryCommand(data []byte) (kv.Command, error) {
	if len(data) < len(commandBinaryMagic)+2 {
		return kv.Command{}, errors.New("command payload too short")
	}
	if data[len(commandBinaryMagic)] != commandBinaryVersion {
		return kv.Command{}, fmt.Errorf("unsupported command binary version: %d", data[len(commandBinaryMagic)])
	}

	rest := data[len(commandBinaryMagic)+2:]
	key, rest, err := readString(rest, "key")
	if err != nil {
		return kv.Command{}, err
	}
	value, rest, err := readBytes(rest, "value")
	if err != nil {
		return kv.Command{}, err
	}
	clientID, rest, err := readString(rest, "client id")
	if err != nil {
		return kv.Command{}, err
	}
	requestID, rest, err := readUvarint(rest, "request id")
	if err != nil {
		return kv.Command{}, err
	}
	if len(rest) != 0 {
		return kv.Command{}, errors.New("command payload has trailing data")
	}

	return kv.Command{
		Type:      kv.CommandType(data[len(commandBinaryMagic)+1]),
		Key:       key,
		Value:     value,
		ClientID:  clientID,
		RequestID: requestID,
	}, nil
}

func readString(data []byte, field string) (string, []byte, error) {
	value, rest, err := readBytes(data, field)
	if err != nil {
		return "", nil, err
	}
	return string(value), rest, nil
}

func readBytes(data []byte, field string) ([]byte, []byte, error) {
	size, rest, err := readUvarint(data, field+" length")
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(rest)) < size {
		return nil, nil, fmt.Errorf("command %s length exceeds payload", field)
	}
	return rest[:size], rest[size:], nil
}

func readUvarint(data []byte, field string) (uint64, []byte, error) {
	value, n := binary.Uvarint(data)
	if n == 0 {
		return 0, nil, fmt.Errorf("command %s is missing", field)
	}
	if n < 0 {
		return 0, nil, fmt.Errorf("command %s overflows uint64", field)
	}
	return value, data[n:], nil
}

type applyResult struct {
	Index  uint64
	Term   uint64
	Data   []byte
	Result kv.ApplyResult
	Err    error
}

type waiter struct {
	mu             sync.Mutex
	waiters        map[uint64][]chan applyResult
	completed      map[uint64]applyResult
	completedOrder []uint64
}

const completedApplyResultsLimit = 1024

func newWaiter() *waiter {
	return &waiter{
		waiters:   make(map[uint64][]chan applyResult),
		completed: make(map[uint64]applyResult),
	}
}

func (w *waiter) wait(ctx context.Context, index uint64) (applyResult, error) {
	ch := make(chan applyResult, 1)
	w.mu.Lock()
	if result, ok := w.completed[index]; ok {
		delete(w.completed, index)
		w.mu.Unlock()
		return result, nil
	}
	w.waiters[index] = append(w.waiters[index], ch)
	w.mu.Unlock()

	return w.waitRegistered(ctx, index, ch)
}

func (w *waiter) waitRegistered(ctx context.Context, index uint64, ch chan applyResult) (applyResult, error) {
	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		w.unregister(index, ch)
		return applyResult{}, ctx.Err()
	}
}

func (w *waiter) notify(result applyResult) {
	w.mu.Lock()
	waiters := w.waiters[result.Index]
	if len(waiters) == 0 {
		w.rememberLocked(result)
		w.mu.Unlock()
		return
	}

	delete(w.waiters, result.Index)
	w.mu.Unlock()
	for _, ch := range waiters {
		ch <- result
	}
}

func (w *waiter) unregister(index uint64, target chan applyResult) {
	w.mu.Lock()
	defer w.mu.Unlock()

	waiters := w.waiters[index]
	for i, ch := range waiters {
		if ch == target {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(w.waiters, index)
		return
	}
	w.waiters[index] = waiters
}

func (w *waiter) rememberLocked(result applyResult) {
	if _, ok := w.completed[result.Index]; !ok {
		w.completedOrder = append(w.completedOrder, result.Index)
	}
	w.completed[result.Index] = result

	for len(w.completedOrder) > completedApplyResultsLimit {
		index := w.completedOrder[0]
		w.completedOrder = w.completedOrder[1:]
		delete(w.completed, index)
	}
}

type Options struct {
	SnapshotThreshold uint64
	NodeID            string
	Registry          *observability.Registry
}

type Runtime struct {
	store             kv.Store
	node              raft.Node
	waiter            *waiter
	nodeID            string
	registry          *observability.Registry
	snapshotThreshold uint64
	snapshotMu        sync.Mutex
	lastSnapshotIndex uint64
	applyMu           sync.Mutex
	appliedIndex      uint64
	appliedWaiters    map[uint64][]chan struct{}
}

func New(store kv.Store, node raft.Node) *Runtime {
	return NewWithOptions(store, node, Options{})
}

func NewWithOptions(store kv.Store, node raft.Node, options Options) *Runtime {
	return &Runtime{
		store:             store,
		node:              node,
		waiter:            newWaiter(),
		nodeID:            options.NodeID,
		registry:          options.Registry,
		snapshotThreshold: options.SnapshotThreshold,
		appliedWaiters:    make(map[uint64][]chan struct{}),
	}
}

func (s *Runtime) Start(ctx context.Context) {
	go s.applyLoop(ctx)
}

func (s *Runtime) IsLeader() bool {
	return s.node.IsLeader()
}

func (s *Runtime) LeaderID() string {
	return s.node.LeaderID()
}

func (s *Runtime) Propose(ctx context.Context, command kv.Command) (kv.ApplyResult, error) {
	startedAt := time.Now()
	if err := s.ensureLeader(); err != nil {
		s.observe("propose", startedAt, err)
		return kv.ApplyResult{}, err
	}

	data, err := EncodeCommand(command)
	if err != nil {
		s.observe("propose", startedAt, err)
		return kv.ApplyResult{}, err
	}

	index, err := s.node.Propose(ctx, data)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			err = NotLeaderError{LeaderID: s.node.LeaderID()}
		}
		s.observe("propose", startedAt, err)
		return kv.ApplyResult{}, err
	}

	applied, err := s.waiter.wait(ctx, index)
	if err != nil {
		s.observe("propose", startedAt, err)
		return kv.ApplyResult{}, err
	}
	if applied.Err != nil {
		s.observe("propose", startedAt, applied.Err)
		return applied.Result, applied.Err
	}
	if !bytes.Equal(applied.Data, data) {
		s.observe("propose", startedAt, ErrProposalMismatch)
		return applied.Result, ErrProposalMismatch
	}
	if applied.Result.Error != "" {
		err = errors.New(applied.Result.Error)
		s.observe("propose", startedAt, err)
		return applied.Result, err
	}

	s.observe("propose", startedAt, nil)
	return applied.Result, nil
}

func (s *Runtime) Set(ctx context.Context, key string, value []byte) error {
	_, err := s.Propose(ctx, kv.Command{
		Type:  kv.CommandPut,
		Key:   key,
		Value: append([]byte(nil), value...),
	})
	return err
}

func (s *Runtime) Delete(ctx context.Context, keys ...string) (int64, error) {
	var deleted int64
	for _, key := range keys {
		result, err := s.Propose(ctx, kv.Command{
			Type: kv.CommandDelete,
			Key:  key,
		})
		if err != nil {
			return deleted, err
		}
		if result.Found {
			deleted++
		}
	}
	return deleted, nil
}

func (s *Runtime) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := s.linearizableRead(ctx); err != nil {
		return nil, false, err
	}

	db := s.store.Reader()
	return db.Get(key)
}

func (s *Runtime) applyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.node.ApplyCh():
			if !ok {
				return
			}
			s.applyMessage(msg)
		}
	}
}

func (s *Runtime) applyMessage(msg raft.ApplyMsg) {
	startedAt := time.Now()
	if msg.Snapshot {
		err := s.store.Restore(msg.SnapshotData)
		if err == nil {
			s.setAppliedIndex(msg.Index)
		}
		s.waiter.notify(applyResult{
			Index: msg.Index,
			Term:  msg.Term,
			Err:   err,
		})
		s.observe("apply_snapshot", startedAt, err)
		return
	}
	if msg.Type == raft.EntryNoop {
		s.setAppliedIndex(msg.Index)
		s.observe("apply_noop", startedAt, nil)
		return
	}
	command, err := DecodeCommand(msg.Data)

	var result kv.ApplyResult
	if err == nil {
		result = s.store.Apply(command)
		if result.Error != "" {
			err = errors.New(result.Error)
		}
		s.setAppliedIndex(msg.Index)
	}

	s.waiter.notify(applyResult{
		Index:  msg.Index,
		Term:   msg.Term,
		Data:   msg.Data,
		Result: result,
		Err:    err,
	})
	s.observe("apply_entry", startedAt, err)
	if err == nil {
		s.maybeSnapshot(msg.Index)
	}
}

func (s *Runtime) ensureLeader() error {
	if s.node.IsLeader() {
		return nil
	}
	return NotLeaderError{LeaderID: s.node.LeaderID()}
}

func (s *Runtime) maybeSnapshot(index uint64) {
	if s.snapshotThreshold == 0 {
		return
	}

	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	if index <= s.lastSnapshotIndex || index-s.lastSnapshotIndex < s.snapshotThreshold {
		return
	}

	startedAt := time.Now()
	data, err := s.store.Snapshot()
	if err != nil {
		s.observe("snapshot_create", startedAt, err)
		return
	}
	if err := s.node.Snapshot(index, data); err != nil {
		s.observe("snapshot_create", startedAt, err)
		return
	}
	s.lastSnapshotIndex = index
	s.observe("snapshot_create", startedAt, nil)
}

func (s *Runtime) linearizableRead(ctx context.Context) error {
	startedAt := time.Now()
	if err := s.ensureLeader(); err != nil {
		s.observe("read_index", startedAt, err)
		return err
	}

	index, err := s.node.ReadIndex(ctx)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			err = NotLeaderError{LeaderID: s.node.LeaderID()}
			s.observe("read_index", startedAt, err)
			return err
		}
		s.observe("read_index", startedAt, err)
		return err
	}

	err = s.waitApplied(ctx, index)
	s.observe("read_index", startedAt, err)
	return err
}

func (s *Runtime) waitApplied(ctx context.Context, index uint64) error {
	s.applyMu.Lock()
	if s.appliedIndex >= index {
		s.applyMu.Unlock()
		return nil
	}

	ch := make(chan struct{})
	s.appliedWaiters[index] = append(s.appliedWaiters[index], ch)
	s.applyMu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		s.unregisterAppliedWaiter(index, ch)
		return ctx.Err()
	}
}

func (s *Runtime) setAppliedIndex(index uint64) {
	s.applyMu.Lock()
	if index <= s.appliedIndex {
		s.applyMu.Unlock()
		return
	}

	s.appliedIndex = index
	var ready []chan struct{}
	for waitIndex, waiters := range s.appliedWaiters {
		if waitIndex <= index {
			ready = append(ready, waiters...)
			delete(s.appliedWaiters, waitIndex)
		}
	}
	s.applyMu.Unlock()

	for _, ch := range ready {
		close(ch)
	}
	if s.registry != nil && s.nodeID != "" {
		s.registry.SetAppliedIndex(s.nodeID, index)
	}
}

func (s *Runtime) unregisterAppliedWaiter(index uint64, target chan struct{}) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	waiters := s.appliedWaiters[index]
	for i, ch := range waiters {
		if ch == target {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(s.appliedWaiters, index)
		return
	}
	s.appliedWaiters[index] = waiters
}

func (s *Runtime) observe(operation string, startedAt time.Time, err error) {
	if s.registry == nil || s.nodeID == "" {
		return
	}
	s.registry.ObserveRaftOperation(s.nodeID, operation, time.Since(startedAt), err)
}
