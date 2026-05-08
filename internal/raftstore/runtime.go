package raftstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"mini-kv/internal/kv"
	"mini-kv/internal/raft"
)

const commandVersion = 1

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
	return json.Marshal(commandEnvelope{
		Version: commandVersion,
		Command: command,
	})
}

func DecodeCommand(data []byte) (kv.Command, error) {
	var envelope commandEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return kv.Command{}, err
	}
	if envelope.Version != commandVersion {
		return kv.Command{}, fmt.Errorf("unsupported command version: %d", envelope.Version)
	}
	return envelope.Command, nil
}

type applyResult struct {
	Index  uint64
	Term   uint64
	Data   []byte
	Result kv.ApplyResult
	Err    error
}

type waiter struct {
	mu        sync.Mutex
	waiters   map[uint64][]chan applyResult
	completed map[uint64]applyResult
}

func newWaiter() *waiter {
	return &waiter{
		waiters:   make(map[uint64][]chan applyResult),
		completed: make(map[uint64]applyResult),
	}
}

func (w *waiter) wait(ctx context.Context, index uint64) (applyResult, error) {
	w.mu.Lock()
	if result, ok := w.completed[index]; ok {
		delete(w.completed, index)
		w.mu.Unlock()
		return result, nil
	}

	ch := make(chan applyResult, 1)
	w.waiters[index] = append(w.waiters[index], ch)
	w.mu.Unlock()

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
		w.completed[result.Index] = result
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

type Options struct {
	SnapshotThreshold uint64
}

type Runtime struct {
	store             kv.Store
	node              raft.Node
	waiter            *waiter
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
	if err := s.ensureLeader(); err != nil {
		return kv.ApplyResult{}, err
	}

	data, err := EncodeCommand(command)
	if err != nil {
		return kv.ApplyResult{}, err
	}

	index, err := s.node.Propose(ctx, data)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return kv.ApplyResult{}, NotLeaderError{LeaderID: s.node.LeaderID()}
		}
		return kv.ApplyResult{}, err
	}

	applied, err := s.waiter.wait(ctx, index)
	if err != nil {
		return kv.ApplyResult{}, err
	}
	if applied.Err != nil {
		return applied.Result, applied.Err
	}
	if !bytes.Equal(applied.Data, data) {
		return applied.Result, ErrProposalMismatch
	}
	if applied.Result.Error != "" {
		return applied.Result, errors.New(applied.Result.Error)
	}

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
		return
	}
	if msg.Type == raft.EntryNoop {
		s.setAppliedIndex(msg.Index)
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
		Data:   append([]byte(nil), msg.Data...),
		Result: result,
		Err:    err,
	})
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

	data, err := s.store.Snapshot()
	if err != nil {
		return
	}
	if err := s.node.Snapshot(index, data); err != nil {
		return
	}
	s.lastSnapshotIndex = index
}

func (s *Runtime) linearizableRead(ctx context.Context) error {
	if err := s.ensureLeader(); err != nil {
		return err
	}

	index, err := s.node.ReadIndex(ctx)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return NotLeaderError{LeaderID: s.node.LeaderID()}
		}
		return err
	}

	return s.waitApplied(ctx, index)
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
