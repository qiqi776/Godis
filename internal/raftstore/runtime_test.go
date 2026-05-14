package raftstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"mini-kv/internal/kv"
	"mini-kv/internal/kv/mem"
	"mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
)

func TestCommandCodecRoundTrip(t *testing.T) {
	t.Parallel()

	command := kv.Command{
		Type:      kv.CommandPut,
		Key:       "key",
		Value:     []byte("value"),
		ClientID:  "client",
		RequestID: 7,
	}
	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("decode command: %v", err)
	}
	if decoded.Type != command.Type || decoded.Key != command.Key || decoded.ClientID != command.ClientID || decoded.RequestID != command.RequestID || !bytes.Equal(decoded.Value, command.Value) {
		t.Fatalf("decoded command = %+v, want %+v", decoded, command)
	}
}

func TestDecodeCommandLegacyJSON(t *testing.T) {
	t.Parallel()

	command := kv.Command{
		Type:      kv.CommandDelete,
		Key:       "legacy-key",
		ClientID:  "legacy-client",
		RequestID: 11,
	}
	data, err := json.Marshal(commandEnvelope{
		Version: commandVersion,
		Command: command,
	})
	if err != nil {
		t.Fatalf("marshal legacy command: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("decode legacy command: %v", err)
	}
	if decoded.Type != command.Type || decoded.Key != command.Key || decoded.ClientID != command.ClientID || decoded.RequestID != command.RequestID || !bytes.Equal(decoded.Value, command.Value) {
		t.Fatalf("decoded legacy command = %+v, want %+v", decoded, command)
	}
}

type testNode struct {
	id      string
	store   *mem.MemoryStore
	raft    raft.Node
	kv      *Runtime
	storage *logstore.FileStorage
}

type stubNode struct {
	leader   bool
	leaderID string
	propose  func(context.Context, []byte) (uint64, error)
}

func (n *stubNode) Start() error { return nil }

func (n *stubNode) Stop() error { return nil }

func (n *stubNode) Propose(ctx context.Context, data []byte) (uint64, error) {
	if n.propose == nil {
		return 0, raft.ErrNotLeader
	}
	return n.propose(ctx, data)
}

func (n *stubNode) ReadIndex(context.Context) (uint64, error) { return 0, nil }

func (n *stubNode) Snapshot(uint64, []byte) error { return nil }

func (n *stubNode) IsLeader() bool { return n.leader }

func (n *stubNode) LeaderID() string { return n.leaderID }

func (n *stubNode) ApplyCh() <-chan raft.ApplyMsg { return nil }

func TestSingleNode(t *testing.T) {
	nodes := newCluster(t, []string{"node1"})
	node := waitLead(t, nodes, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := node.kv.Set(ctx, "key", []byte("value")); err != nil {
		t.Fatalf("set error: %v", err)
	}

	value, ok, err := node.kv.Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("get = %q, %v; want value, true", value, ok)
	}

	deleted, err := node.kv.Delete(ctx, "key")
	if err != nil {
		t.Fatalf("delete error: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("delete = %d, want 1", deleted)
	}

	_, ok, err = node.kv.Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("get after delete error: %v", err)
	}
	if ok {
		t.Fatalf("key should be deleted")
	}
}

func TestReplicate(t *testing.T) {
	nodes := newCluster(t, []string{"node1", "node2", "node3"})
	leader := waitLead(t, nodes, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := leader.kv.Set(ctx, "shared", []byte("value")); err != nil {
		t.Fatalf("set error: %v", err)
	}

	waitStores(t, nodes, "shared", []byte("value"), time.Second)
}

func TestRejectCmd(t *testing.T) {
	nodes := newCluster(t, []string{"node1", "node2", "node3"})
	leader := waitLead(t, nodes, time.Second)

	var follower *testNode
	for _, node := range nodes {
		if node.id != leader.id {
			follower = node
			break
		}
	}
	if follower == nil {
		t.Fatalf("expected follower")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := follower.kv.Set(ctx, "key", []byte("value"))
	assertLeaderErr(t, err)

	_, _, err = follower.kv.Get(context.Background(), "key")
	assertLeaderErr(t, err)
}

func TestReplayKV(t *testing.T) {
	cluster := newPersistCluster(t, []string{"node1"})
	node := waitLead(t, cluster.nodes, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := node.kv.Set(ctx, "stable", []byte("value")); err != nil {
		t.Fatalf("set stable: %v", err)
	}
	if err := node.kv.Set(ctx, "deleted", []byte("value")); err != nil {
		t.Fatalf("set deleted: %v", err)
	}
	deleted, err := node.kv.Delete(ctx, "deleted")
	if err != nil {
		t.Fatalf("delete deleted: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("delete = %d, want 1", deleted)
	}
	if err := node.kv.Set(ctx, "tail", []byte("done")); err != nil {
		t.Fatalf("set tail: %v", err)
	}

	restarted := cluster.restart(t, 0)
	_ = waitLead(t, cluster.nodes, time.Second)
	waitValue(t, restarted, "tail", []byte("done"), time.Second)

	value, ok, err := restarted.kv.Get(context.Background(), "stable")
	if err != nil {
		t.Fatalf("get stable after restart: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("stable = %q, %v; want value, true", value, ok)
	}

	value, ok, err = restarted.kv.Get(context.Background(), "deleted")
	if err != nil {
		t.Fatalf("get deleted after restart: %v", err)
	}
	if ok {
		t.Fatalf("deleted key should stay deleted after restart, got %q", value)
	}
}

func TestCatchup(t *testing.T) {
	cluster := newPersistCluster(t, []string{"node1", "node2", "node3"})
	leader := waitLead(t, cluster.nodes, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := leader.kv.Set(ctx, "before", []byte("restart")); err != nil {
		t.Fatalf("set before: %v", err)
	}
	waitStores(t, cluster.nodes, "before", []byte("restart"), time.Second)

	followerIndex := -1
	for i, node := range cluster.nodes {
		if node.id != leader.id {
			followerIndex = i
			break
		}
	}
	if followerIndex < 0 {
		t.Fatalf("expected follower")
	}

	stoppedID := cluster.nodes[followerIndex].id
	cluster.stop(t, followerIndex)

	if err := leader.kv.Set(ctx, "while-down", []byte("committed")); err != nil {
		t.Fatalf("set while-down: %v", err)
	}

	restarted := cluster.restart(t, followerIndex)
	if restarted.id != stoppedID {
		t.Fatalf("restarted id = %s, want %s", restarted.id, stoppedID)
	}

	waitValue(t, restarted, "before", []byte("restart"), time.Second)
	waitValue(t, restarted, "while-down", []byte("committed"), time.Second)

	currentLeader := waitLead(t, cluster.nodes, time.Second)
	if err := currentLeader.kv.Set(ctx, "after", []byte("restart")); err != nil {
		t.Fatalf("set after restart: %v", err)
	}
	waitStores(t, cluster.nodes, "after", []byte("restart"), time.Second)
}

func TestLaggingSnap(t *testing.T) {
	cluster := newPersistClusterWithSnap(t, []string{"node1", "node2", "node3"}, 2)
	leader := waitLead(t, cluster.nodes, time.Second)

	followerIndex := -1
	for i, node := range cluster.nodes {
		if node.id != leader.id {
			followerIndex = i
			break
		}
	}
	if followerIndex < 0 {
		t.Fatalf("expected follower")
	}

	stoppedID := cluster.nodes[followerIndex].id
	cluster.stop(t, followerIndex)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("snap:%d", i)
		value := []byte(fmt.Sprintf("value:%d", i))
		if err := leader.kv.Set(ctx, key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	waitSnap(t, leader.storage, time.Second)

	restarted := cluster.restart(t, followerIndex)
	if restarted.id != stoppedID {
		t.Fatalf("restarted id = %s, want %s", restarted.id, stoppedID)
	}

	waitValue(t, restarted, "snap:5", []byte("value:5"), time.Second)
	waitSnap(t, restarted.storage, time.Second)
}

func TestApplySnap(t *testing.T) {
	source := mem.NewMemoryStore()
	if result := source.Apply(kv.Command{Type: kv.CommandPut, Key: "snap", Value: []byte("value")}); result.Error != "" {
		t.Fatalf("apply source: %s", result.Error)
	}
	data, err := source.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	target := mem.NewMemoryStore()
	server := New(target, nil)
	server.applyMessage(raft.ApplyMsg{
		Index:        7,
		Term:         2,
		Snapshot:     true,
		SnapshotData: data,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.waitApplied(ctx, 7); err != nil {
		t.Fatalf("wait applied snapshot: %v", err)
	}

	value, ok, err := target.Get("snap")
	if err != nil || !ok || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("snapshot value = %q, ok=%v err=%v; want value, true, nil", value, ok, err)
	}
}

func TestWaiterReturnsCompletedResult(t *testing.T) {
	w := newWaiter()
	w.notify(applyResult{Index: 7, Data: []byte("late")})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	result, err := w.wait(ctx, 7)
	if err != nil {
		t.Fatalf("wait completed result: %v", err)
	}
	if !bytes.Equal(result.Data, []byte("late")) {
		t.Fatalf("completed data = %q, want late", result.Data)
	}
}

func TestProposeDoesNotHoldWaiterLock(t *testing.T) {
	store := mem.NewMemoryStore()
	node := &stubNode{leader: true, leaderID: "node1"}
	rt := New(store, node)

	node.propose = func(ctx context.Context, data []byte) (uint64, error) {
		if !rt.waiter.mu.TryLock() {
			t.Fatal("waiter mutex is held during node propose")
		}
		rt.waiter.mu.Unlock()
		go rt.applyMessage(raft.ApplyMsg{
			Index: 1,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  data,
		})
		return 1, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := rt.Propose(ctx, kv.Command{
		Type:  kv.CommandPut,
		Key:   "lock",
		Value: []byte("value"),
	})
	if err != nil {
		t.Fatalf("propose error: %v", err)
	}
}

func TestFastPropose(t *testing.T) {
	store := mem.NewMemoryStore()
	node := &stubNode{leader: true, leaderID: "node1"}
	rt := New(store, node)

	node.propose = func(ctx context.Context, data []byte) (uint64, error) {
		go rt.applyMessage(raft.ApplyMsg{
			Index: 1,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  append([]byte(nil), data...),
		})
		return 1, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := rt.Propose(ctx, kv.Command{
		Type:  kv.CommandPut,
		Key:   "fast",
		Value: []byte("value"),
	})
	if err != nil {
		t.Fatalf("propose error: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("apply result error: %s", result.Error)
	}

	value, ok, err := store.Get("fast")
	if err != nil || !ok || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("store value = %q, ok=%v err=%v; want value, true, nil", value, ok, err)
	}
}

func newCluster(t *testing.T, ids []string) []*testNode {
	t.Helper()

	transport := raft.NewFakeTransport()
	ctx, cancel := context.WithCancel(context.Background())

	nodes := make([]*testNode, 0, len(ids))
	for _, id := range ids {
		store := mem.NewMemoryStore()
		node, err := raft.NewNode(raft.Config{
			ID:               id,
			Peers:            ids,
			Storage:          logstore.NewMemoryStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new raft node %s error: %v", id, err)
		}

		handler, ok := node.(raft.RPCHandler)
		if !ok {
			t.Fatalf("raft node %s should implement RPCHandler", id)
		}
		transport.Register(id, handler)

		kvServer := New(store, node)
		nodes = append(nodes, &testNode{
			id:    id,
			store: store,
			raft:  node,
			kv:    kvServer,
		})
	}

	for _, node := range nodes {
		if err := node.raft.Start(); err != nil {
			t.Fatalf("start raft node %s error: %v", node.id, err)
		}
		node.kv.Start(ctx)
	}

	t.Cleanup(func() {
		cancel()
		for _, node := range nodes {
			_ = node.raft.Stop()
			node.store.Close()
		}
	})

	return nodes
}

type persistentTestCluster struct {
	ctx               context.Context
	cancel            context.CancelFunc
	ids               []string
	paths             map[string]string
	transport         *raft.FakeTransport
	nodes             []*testNode
	snapshotThreshold uint64
}

func newPersistCluster(t *testing.T, ids []string) *persistentTestCluster {
	t.Helper()

	return newPersistClusterWithSnap(t, ids, 0)
}

func newPersistClusterWithSnap(t *testing.T, ids []string, snapshotThreshold uint64) *persistentTestCluster {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cluster := &persistentTestCluster{
		ctx:               ctx,
		cancel:            cancel,
		ids:               append([]string(nil), ids...),
		paths:             make(map[string]string, len(ids)),
		transport:         raft.NewFakeTransport(),
		nodes:             make([]*testNode, 0, len(ids)),
		snapshotThreshold: snapshotThreshold,
	}

	dir := t.TempDir()
	for _, id := range ids {
		cluster.paths[id] = filepath.Join(dir, id+".wal")
		cluster.nodes = append(cluster.nodes, cluster.start(t, id))
	}

	t.Cleanup(func() {
		cluster.cancel()
		for i := range cluster.nodes {
			cluster.stop(t, i)
		}
	})

	return cluster
}

func (c *persistentTestCluster) start(t *testing.T, id string) *testNode {
	t.Helper()

	storage, err := logstore.OpenFileStorage(c.paths[id])
	if err != nil {
		t.Fatalf("open file storage for %s: %v", id, err)
	}

	store := mem.NewMemoryStore()
	node, err := raft.NewNode(raft.Config{
		ID:               id,
		Peers:            c.ids,
		Storage:          storage,
		Transport:        c.transport,
		ElectionTimeout:  80 * time.Millisecond,
		HeartbeatTimeout: 20 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		_ = storage.Close()
		t.Fatalf("new raft node %s error: %v", id, err)
	}

	handler, ok := node.(raft.RPCHandler)
	if !ok {
		_ = storage.Close()
		t.Fatalf("raft node %s should implement RPCHandler", id)
	}
	c.transport.Register(id, handler)

	kvServer := NewWithOptions(store, node, Options{
		SnapshotThreshold: c.snapshotThreshold,
	})
	if err := node.Start(); err != nil {
		_ = storage.Close()
		t.Fatalf("start raft node %s error: %v", id, err)
	}
	kvServer.Start(c.ctx)

	return &testNode{
		id:      id,
		store:   store,
		raft:    node,
		kv:      kvServer,
		storage: storage,
	}
}

func (c *persistentTestCluster) stop(t *testing.T, index int) {
	t.Helper()

	if index < 0 || index >= len(c.nodes) {
		t.Fatalf("node index %d out of range", index)
	}
	node := c.nodes[index]
	if node == nil {
		return
	}

	c.transport.Unregister(node.id)
	_ = node.raft.Stop()
	if node.storage != nil {
		if err := node.storage.Close(); err != nil {
			t.Fatalf("close storage for %s: %v", node.id, err)
		}
		node.storage = nil
	}
	node.store.Close()
}

func (c *persistentTestCluster) restart(t *testing.T, index int) *testNode {
	t.Helper()

	if index < 0 || index >= len(c.nodes) {
		t.Fatalf("node index %d out of range", index)
	}
	id := c.nodes[index].id
	c.stop(t, index)

	node := c.start(t, id)
	c.nodes[index] = node
	return node
}

func waitLead(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var stableLeader *testNode
	var stableSince time.Time
	for time.Now().Before(deadline) {
		var leader *testNode
		leaderCount := 0
		for _, node := range nodes {
			if node.raft.IsLeader() {
				leader = node
				leaderCount++
			}
		}
		if leaderCount == 1 {
			if stableLeader != nil && stableLeader.id == leader.id {
				if time.Since(stableSince) >= 50*time.Millisecond {
					return leader
				}
			} else {
				stableLeader = leader
				stableSince = time.Now()
			}
		} else {
			stableLeader = nil
			stableSince = time.Time{}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader")
	return nil
}

func waitStores(t *testing.T, nodes []*testNode, key string, want []byte, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, node := range nodes {
			value, ok, err := node.store.Get(key)
			if err != nil || !ok || !bytes.Equal(value, want) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, node := range nodes {
		value, ok, err := node.store.Get(key)
		t.Logf("node=%s ok=%v value=%q err=%v", node.id, ok, value, err)
	}
	t.Fatalf("timed out waiting for key %q to replicate", key)
}

func waitValue(t *testing.T, node *testNode, key string, want []byte, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value, ok, err := node.store.Get(key)
		if err == nil && ok && bytes.Equal(value, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	value, ok, err := node.store.Get(key)
	t.Fatalf("timed out waiting for node=%s key=%q want=%q; ok=%v value=%q err=%v", node.id, key, want, ok, value, err)
}

func waitSnap(t *testing.T, storage *logstore.FileStorage, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, err := storage.LoadSnapshot()
		if err == nil && snapshot.Index > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	snapshot, err := storage.LoadSnapshot()
	t.Fatalf("timed out waiting for snapshot; snapshot=%+v err=%v", snapshot, err)
}

func assertLeaderErr(t *testing.T, err error) {
	t.Helper()

	var notLeader NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("error = %v, want NotLeaderError", err)
	}
}
