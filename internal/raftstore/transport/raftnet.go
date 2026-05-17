package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"mini-kv/internal/raft"
)

const peerConnPoolSize = 4

var (
	ErrPeerNotFound    = errors.New("raftnet: peer address not found")
	ErrTransportClosed = errors.New("raftnet: transport closed")
)

type Transport struct {
	id         string
	listenAddr string
	mu         sync.RWMutex
	peerAddrs  map[string]string
	peers      map[string]*peerClient
	handler    raft.RPCHandler
	listener   net.Listener
	conns      map[net.Conn]struct{}
	wg         sync.WaitGroup
	closeOnce  sync.Once
	closed     chan struct{}
	accepted   atomic.Uint64
	requestSeq atomic.Uint64
}

func New(id string, listenAddr string, peerAddrs map[string]string) (*Transport, error) {
	if id == "" {
		return nil, errors.New("raftnet: node id is empty")
	}
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	copied := make(map[string]string, len(peerAddrs)+1)
	for peer, addr := range peerAddrs {
		if peer != "" && addr != "" {
			copied[peer] = addr
		}
	}
	return &Transport{
		id:         id,
		listenAddr: listenAddr,
		peerAddrs:  copied,
		peers:      make(map[string]*peerClient),
		conns:      make(map[net.Conn]struct{}),
		closed:     make(chan struct{}),
	}, nil
}

func (t *Transport) Start(handler raft.RPCHandler) error {
	if handler == nil {
		return errors.New("raftnet: rpc handler is nil")
	}
	listener, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return err
	}

	t.mu.Lock()
	if t.listener != nil {
		t.mu.Unlock()
		_ = listener.Close()
		return nil
	}
	t.handler = handler
	t.listener = listener
	t.listenAddr = listener.Addr().String()
	t.peerAddrs[t.id] = t.listenAddr
	t.mu.Unlock()

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.acceptLoop(listener)
	}()

	return nil
}

func (t *Transport) Addr() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.listenAddr
}

func (t *Transport) SetPeer(id string, addr string) {
	if id == "" || addr == "" {
		return
	}

	var old *peerClient
	t.mu.Lock()
	if t.peerAddrs[id] != addr {
		old = t.peers[id]
		delete(t.peers, id)
	}
	t.peerAddrs[id] = addr
	t.mu.Unlock()

	if old != nil {
		old.close()
	}
}

func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.closed)

		t.mu.Lock()
		if t.listener != nil {
			err = t.listener.Close()
			t.listener = nil
		}
		peers := make([]*peerClient, 0, len(t.peers))
		for _, peer := range t.peers {
			peers = append(peers, peer)
		}
		t.peers = make(map[string]*peerClient)
		conns := make([]net.Conn, 0, len(t.conns))
		for conn := range t.conns {
			conns = append(conns, conn)
			delete(t.conns, conn)
		}
		t.mu.Unlock()

		for _, peer := range peers {
			peer.close()
		}
		for _, conn := range conns {
			_ = conn.Close()
		}
		t.wg.Wait()
	})
	return err
}

func (t *Transport) RequestVote(ctx context.Context, target string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	payload, err := encodeRequestVoteRequest(req)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	respPayload, err := t.call(ctx, target, messageRequestVote, messageRequestVoteResponse, payload)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return decodeRequestVoteResponse(respPayload)
}

func (t *Transport) AppendEntries(ctx context.Context, target string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	payload, err := encodeAppendEntriesRequest(req)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	respPayload, err := t.call(ctx, target, messageAppendEntries, messageAppendEntriesResponse, payload)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return decodeAppendEntriesResponse(respPayload)
}

func (t *Transport) InstallSnapshot(ctx context.Context, target string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	payload, err := encodeInstallSnapshotRequest(req)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	respPayload, err := t.call(ctx, target, messageInstallSnapshot, messageInstallSnapshotResponse, payload)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return decodeInstallSnapshotResponse(respPayload)
}

func (t *Transport) acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-t.closed:
				return
			default:
				continue
			}
		}

		t.accepted.Add(1)
		t.registerConn(conn)
		t.wg.Add(1)
		go func(c net.Conn) {
			defer t.wg.Done()
			defer t.unregisterConn(c)
			defer c.Close()
			t.serveConn(c)
		}(conn)
	}
}

func (t *Transport) serveConn(conn net.Conn) {
	for {
		frame, err := readFrame(conn)
		if err != nil {
			return
		}

		responseType, payload, err := t.handleFrame(frame.typ, frame.payload)
		if err != nil {
			responseType = messageErrorResponse
			payload, _ = encodeErrorResponse(err)
		}
		if err := writeFrame(conn, responseType, frame.id, payload); err != nil {
			return
		}
	}
}

func (t *Transport) handleFrame(typ messageType, payload []byte) (messageType, []byte, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()
	if handler == nil {
		return 0, nil, ErrTransportClosed
	}

	switch typ {
	case messageRequestVote:
		req, err := decodeRequestVoteRequest(payload)
		if err != nil {
			return 0, nil, err
		}
		resp, err := handler.HandleRequestVote(context.Background(), req)
		if err != nil {
			return 0, nil, err
		}
		payload, err := encodeRequestVoteResponse(resp)
		return messageRequestVoteResponse, payload, err
	case messageAppendEntries:
		req, err := decodeAppendEntriesRequest(payload)
		if err != nil {
			return 0, nil, err
		}
		resp, err := handler.HandleAppendEntries(context.Background(), req)
		if err != nil {
			return 0, nil, err
		}
		payload, err := encodeAppendEntriesResponse(resp)
		return messageAppendEntriesResponse, payload, err
	case messageInstallSnapshot:
		req, err := decodeInstallSnapshotRequest(payload)
		if err != nil {
			return 0, nil, err
		}
		resp, err := handler.HandleInstallSnapshot(context.Background(), req)
		if err != nil {
			return 0, nil, err
		}
		payload, err := encodeInstallSnapshotResponse(resp)
		return messageInstallSnapshotResponse, payload, err
	default:
		return 0, nil, fmt.Errorf("raftnet: unknown message type %d", typ)
	}
}

func (t *Transport) call(ctx context.Context, target string, requestType messageType, responseType messageType, payload []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	peer, err := t.peerClient(target)
	if err != nil {
		return nil, err
	}
	id := t.requestSeq.Add(1)
	if id == 0 {
		id = t.requestSeq.Add(1)
	}
	return peer.call(ctx, requestType, responseType, id, payload)
}

func (t *Transport) peerClient(target string) (*peerClient, error) {
	var old *peerClient

	t.mu.Lock()
	select {
	case <-t.closed:
		t.mu.Unlock()
		return nil, ErrTransportClosed
	default:
	}

	addr := t.peerAddrs[target]
	if addr == "" {
		t.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrPeerNotFound, target)
	}

	peer := t.peers[target]
	if peer != nil && peer.addr == addr {
		t.mu.Unlock()
		return peer, nil
	}
	if peer != nil {
		old = peer
	}
	peer = newPeerClient(addr)
	t.peers[target] = peer
	t.mu.Unlock()

	if old != nil {
		old.close()
	}
	return peer, nil
}

func (t *Transport) registerConn(conn net.Conn) {
	t.mu.Lock()
	t.conns[conn] = struct{}{}
	t.mu.Unlock()
}

func (t *Transport) unregisterConn(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

func (t *Transport) acceptedConnCount() uint64 {
	return t.accepted.Load()
}

type peerClient struct {
	addr   string
	lanes  []peerConn
	next   atomic.Uint64
	mu     sync.RWMutex
	closed bool
}

type peerConn struct {
	owner  *peerClient
	mu     sync.Mutex
	connMu sync.Mutex
	conn   net.Conn
}

func newPeerClient(addr string) *peerClient {
	client := &peerClient{
		addr:  addr,
		lanes: make([]peerConn, peerConnPoolSize),
	}
	for i := range client.lanes {
		client.lanes[i].owner = client
	}
	return client
}

func (c *peerClient) call(ctx context.Context, requestType messageType, responseType messageType, id uint64, payload []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	lane := c.acquireLane()
	defer lane.mu.Unlock()

	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return lane.callLocked(ctx, requestType, responseType, id, payload)
}

func (c *peerClient) acquireLane() *peerConn {
	for i := range c.lanes {
		lane := &c.lanes[i]
		if lane.mu.TryLock() {
			return lane
		}
	}

	index := int(c.next.Add(1)-1) % len(c.lanes)
	lane := &c.lanes[index]
	lane.mu.Lock()
	return lane
}

func (c *peerClient) checkOpen() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrTransportClosed
	}
	return nil
}

func (c *peerClient) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	lanes := c.lanes
	c.mu.Unlock()

	for i := range lanes {
		lanes[i].close()
	}
}

func (c *peerClient) storeConn(lane *peerConn, conn net.Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = conn.Close()
		return ErrTransportClosed
	}

	lane.connMu.Lock()
	lane.conn = conn
	lane.connMu.Unlock()
	return nil
}

func (c *peerConn) callLocked(ctx context.Context, requestType messageType, responseType messageType, id uint64, payload []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	conn, err := c.ensureConn(ctx)
	if err != nil {
		return nil, err
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Time{})
	}

	if err := writeFrame(conn, requestType, id, payload); err != nil {
		c.failConn(conn)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}

	frame, err := readFrame(conn)
	if err != nil {
		c.failConn(conn)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	if frame.id != id {
		c.failConn(conn)
		return nil, errors.New("raftnet: response id mismatch")
	}
	if frame.typ == messageErrorResponse {
		message, err := decodeErrorResponse(frame.payload)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(message)
	}
	if frame.typ != responseType {
		return nil, fmt.Errorf("raftnet: response type %d does not match request type %d", frame.typ, responseType)
	}
	return frame.payload, nil
}

func (c *peerConn) ensureConn(ctx context.Context) (net.Conn, error) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn != nil {
		return conn, nil
	}

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", c.owner.addr)
	if err != nil {
		return nil, err
	}
	if err := c.owner.storeConn(c, conn); err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *peerConn) failConn(conn net.Conn) {
	c.connMu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.connMu.Unlock()
	_ = conn.Close()
}

func (c *peerConn) close() {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}
