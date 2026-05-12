package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"

	"mini-kv/internal/raft"
)

const serviceName = "Raft"

var ErrPeerNotFound = errors.New("raftnet: peer address not found")

type Transport struct {
	id         string
	listenAddr string
	mu         sync.RWMutex
	peerAddrs  map[string]string
	handler    raft.RPCHandler
	listener   net.Listener
	server     *rpc.Server
	conns      map[net.Conn]struct{}
	wg         sync.WaitGroup
	closeOnce  sync.Once
	closed     chan struct{}
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

	server := rpc.NewServer()
	if err := server.RegisterName(serviceName, &rpcService{handler: handler}); err != nil {
		_ = listener.Close()
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
	t.server = server
	t.listenAddr = listener.Addr().String()
	t.peerAddrs[t.id] = t.listenAddr
	t.mu.Unlock()
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.acceptLoop(listener, server)
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
	t.mu.Lock()
	t.peerAddrs[id] = addr
	t.mu.Unlock()
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
		for conn := range t.conns {
			_ = conn.Close()
			delete(t.conns, conn)
		}
		t.mu.Unlock()

		t.wg.Wait()
	})
	return err
}

func (t *Transport) RequestVote(ctx context.Context, target string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	err := t.call(ctx, target, "RequestVote", req, &resp)
	return resp, err
}

func (t *Transport) AppendEntries(ctx context.Context, target string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	err := t.call(ctx, target, "AppendEntries", req, &resp)
	return resp, err
}

func (t *Transport) InstallSnapshot(ctx context.Context, target string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	var resp raft.InstallSnapshotResponse
	err := t.call(ctx, target, "InstallSnapshot", req, &resp)
	return resp, err
}

func (t *Transport) acceptLoop(listener net.Listener, server *rpc.Server) {
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

		t.registerConn(conn)
		t.wg.Add(1)
		go func(c net.Conn) {
			defer t.wg.Done()
			defer t.unregisterConn(c)
			defer c.Close()
			server.ServeConn(c)
		}(conn)
	}
}

func (t *Transport) call(ctx context.Context, target string, method string, req any, resp any) error {
	if ctx == nil {
		ctx = context.Background()
	}

	addr, err := t.peerAddr(target)
	if err != nil {
		return err
	}

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	client := rpc.NewClient(conn)
	done := make(chan error, 1)
	go func() {
		done <- client.Call(fmt.Sprintf("%s.%s", serviceName, method), req, resp)
	}()

	select {
	case err := <-done:
		_ = client.Close()
		return err
	case <-ctx.Done():
		_ = client.Close()
		<-done
		return ctx.Err()
	}
}

func (t *Transport) peerAddr(target string) (string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	addr := t.peerAddrs[target]
	if addr == "" {
		return "", fmt.Errorf("%w: %s", ErrPeerNotFound, target)
	}
	return addr, nil
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

type rpcService struct {
	handler raft.RPCHandler
}

func (s *rpcService) RequestVote(req raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	out, err := s.handler.HandleRequestVote(context.Background(), req)
	if err != nil {
		return err
	}
	*resp = out
	return nil
}

func (s *rpcService) AppendEntries(req raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	out, err := s.handler.HandleAppendEntries(context.Background(), req)
	if err != nil {
		return err
	}
	*resp = out
	return nil
}

func (s *rpcService) InstallSnapshot(req raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse) error {
	out, err := s.handler.HandleInstallSnapshot(context.Background(), req)
	if err != nil {
		return err
	}
	*resp = out
	return nil
}
