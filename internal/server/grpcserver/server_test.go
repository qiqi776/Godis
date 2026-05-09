package grpcserver

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	minikvv1 "mini-kv/api/minikv/v1"
	"mini-kv/internal/service/minikv"
)

const bufSize = 1024 * 1024

type fakeService struct {
	values map[string][]byte
}

var _ minikv.Service = (*fakeService)(nil)

func newSvc() *fakeService {
	return &fakeService{
		values: make(map[string][]byte),
	}
}

func (s *fakeService) Get(_ context.Context, key string) ([]byte, bool, error) {
	value, ok := s.values[key]
	return append([]byte(nil), value...), ok, nil
}

func (s *fakeService) Set(_ context.Context, key string, value []byte) error {
	s.values[key] = append([]byte(nil), value...)
	return nil
}

func (s *fakeService) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}

func TestGRPC(t *testing.T) {
	t.Parallel()

	service := newSvc()
	client, cleanup := newClient(t, service)
	defer cleanup()

	ctx := context.Background()

	if _, err := client.Set(ctx, &minikvv1.SetRequest{Key: "a", Value: []byte("1")}); err != nil {
		t.Fatalf("set error: %v", err)
	}

	getResp, err := client.Get(ctx, &minikvv1.GetRequest{Key: "a"})
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if !getResp.GetFound() || string(getResp.GetValue()) != "1" {
		t.Fatalf("get = found:%v value:%q, want true 1", getResp.GetFound(), getResp.GetValue())
	}

	if _, err := client.Delete(ctx, &minikvv1.DeleteRequest{Key: "a"}); err != nil {
		t.Fatalf("delete error: %v", err)
	}

	missingResp, err := client.Get(ctx, &minikvv1.GetRequest{Key: "a"})
	if err != nil {
		t.Fatalf("missing get error: %v", err)
	}
	if missingResp.GetFound() {
		t.Fatalf("missing get found = true, want false")
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()

	client, cleanup := newClient(t, errorService{})
	defer cleanup()

	_, err := client.Get(context.Background(), &minikvv1.GetRequest{Key: "a"})
	if err == nil {
		t.Fatal("expected grpc error")
	}
}

type errorService struct{}

var _ minikv.Service = errorService{}

func (errorService) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("boom")
}

func (errorService) Set(context.Context, string, []byte) error {
	return errors.New("boom")
}

func (errorService) Delete(context.Context, string) error {
	return errors.New("boom")
}

func newClient(t *testing.T, service minikv.Service) (minikvv1.KVClient, func()) {
	t.Helper()

	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	minikvv1.RegisterKVServer(server, newKVHandler(service))

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc bufconn: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	}

	return minikvv1.NewKVClient(conn), cleanup
}
