package minikv

import (
	"context"

	"mini-kv/internal/raftstore"
)

// Service 是所有客户端协议层可见的最小 KV 接口
// 它隐藏底层是单机引擎还是 Raft 集群
type Service interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// RaftService 基于 raftstore.Runtime 实现 Service
type RaftService struct {
	runtime *raftstore.Runtime
}

// 编译期检查是否实现了接口
var _ Service = (*RaftService)(nil)

func NewRaft(runtime *raftstore.Runtime) *RaftService {
	return &RaftService{runtime: runtime}
}

func (s *RaftService) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return s.runtime.Get(ctx, key)
}

func (s *RaftService) Set(ctx context.Context, key string, value []byte) error {
	return s.runtime.Set(ctx, key, value)
}

func (s *RaftService) Delete(ctx context.Context, key string) error {
	_, err := s.runtime.Delete(ctx, key)
	return err
}
