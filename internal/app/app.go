package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mini-kv/internal/config"
	"mini-kv/internal/kv/mem"
	"mini-kv/internal/logger"
	"mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
	"mini-kv/internal/raftstore"
	raftstoretransport "mini-kv/internal/raftstore/transport"
	grpcserver "mini-kv/internal/server/grpcserver"
	"mini-kv/internal/service/minikv"
)

type App struct {
	Config        config.Config
	Logger        *logger.Logger
	KVStore       *mem.MemoryStore
	KVService     minikv.Service
	Server        *grpcserver.Server
	RaftNode      raft.Node
	RaftStorage   *logstore.FileStorage
	RaftRuntime   *raftstore.Runtime
	RaftTransport *raftstoretransport.Transport
}

func Start(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}

	l := logger.New(cfg.LogLevel)
	if err := validate(cfg.Raft); err != nil {
		return nil, err
	}

	engine := mem.NewMemoryStore()

	raftTransport, err := raftstoretransport.New(cfg.Raft.ID, cfg.Raft.ListenAddr, cfg.Raft.PeerAddrs)
	if err != nil {
		return nil, err
	}

	raftStorage, err := logstore.OpenFileStorage(cfg.Raft.WALPath)
	if err != nil {
		_ = raftTransport.Close()
		return nil, err
	}

	raftNode, err := raft.NewNode(raft.Config{
		ID:               cfg.Raft.ID,
		Peers:            cfg.Raft.Peers,
		Storage:          raftStorage,
		Transport:        raftTransport,
		ElectionTimeout:  time.Duration(cfg.Raft.ElectionTimeoutMS) * time.Millisecond,
		HeartbeatTimeout: time.Duration(cfg.Raft.HeartbeatTimeoutMS) * time.Millisecond,
		ApplyBufferSize:  cfg.Raft.ApplyBufferSize,
	})
	if err != nil {
		_ = raftTransport.Close()
		_ = raftStorage.Close()
		return nil, err
	}

	handler, ok := raftNode.(raft.RPCHandler)
	if !ok {
		_ = raftTransport.Close()
		_ = raftStorage.Close()
		return nil, errors.New("raft node does not implement RPCHandler")
	}
	if err := raftTransport.Start(handler); err != nil {
		_ = raftTransport.Close()
		_ = raftStorage.Close()
		return nil, err
	}

	if err := raftNode.Start(); err != nil {
		_ = raftTransport.Close()
		_ = raftStorage.Close()
		return nil, err
	}

	runtime := raftstore.NewWithOptions(engine, raftNode, raftstore.Options{
		SnapshotThreshold: cfg.Raft.SnapshotThreshold,
	})
	service := minikv.NewRaft(runtime)
	srv := grpcserver.New(cfg, l, service)

	return &App{
		Config:        cfg,
		Logger:        l,
		KVStore:       engine,
		KVService:     service,
		Server:        srv,
		RaftNode:      raftNode,
		RaftStorage:   raftStorage,
		RaftRuntime:   runtime,
		RaftTransport: raftTransport,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	if a.RaftRuntime != nil {
		a.RaftRuntime.Start(ctx)
	}
	defer func() {
		if a.RaftNode != nil {
			_ = a.RaftNode.Stop()
		}
		if a.RaftTransport != nil {
			_ = a.RaftTransport.Close()
		}
		if a.RaftStorage != nil {
			_ = a.RaftStorage.Close()
		}
		if a.KVStore != nil {
			a.KVStore.Close()
		}
	}()
	return a.Server.Run(ctx)
}

func validate(cfg config.RaftConfig) error {
	for _, peer := range cfg.Peers {
		if cfg.PeerAddrs[peer] == "" {
			return fmt.Errorf("raft peer %q has no address", peer)
		}
	}
	return nil
}
