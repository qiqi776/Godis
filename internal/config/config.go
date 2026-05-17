package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Host     string        `yaml:"host"`
	Port     int           `yaml:"port"`
	LogLevel string        `yaml:"log_level"`
	Debug    DebugConfig   `yaml:"debug"`
	Storage  StorageConfig `yaml:"storage"`
	Raft     RaftConfig    `yaml:"raft"`
}

type StorageConfig struct {
	LSMPath string `yaml:"lsm_path"`
}

type DebugConfig struct {
	Enabled bool   `yaml:"enabled"`
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
}

type RaftConfig struct {
	ID                 string            `yaml:"id"`
	Peers              []string          `yaml:"peers"`
	ListenAddr         string            `yaml:"listen_addr"`
	PeerAddrs          map[string]string `yaml:"peer_addrs"`
	WALPath            string            `yaml:"wal_path"`
	ElectionTimeoutMS  int               `yaml:"election_timeout_ms"`
	HeartbeatTimeoutMS int               `yaml:"heartbeat_timeout_ms"`
	ApplyBufferSize    int               `yaml:"apply_buffer_size"`
	SnapshotThreshold  uint64            `yaml:"snapshot_threshold"`
}

func Default() Config {
	return Config{
		Host:     "127.0.0.1",
		Port:     6380,
		LogLevel: "debug",
		Debug: DebugConfig{
			Enabled: false,
			Host:    "127.0.0.1",
			Port:    6060,
		},
		Storage: StorageConfig{
			LSMPath: "data/lsm-node1",
		},
		Raft: RaftConfig{
			ID:                 "node1",
			Peers:              []string{"node1"},
			ListenAddr:         "127.0.0.1:16380",
			PeerAddrs:          map[string]string{"node1": "127.0.0.1:16380"},
			WALPath:            "data/raft-node1.wal",
			ElectionTimeoutMS:  150,
			HeartbeatTimeoutMS: 50,
			ApplyBufferSize:    128,
			SnapshotThreshold:  1024,
		},
	}
}

func Load(path string) (Config, error) {
	defaults := Default()
	var cfg Config
	if path == "" {
		path = os.Getenv("MINIKV_CONFIG")
		if path == "" {
			path = "configs/dev.yaml"
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return defaults, nil
		}
		return Config{}, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	var fields map[string]any
	if err := yaml.Unmarshal(data, &fields); err != nil {
		return Config{}, err
	}

	if cfg.Host == "" {
		cfg.Host = defaults.Host
	}
	_, hasPort := fields["port"]
	if cfg.Port < 0 || (!hasPort && cfg.Port == 0) {
		cfg.Port = defaults.Port
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = defaults.LogLevel
	}
	if cfg.Debug.Host == "" {
		cfg.Debug.Host = defaults.Debug.Host
	}
	if cfg.Debug.Port <= 0 {
		cfg.Debug.Port = defaults.Debug.Port
	}
	if cfg.Raft.ID == "" {
		cfg.Raft.ID = defaults.Raft.ID
	}
	if len(cfg.Raft.Peers) == 0 {
		cfg.Raft.Peers = []string{cfg.Raft.ID}
	}
	if cfg.Raft.PeerAddrs == nil {
		cfg.Raft.PeerAddrs = make(map[string]string)
	}
	if cfg.Raft.ListenAddr == "" {
		if addr := cfg.Raft.PeerAddrs[cfg.Raft.ID]; addr != "" {
			cfg.Raft.ListenAddr = addr
		} else {
			cfg.Raft.ListenAddr = defaults.Raft.ListenAddr
		}
	}
	if cfg.Raft.PeerAddrs[cfg.Raft.ID] == "" {
		cfg.Raft.PeerAddrs[cfg.Raft.ID] = cfg.Raft.ListenAddr
	}
	if cfg.Raft.WALPath == "" {
		cfg.Raft.WALPath = fmt.Sprintf("data/raft-%s.wal", cfg.Raft.ID)
	}
	if cfg.Storage.LSMPath == "" {
		cfg.Storage.LSMPath = fmt.Sprintf("data/lsm-%s", cfg.Raft.ID)
	}
	if cfg.Raft.ElectionTimeoutMS <= 0 {
		cfg.Raft.ElectionTimeoutMS = defaults.Raft.ElectionTimeoutMS
	}
	if cfg.Raft.HeartbeatTimeoutMS <= 0 {
		cfg.Raft.HeartbeatTimeoutMS = defaults.Raft.HeartbeatTimeoutMS
	}
	if cfg.Raft.ApplyBufferSize <= 0 {
		cfg.Raft.ApplyBufferSize = defaults.Raft.ApplyBufferSize
	}
	if cfg.Raft.SnapshotThreshold == 0 {
		cfg.Raft.SnapshotThreshold = defaults.Raft.SnapshotThreshold
	}

	return cfg, nil
}

func (c Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c DebugConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
