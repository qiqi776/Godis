package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	LogLevel          string `yaml:"log_level"`
	Databases         int    `yaml:"databases"`
	AOFEnabled        bool   `yaml:"aof_enabled"`
	AOFPath           string `yaml:"aof_path"`
	AOFFsync          string `yaml:"aof_fsync"`
	AOFUseRDBPreamble bool   `yaml:"aof_use_rdb_preamble"`
	RDBEnabled        bool   `yaml:"rdb_enabled"`
	RDBPath           string `yaml:"rdb_path"`
}

func Default() Config {
	return Config{
		Host:       "127.0.0.1",
		Port:       6380,
		LogLevel:   "debug",
		Databases:  16,
		AOFEnabled: false,
		AOFPath:    "appendonly.aof",
		AOFFsync:   "everysec",
		RDBEnabled: false,
		RDBPath:    "dump.rdb",
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		path = os.Getenv("GODIS_CONFIG")
		if path == "" {
			path = "configs/dev.yaml"
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}

	if cfg.Databases <= 0 {
		cfg.Databases = Default().Databases
	}
	if cfg.AOFPath == "" {
		cfg.AOFPath = Default().AOFPath
	}
	if cfg.AOFFsync == "" {
		cfg.AOFFsync = Default().AOFFsync
	}
	if cfg.RDBPath == "" {
		cfg.RDBPath = Default().RDBPath
	}
	return cfg, nil
}

func (c Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
