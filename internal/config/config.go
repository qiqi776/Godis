package config

import (
	"fmt"
	"os"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Host 	  string `yaml:"host"`
	Port 	  int 	 `yaml:"port"`
	LogLevel  string `yaml:"log_level"`
	Databases int    `yaml:"databases"`
}

func Default() Config {
	return Config{
		Host: 	   "127.0.0.1",
		Port: 	   6380,
		LogLevel:  "debug",
		Databases: 16,
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
	return cfg, nil
}

func (c Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}