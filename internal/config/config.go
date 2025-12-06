package config

import (
	"bufio"
	"os"
	"strings"
)

type Config struct {
	Port       string
	LogLevel   string
	LogFile    string
	AppendOnly bool
	AppendFile string
}

func Load(path string) *Config {
	cfg := &Config{
		Port:     "6378",
		LogLevel: "info",
		AppendOnly: false,			  // 默认关闭
		AppendFile: "appendonly.aof", // 默认文件名
	}
	if path == "" {
		return cfg
	}

	file, err := os.Open(path)
	if err != nil {
		return cfg
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) >= 2 {
			key := parts[0]
			val := parts[1]
			switch key {
			case "port":
				cfg.Port = val
			case "loglevel":
				cfg.LogLevel = val
			case "logfile":
				cfg.LogFile = val
			case "appendonly":
				cfg.AppendOnly = (val == "yes")
			case "appendfilename":
				cfg.AppendFile = strings.Trim(val, "\"")
			}
		}
	}
	return cfg
}