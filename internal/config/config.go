package config

type Config struct {
	Port     string
	LogLevel string
}

func Load() *Config {
	// 简单起见，先硬编码，以后从 redis.conf 读取
	return &Config{
		Port:     "6378",
		LogLevel: "debug",
	}
}