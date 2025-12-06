package main

import (
	"flag"
	"godis/internal/aof"
	"godis/internal/config"
	"godis/internal/db"
	"godis/internal/server"
	"godis/pkg/logger"
	"godis/pkg/protocol"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 1. 解析命令行参数
	configFile := flag.String("conf", "redis.conf", "path to config file")
	flag.Parse()
	cfg := config.Load(*configFile)

	// 2. 初始化日志 (传入配置中的日志文件路径)
	logger.Init(cfg.LogLevel, cfg.LogFile)
	logger.Info("Godis server initializing...")

	// 3. 初始化AOF
	var aofEngine *aof.Aof
	var err error
	if cfg.AppendOnly {
		aofEngine, err = aof.NewAof(cfg.AppendFile)
		if err != nil {
			logger.Fatal("Failed to open AOF file: %v", err)
		}
		defer aofEngine.Close()
	}

	// 4. 初始化db
	database := db.NewDatabase()

	// 5. AOF数据恢复
	if cfg.AppendOnly {
		logger.Info("Loading data from AOF...")
		err := aofEngine.Read(func(cmd protocol.Value) {
			database.Exec(cmd)
		})
		if err != nil {
			logger.Error("AOF read error: %v", err)
		}

		database.SetAof(aofEngine)
		logger.Info("AOF loaded")
	}


	// 6. 启动服务器以及优雅退出
	srv := server.NewServer(cfg, database)
	go srv.Start()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Godis server shutting down...")
	// srv.Stop() // 可以在 Server 中实现 Stop 方法来关闭 listener
}