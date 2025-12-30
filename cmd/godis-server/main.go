package main

import (
	"flag"
	"godis/internal/aof"
	"godis/internal/commands"
	"godis/internal/config"
	"godis/internal/core"
	"godis/internal/database"
	"godis/internal/tcp"
	"godis/pkg/logger"
	"godis/pkg/protocol"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	// 1. 解析命令行参数
	configFile := flag.String("conf", "redis.conf", "path to config file")
	flag.Parse()
	cfg := config.Load(*configFile)

	// 2. 初始化日志
	logger.Init(cfg.LogLevel, cfg.LogFile)
	logger.Info("Godis server initializing...")

	// 3. 初始化命令层和数据库
	commands.Init()
	db := database.NewStandalone()
	db.StartCleanTask()

	// 4. AOF处理
	if cfg.AppendOnly {
		aofEngine, err := aof.NewAof(cfg.AppendFile, cfg.AppendFsync)
		if err != nil {
			logger.Fatal("Failed to open AOF file: %v", err)
		}
		db.SetAof(aofEngine)
		logger.Info("Loading data from AOF...")
		fakeConn := core.NewConnection()

		aofEngine.Read(func(cmd protocol.Value) {
			if cmd.Type == protocol.Array && len(cmd.Array) > 0 {
				cmdName := strings.ToUpper(string(cmd.Array[0].Bulk))
				args := cmd.Array[1:]
				ctx := &core.Context{
					Args: args,
					DB:   db,
					Conn: fakeConn,
				}
				commands.Execute(cmdName, ctx)
			}
		})
		logger.Info("AOF loaded.")
	}

	// 5. 启动 TCP 服务器
	server := tcp.NewServer(cfg, db)
	go server.Start()

	// 6. 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Godis server shutting down...")
	server.Stop()
	db.Close()

	logger.Info("Godis server exited properly.")
}