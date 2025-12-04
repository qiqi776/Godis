package main

import (
	"godis/internal/config"
	"godis/internal/server"
	"godis/pkg/logger"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 1. 初始化配置
	cfg := config.Load()

	// 2. 初始化日志
	logger.Init(cfg.LogLevel)
	logger.Info("Godis server starting...")

	// 3. 启动 TCP 服务器
	srv := server.NewKVServer(cfg)
	go srv.Start()

	// 4. 优雅退出 (监听 Ctrl+C)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Godis server shutting down...")
	srv.Stop()
}