package main

import (
	"flag"
	"godis/internal/config"
	"godis/internal/server"
	"godis/pkg/logger"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 1. 解析命令行参数
	configFile := flag.String("conf", "", "path to config file")
	flag.Parse()

	// 2. 初始化配置
	cfg := config.Load(*configFile)

	// 3. 初始化日志 (传入配置中的日志文件路径)
	logger.Init(cfg.LogLevel, cfg.LogFile)
	logger.Info("Godis server initializing...")

	// 4. 启动服务器
	srv := server.NewServer(cfg)
	go srv.Start()

	// 5. 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Godis server shutting down...")
	// srv.Stop() // 可以在 Server 中实现 Stop 方法来关闭 listener
}