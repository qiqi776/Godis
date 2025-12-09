package database

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Stats 存储服务器统计信息
type Stats struct {
	startTime              time.Time
	ConnectedClients       int64
	TotalCommandsProcessed int64
	KeyspaceHits           int64
	KeyspaceMisses         int64
}

// NewStats 初始化统计模块
func NewStats() *Stats {
	return &Stats{
		startTime: time.Now(),
	}
}

// GetInfo 生成 INFO 命令需要的字符串报告
func (s *Stats) GetInfo(keyCount int) string {
	uptime := int64(time.Since(s.startTime).Seconds())
	
	// 使用 string builder 拼接（简化版）
	info := "# Server\r\n"
	info += "godis_version:0.0.1\r\n"
	info += fmt.Sprintf("uptime_in_seconds:%d\r\n", uptime)
	info += "\r\n"

	info += "# Clients\r\n"
	info += fmt.Sprintf("connected_clients:%d\r\n", atomic.LoadInt64(&s.ConnectedClients))
	info += "\r\n"

	info += "# Stats\r\n"
	info += fmt.Sprintf("total_commands_processed:%d\r\n", atomic.LoadInt64(&s.TotalCommandsProcessed))
	info += fmt.Sprintf("keyspace_hits:%d\r\n", atomic.LoadInt64(&s.KeyspaceHits))
	info += fmt.Sprintf("keyspace_misses:%d\r\n", atomic.LoadInt64(&s.KeyspaceMisses))
	info += "\r\n"

	info += "# Keyspace\r\n"
	info += fmt.Sprintf("db0:keys=%d,expires=0,avg_ttl=0\r\n", keyCount)
	
	return info
}