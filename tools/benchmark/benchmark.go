package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	successCount int64
	failCount    int64
)

// encodeRESP 将普通命令转换为 RESP 协议格式字符串
func encodeRESP(command string) string {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return ""
	}
	var sb strings.Builder
	// 写入数组头 *<参数个数>\r\n
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	// 写入每个参数 $<长度>\r\n<内容>\r\n
	for _, part := range parts {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(part), part))
	}
	return sb.String()
}

func worker(id int, address string, requests int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Client %d connect failed: %v\n", id, err)
		atomic.AddInt64(&failCount, int64(requests))
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 预先生成命令，避免在循环中重复进行字符串拼接和内存分配，让压测更纯粹
	// 这里我们测试 SET 命令
	key := fmt.Sprintf("key_%d", id)
	value := fmt.Sprintf("value_%d", id)
	command := fmt.Sprintf("SET %s %s", key, value)
	respCmd := []byte(encodeRESP(command))

	for i := 0; i < requests; i++ {
		// 1. 发送请求
		if _, err := conn.Write(respCmd); err != nil {
			atomic.AddInt64(&failCount, 1)
			break
		}

		// 2. 读取响应 (SET 返回 +OK\r\n)
		// 我们只读取一行即可，因为我们知道 SET 的响应是 Simple String
		line, err := reader.ReadString('\n')
		if err != nil {
			atomic.AddInt64(&failCount, 1)
			break
		}

		// 简单的校验
		if strings.HasPrefix(line, "+OK") {
			atomic.AddInt64(&successCount, 1)
		} else {
			atomic.AddInt64(&failCount, 1)
		}
	}
}

func main() {
	ip := flag.String("ip", "127.0.0.1", "Server IP")
	port := flag.Int("port", 6378, "Server Port")
	clients := flag.Int("c", 200, "Number of concurrent clients (并发数)")
	requests := flag.Int("n", 10000, "Requests per client (每个客户端请求数)")
	flag.Parse()

	address := fmt.Sprintf("%s:%d", *ip, *port)
	totalRequests := *clients * *requests

	fmt.Println("--- Performance Test Start (RESP Protocol) ---")
	fmt.Printf("Target: %s\n", address)
	fmt.Printf("Clients: %d\n", *clients)
	fmt.Printf("Requests per client: %d\n", *requests)
	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Println("--------------------------------------------")

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go worker(i, address, *requests, &wg)
	}

	wg.Wait()
	duration := time.Since(start).Seconds()

	fmt.Println("\n--- Result ---")
	fmt.Printf("Duration:   %.4f s\n", duration)
	fmt.Printf("Successful: %d\n", atomic.LoadInt64(&successCount))
	fmt.Printf("Failed:     %d\n", atomic.LoadInt64(&failCount))
	fmt.Printf("QPS:        %.2f\n", float64(atomic.LoadInt64(&successCount))/duration)
}
