package main

import (
	"flag"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	successCount int64
	failCount    int64
	totalBytes   int64
)

func worker(address string, requests int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", address)
	if err != nil {
		atomic.AddInt64(&failCount, 1)
		return
	}
	defer conn.Close()

	msg := []byte("SET key value")
	buf := make([]byte, 1024)

	for i := 0; i < requests; i++ {
		if _, err := conn.Write(msg); err != nil {
			break
		}
		
		n, err := conn.Read(buf)
		if err != nil || n == 0 {
			break
		}
		
		atomic.AddInt64(&successCount, 1)
		atomic.AddInt64(&totalBytes, int64(n))
	}
}

func main() {
	ip := flag.String("ip", "127.0.0.1", "Server IP")
	port := flag.Int("port", 6378, "Server Port")
	clients := flag.Int("c", 50, "Number of concurrent clients")
	requests := flag.Int("n", 1000, "Requests per client")
	flag.Parse()

	address := fmt.Sprintf("%s:%d", *ip, *port)
	fmt.Println("--- Performance Test Start ---")
	fmt.Printf("Target: %s\nClients: %d\nRequests per client: %d\n", address, *clients, *requests)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go worker(address, *requests, &wg)
	}

	wg.Wait()
	duration := time.Since(start).Seconds()

	fmt.Println("\n--- Result ---")
	fmt.Printf("Duration: %.2f s\n", duration)
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failCount)
	fmt.Printf("QPS: %.2f\n", float64(successCount)/duration)
}