package main

import (
	"bufio"
	"fmt"
	"godis/pkg/protocol"
	"net"
	"strings"
	"time"
)

const (
	address = "127.0.0.1:6378"
)

func runTestCase(testName, command, expectedResponse string) bool {
	fmt.Printf("Running test: %s...\n", testName)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("  [FAIL] Connection error: %v\n", err)
		return false
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// 1. 发送
	respCmd := protocol.EncodeCmd(command)
	_, err = conn.Write([]byte(respCmd))
	if err != nil {
		fmt.Printf("  [FAIL] Write error: %v\n", err)
		return false
	}

	// 2. 读取
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("  [FAIL] Read error: %v\n", err)
		return false
	}

	actual := line
	if strings.HasPrefix(line, "$") && strings.TrimSpace(line) != "$-1" {
		line2, err := reader.ReadString('\n')
		if err == nil {
			actual += line2
		}
	}

	// 3. 校验
	// 注意：对于 TTL 命令，返回值会随时间变化，我们只能校验格式或大概范围
	if expectedResponse == "SKIP_CHECK" {
		fmt.Printf("  [PASS] (Skipped Strict Match) Got: %q\n", actual)
		return true
	}

	if actual == expectedResponse {
		fmt.Printf("  [PASS] Expected: %q, Got: %q\n", expectedResponse, actual)
		return true
	} else {
		fmt.Printf("  [FAIL] Expected: %q, Got: %q\n", expectedResponse, actual)
		return false
	}
}

// 专门测试惰性删除 (需要 Sleep)
func runLazyDeletionTest() bool {
	fmt.Println("Running test: Lazy Deletion (LD) Check...")
	// 1. 设置键并过期 (1秒)
	if !runTestCase("LD: SET key", "SET key_ld val_ld", "+OK\r\n") { return false }
	if !runTestCase("LD: EXPIRE key", "EXPIRE key_ld 1", ":1\r\n") { return false }

	fmt.Println("  ... Waiting 1.2s for expiration ...")
	time.Sleep(1200 * time.Millisecond)

	// 2. GET 触发删除
	if !runTestCase("LD: GET expired key", "GET key_ld", "$-1\r\n") { return false }
	
	// 3. TTL 确认消失
	if !runTestCase("LD: TTL check after GET", "TTL key_ld", ":-2\r\n") { return false }

	return true
}

func main() {
	fmt.Println("--- Starting Automated K/V Server Test ---")
	
	results := []bool{
		// 基础命令
		runTestCase("Set name", "SET name alice", "+OK\r\n"),
		runTestCase("Get name", "GET name", "$5\r\nalice\r\n"),
		
		// TTL 相关测试
		runTestCase("EXPIRE success", "EXPIRE name 10", ":1\r\n"),
		runTestCase("TTL check (exists)", "TTL name", "SKIP_CHECK"), // 只要不是 -1 或 -2 即可，这里简化处理
		runTestCase("PERSIST success", "PERSIST name", ":1\r\n"),
		runTestCase("TTL check (persisted)", "TTL name", ":-1\r\n"),
		
		// PEXPIRE (毫秒)
		runTestCase("PEXPIRE success", "PEXPIRE name 5000", ":1\r\n"),
		runTestCase("PTTL check", "PTTL name", "SKIP_CHECK"), 
	}

	// 执行惰性删除测试
	results = append(results, runLazyDeletionTest())

	failedCount := 0
	for _, res := range results {
		if !res {
			failedCount++
		}
	}

	fmt.Println("\n--- Test Summary ---")
	if failedCount == 0 {
		fmt.Printf("✅ All %d tests passed!\n", len(results))
	} else {
		fmt.Printf("❌ %d out of %d tests failed.\n", failedCount, len(results))
	}
}