package main

import (
	"fmt"
	"net"
	"strings"
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

	// 发送命令 (Go 字符串已经是 UTF-8，直接转 []byte)
	_, err = conn.Write([]byte(command))
	if err != nil {
		fmt.Printf("  [FAIL] Write error: %v\n", err)
		return false
	}

	// 接收响应
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("  [FAIL] Read error: %v\n", err)
		return false
	}

	actual := string(buf[:n])

	if strings.TrimSpace(actual) == strings.TrimSpace(expectedResponse) {
		fmt.Printf("  [PASS] Expected: '%s', Got: '%s'\n", expectedResponse, actual)
		return true
	} else {
		fmt.Printf("  [FAIL] Expected: '%s', Got: '%s'\n", expectedResponse, actual)
		return false
	}
}

func main() {
	fmt.Println("--- Starting Automated K/V Server Test ---")
	
	results := []bool{
		runTestCase("Set name", "SET name alice", "OK"),
		runTestCase("Get name", "GET name", "\"alice\""), // 注意转义引号
		runTestCase("Set age", "SET age 30", "OK"),
		runTestCase("Get age", "GET age", "\"30\""),
		runTestCase("Get non-exist", "GET noname", "(nil)"),
	}

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