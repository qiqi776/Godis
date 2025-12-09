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

	// 设置读写超时，防止测试卡死
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// 1. 发送 RESP 格式的命令
	respCmd := protocol.EncodeCmd(command)
	_, err = conn.Write([]byte(respCmd))
	if err != nil {
		fmt.Printf("  [FAIL] Write error: %v\n", err)
		return false
	}

	// 2. 读取响应
	reader := bufio.NewReader(conn)
	
	// 读取第一行 (包含类型前缀和内容，或者数组/Bulk长度)
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("  [FAIL] Read error: %v\n", err)
		return false
	}

	actual := line
	
	// 如果是 Bulk String ($5\r\n...) 且不是空值 ($-1\r\n)，则还需要读下一行数据
	if strings.HasPrefix(line, "$") && strings.TrimSpace(line) != "$-1" {
		line2, err := reader.ReadString('\n')
		if err == nil {
			actual += line2
		}
	}

	// 3. 比较结果
	// 注意：这里进行精确的字符串匹配，包括 \r\n
	if actual == expectedResponse {
		// 使用 %q 可以打印出不可见的 \r\n，方便调试
		fmt.Printf("  [PASS] Expected: %q, Got: %q\n", expectedResponse, actual)
		return true
	} else {
		fmt.Printf("  [FAIL] Expected: %q, Got: %q\n", expectedResponse, actual)
		return false
	}
}

func main() {
	fmt.Println("--- Starting Automated K/V Server Test (RESP Protocol) ---")
	
	// 注意：预期结果必须是严格的 RESP 格式
	results := []bool{
		// 基础功能
		runTestCase("Set name",      "SET name alice", "+OK\r\n"),
		runTestCase("Get name",      "GET name",       "$5\r\nalice\r\n"),
		
		// 对应 C++ 新增的边界测试
		// 1. 大小写不敏感测试
		runTestCase("Case Insensitive SET", "sEt name bob", "+OK\r\n"),
		runTestCase("Case Insensitive GET", "get name",     "$3\r\nbob\r\n"),
		
		// 2. 覆盖测试
		runTestCase("Overwrite value", "SET name charlie", "+OK\r\n"),
		runTestCase("Get overwritten", "GET name",         "$7\r\ncharlie\r\n"),

		// 3. 错误处理测试 (确保 Server 端返回标准的 Redis 错误前缀)
		// 注意：你需要确保 internal/db/database.go 中的错误信息与这里匹配
		runTestCase("GET wrong args", "GET",           "-ERR wrong number of arguments for 'get' command\r\n"),
		runTestCase("SET wrong args", "SET k",         "-ERR wrong number of arguments for 'set' command\r\n"),
		runTestCase("Unknown Cmd",    "UNKNOWN_CMD k", "-ERR unknown command 'UNKNOWN_CMD'\r\n"),
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