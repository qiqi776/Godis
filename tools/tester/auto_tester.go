package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

const (
	address = "127.0.0.1:6378"
)

// encodeRESP 将普通命令转换为 RESP 协议格式字符串
// 例如: "SET name alice" -> "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nalice\r\n"
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
	respCmd := encodeRESP(command)
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
		runTestCase("Set name",      "SET name alice", "+OK\r\n"),
		runTestCase("Get name",      "GET name",       "$5\r\nalice\r\n"),
		runTestCase("Set age",       "SET age 30",     "+OK\r\n"),
		runTestCase("Get age",       "GET age",        "$2\r\n30\r\n"),
		runTestCase("Get non-exist", "GET noname",     "$-1\r\n"),
		runTestCase("Ping",          "PING",           "+PONG\r\n"),
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