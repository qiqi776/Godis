package protocol

import (
	"fmt"
	"strings"
)

// MakeArrayHeader 生成 RESP 数组头 (*3\r\n)
func MakeArrayHeader(n int) string {
	return fmt.Sprintf("*%d\r\n", n)
}

// MakeBulkString 生成 RESP Bulk String ($3\r\nSET\r\n)
func MakeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// MakeNullBulkString 生成 (nil) ($-1\r\n)
func MakeNullBulkString() string {
	return "$-1\r\n"
}

// EncodeCmd 将普通命令转为 RESP
func EncodeCmd(command string) string {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(MakeArrayHeader(len(parts)))
	for _, part := range parts {
		sb.WriteString(MakeBulkString(part))
	}
	return sb.String()
}